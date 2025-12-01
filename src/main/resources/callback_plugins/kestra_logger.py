# (c) 2012-2014, Michael DeHaan <michael.dehaan@gmail.com>
# (c) 2017 Ansible Project
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import annotations
import json
import os
from datetime import datetime, timezone

DOCUMENTATION = """
    name: kestra_logger
    type: aggregate
    short_description: default Ansible screen output
    version_added: historical
    description:
        - This is the default output callback for ansible-playbook.
    extends_documentation_fragment:
      - default_callback
      - result_format_callback
    requirements:
      - set as stdout in configuration
"""

from ansible import constants as C
from ansible import context
from ansible.playbook.task_include import TaskInclude
from ansible.plugins.callback import CallbackBase
from ansible.utils.color import colorize, hostcolor
from ansible.utils.fqcn import add_internal_fqcns


class CallbackModule(CallbackBase):

    """
    This is the default callback interface, which simply prints messages
    to stdout when new callback events are received.
    """

    CALLBACK_VERSION = 2.0
    CALLBACK_TYPE = 'aggregate'
    CALLBACK_NAME = 'kestra_logger'

    def __init__(self):
        # Raw Kestra output list (backward compatible)
        self._kestra_outputs = []

        # Structured outputs
        self._kestra_playbooks = []
        self._current_playbook = None
        self._current_play = None
        self._current_task = None
        self._unnamed_task_counter = 0

        # --- timing (minimal add) ---
        self._current_task_started_at = None  # datetime internal only
        # ----------------------------

        self._play = None
        self._last_task_banner = None
        self._last_task_name = None
        self._task_type_cache = {}

        super(CallbackModule, self).__init__()

        # aggregate collector only: avoid duplicating stdout_callback output
        self._silent = True

        # Best-effort discovery of log_path (from ansible.cfg)
        self._log_file_path = getattr(C, "LOG_PATH", None) or getattr(C, "DEFAULT_LOG_PATH", None)
        if self._log_file_path in (None, "", "None"):
            self._log_file_path = None
        else:
            self._log_file_path = os.path.abspath(os.path.expanduser(str(self._log_file_path)))

        self._log_fh = None
        if self._log_file_path:
            try:
                os.makedirs(os.path.dirname(self._log_file_path), exist_ok=True)
                self._log_fh = open(self._log_file_path, "a", encoding="utf-8")
            except Exception:
                self._log_fh = None

    # -------------------------------------------------------------------------
    # Kestra serialization helpers
    # -------------------------------------------------------------------------

    def _log_kestra_outputs(self):
        """
        Final payload printed to stdout for Kestra parsing.
        We must put structured outputs under "outputs"
        because Kestra only reads that key.
        """
        payload = {
            "outputs": {
                "outputs": self._kestra_outputs,
                "playbooks": self._kestra_playbooks
            }
        }
        print("::" + json.dumps(payload) + "::")

    def _add_results_to_kestra_outputs(self, result):
        self._kestra_outputs.append(dict(result._result))

    def _write_log_line(self, line: str):
        """
        Write a line to log_path if available.
        """
        if not self._log_fh:
            return
        try:
            self._log_fh.write(line + "\n")
            self._log_fh.flush()
        except Exception:
            pass

    def _flush_current_task_log(self):
        """
        Emit ONE compact Kestra log line for the current task,
        including all host results already collected,
        and add timing fields.
        Also finalize structured task timing fields.
        """
        if self._current_task is None or self._current_play is None:
            return

        play_name = self._current_play.get("name", "implicit_play")
        task_name = self._current_task.get("name", "unnamed_task")

        ended_at_dt = datetime.now(timezone.utc)
        started_at_dt = self._current_task_started_at

        duration_ms = None
        if isinstance(started_at_dt, datetime):
            duration_ms = int((ended_at_dt - started_at_dt).total_seconds() * 1000)

        started_at_str = started_at_dt.isoformat().replace("+00:00", "Z") if isinstance(started_at_dt, datetime) else None
        ended_at_str = ended_at_dt.isoformat().replace("+00:00", "Z")

        # finalize structured task (strings only!)
        self._current_task["startedAt"] = started_at_str
        self._current_task["endedAt"] = ended_at_str

        task_payload = {
            "play": play_name,
            "task": task_name,
            "uid": self._current_task.get("uid"),
            "startedAt": started_at_str,
            "endedAt": ended_at_str,
            "durationMs": duration_ms,
            "hosts": self._current_task.get("hosts", [])
        }

        json_line = json.dumps(task_payload, separators=(",", ":"))

        # stdout for Kestra live parsing
        print(json_line)

        # same line in log_path with timestamp prefix
        ts = ended_at_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        self._write_log_line(f"{ts} {json_line}")

    # -------------------------------------------------------------------------
    # Structured model builders
    # -------------------------------------------------------------------------

    def _start_playbook_if_needed(self):
        """
        Ensure a current playbook container exists.
        No index/command metadata; one container per ansible-playbook run.
        """
        if self._current_playbook is not None:
            return

        self._current_playbook = {
            "plays": []
        }
        self._kestra_playbooks.append(self._current_playbook)

    def _start_play(self, play):
        self._start_playbook_if_needed()

        name = play.get_name().strip()
        if not name:
            name = "unnamed_play"

        self._current_play = {
            "name": name,
            "tasks": []
        }
        self._current_playbook["plays"].append(self._current_play)

    def _start_task(self, task):
        """
        Create a task entry inside the current play.
        If task name is empty, fallback to action or unnamed_task_<n>.
        Also record start time.
        IMPORTANT: store only JSON-serializable fields in task_entry.
        """
        if self._current_play is None:
            self._start_playbook_if_needed()
            self._current_play = {
                "name": "implicit_play",
                "tasks": []
            }
            self._current_playbook["plays"].append(self._current_play)

        task_name = task.get_name().strip()
        if not task_name:
            action = getattr(task, "action", None)
            if action:
                task_name = str(action)
            else:
                self._unnamed_task_counter += 1
                task_name = f"unnamed_task_{self._unnamed_task_counter}"

        started_at_dt = datetime.now(timezone.utc)
        self._current_task_started_at = started_at_dt

        play_name = self._current_play.get("name", "implicit_play")
        uid = f"{play_name} | {task_name}"

        task_entry = {
            "uid": uid,
            "name": task_name,
            "startedAt": started_at_dt.isoformat().replace("+00:00", "Z"),
            "endedAt": None,
            "hosts": []
        }

        self._current_task = task_entry
        self._current_play["tasks"].append(task_entry)

    def _add_host_result(self, result, status):
        """
        Add per-host result under current task (structured),
        and also append to raw outputs (compat).
        """
        self._add_results_to_kestra_outputs(result)

        if self._current_task is None:
            self._start_task(result._task)

        host_name = result._host.get_name() if result._host else "unknown_host"
        host_result = {
            "host": host_name,
            "status": status,
            "result": dict(result._result)
        }
        self._current_task["hosts"].append(host_result)

    # -------------------------------------------------------------------------
    # Ansible callbacks
    # -------------------------------------------------------------------------

    def v2_playbook_on_start(self, playbook):
        self._start_playbook_if_needed()

        if self._silent:
            return

        if self._display.verbosity > 1:
            from os.path import basename
            self._display.banner("PLAYBOOK: %s" % basename(playbook._file_name))

        if self._display.verbosity > 3:
            if context.CLIARGS.get('args'):
                self._display.display(
                    'Positional arguments: %s' % ' '.join(context.CLIARGS['args']),
                    color=C.COLOR_VERBOSE,
                    screen_only=True
                )

            for argument in (a for a in context.CLIARGS if a != 'args'):
                val = context.CLIARGS[argument]
                if val:
                    self._display.display(
                        '%s: %s' % (argument, val),
                        color=C.COLOR_VERBOSE,
                        screen_only=True
                    )

        if context.CLIARGS['check'] and self.get_option('check_mode_markers'):
            self._display.banner("DRY RUN")

    def v2_playbook_on_play_start(self, play):
        self._flush_current_task_log()

        self._play = play
        self._start_play(play)

        if self._silent:
            return

        name = play.get_name().strip()
        checkmsg = " [CHECK MODE]" if play.check_mode and self.get_option('check_mode_markers') else ""

        msg = u"PLAY%s" % checkmsg if not name else u"PLAY [%s]%s" % (name, checkmsg)
        self._display.banner(msg)

    def v2_playbook_on_task_start(self, task, is_conditional):
        self._flush_current_task_log()

        self._task_start(task, prefix='TASK')
        self._start_task(task)

        if self._silent:
            return

    def _task_start(self, task, prefix=None):
        if prefix is not None:
            self._task_type_cache[task._uuid] = prefix

        if self._play.strategy in add_internal_fqcns(('free', 'host_pinned')):
            self._last_task_name = None
        else:
            self._last_task_name = task.get_name().strip()

            if not self._silent:
                if self.get_option('display_skipped_hosts') and self.get_option('display_ok_hosts'):
                    self._print_task_banner(task)

    def v2_runner_on_ok(self, result):
        host_label = self.host_label(result)

        self._add_host_result(result, "ok")

        if self._silent:
            return

        if isinstance(result._task, TaskInclude):
            if self._last_task_banner != result._task._uuid:
                self._print_task_banner(result._task)
            return
        elif result._result.get('changed', False):
            if self._last_task_banner != result._task._uuid:
                self._print_task_banner(result._task)
            msg = "changed: [%s]" % (host_label,)
            color = C.COLOR_CHANGED
        else:
            if not self.get_option('display_ok_hosts'):
                return
            if self._last_task_banner != result._task._uuid:
                self._print_task_banner(result._task)
            msg = "ok: [%s]" % (host_label,)
            color = C.COLOR_OK

        self._handle_warnings(result._result)

        if result._task.loop and 'results' in result._result:
            self._process_items(result)
        else:
            self._clean_results(result._result, result._task.action)
            if self._run_is_verbose(result):
                msg += " => %s" % (self._dump_results(result._result),)
            self._display.display(msg, color=color)

    def v2_runner_on_failed(self, result, ignore_errors=False):
        host_label = self.host_label(result)
        self._clean_results(result._result, result._task.action)

        self._add_host_result(result, "failed")

        if self._silent:
            return

        if self._last_task_banner != result._task._uuid:
            self._print_task_banner(result._task)

        self._handle_exception(result._result, use_stderr=self.get_option('display_failed_stderr'))
        self._handle_warnings(result._result)

        if result._task.loop and 'results' in result._result:
            self._process_items(result)
        else:
            if self._display.verbosity < 2 and self.get_option('show_task_path_on_failure'):
                self._print_task_path(result._task)
            msg = "fatal: [%s]: FAILED! => %s" % (
                host_label, self._dump_results(result._result)
            )
            self._display.display(
                msg,
                color=C.COLOR_ERROR,
                stderr=self.get_option('display_failed_stderr')
            )

        if ignore_errors:
            self._display.display("...ignoring", color=C.COLOR_SKIP)

    def v2_runner_on_skipped(self, result):
        self._add_host_result(result, "skipped")

        if self._silent:
            return

        if self.get_option('display_skipped_hosts'):
            self._clean_results(result._result, result._task.action)
            if self._last_task_banner != result._task._uuid:
                self._print_task_banner(result._task)

            if result._task.loop is not None and 'results' in result._result:
                self._process_items(result)

            msg = "skipping: [%s]" % result._host.get_name()
            if self._run_is_verbose(result):
                msg += " => %s" % self._dump_results(result._result)
            self._display.display(msg, color=C.COLOR_SKIP)

    def v2_runner_on_unreachable(self, result):
        self._add_host_result(result, "unreachable")

        if self._silent:
            return

        if self._last_task_banner != result._task._uuid:
            self._print_task_banner(result._task)

        host_label = self.host_label(result)
        msg = "fatal: [%s]: UNREACHABLE! => %s" % (
            host_label, self._dump_results(result._result)
        )
        self._display.display(
            msg,
            color=C.COLOR_UNREACHABLE,
            stderr=self.get_option('display_failed_stderr')
        )

        if result._task.ignore_unreachable:
            self._display.display("...ignoring", color=C.COLOR_SKIP)

    def v2_playbook_on_stats(self, stats):
        if not self._silent:
            self._display.banner("PLAY RECAP")

        self._flush_current_task_log()
        self._log_kestra_outputs()

        if self._log_fh:
            try:
                self._log_fh.close()
            except Exception:
                pass

        if self._silent:
            return

        hosts = sorted(stats.processed.keys())
        for h in hosts:
            t = stats.summarize(h)

            self._display.display(
                u"%s : %s %s %s %s %s %s %s" % (
                    hostcolor(h, t),
                    colorize(u'ok', t['ok'], C.COLOR_OK),
                    colorize(u'changed', t['changed'], C.COLOR_CHANGED),
                    colorize(u'unreachable', t['unreachable'], C.COLOR_UNREACHABLE),
                    colorize(u'failed', t['failures'], C.COLOR_ERROR),
                    colorize(u'skipped', t['skipped'], C.COLOR_SKIP),
                    colorize(u'rescued', t['rescued'], C.COLOR_OK),
                    colorize(u'ignored', t['ignored'], C.COLOR_WARN),
                ),
                screen_only=True
            )

            self._display.display(
                u"%s : %s %s %s %s %s %s %s" % (
                    hostcolor(h, t, False),
                    colorize(u'ok', t['ok'], None),
                    colorize(u'changed', t['changed'], None),
                    colorize(u'unreachable', t['unreachable'], None),
                    colorize(u'failed', t['failures'], None),
                    colorize(u'skipped', t['skipped'], None),
                    colorize(u'rescued', t['rescued'], None),
                    colorize(u'ignored', t['ignored'], None),
                ),
                log_only=True
            )

        self._display.display("", screen_only=True)

        if stats.custom and self.get_option('show_custom_stats'):
            self._display.banner("CUSTOM STATS: ")
            for k in sorted(stats.custom.keys()):
                if k == '_run':
                    continue
                self._display.display(
                    '\t%s: %s' % (k, self._dump_results(stats.custom[k], indent=1).replace('\n', ''))
                )

            if '_run' in stats.custom:
                self._display.display("", screen_only=True)
                self._display.display(
                    '\tRUN: %s' % self._dump_results(stats.custom['_run'], indent=1).replace('\n', '')
                )
            self._display.display("", screen_only=True)

        if context.CLIARGS['check'] and self.get_option('check_mode_markers'):
            self._display.banner("DRY RUN")

    def v2_playbook_on_no_hosts_matched(self):
        if self._silent:
            return
        self._display.display("skipping: no hosts matched", color=C.COLOR_SKIP)

    def v2_playbook_on_no_hosts_remaining(self):
        if self._silent:
            return
        self._display.banner("NO MORE HOSTS LEFT")

    def _print_task_banner(self, task):
        if self._silent:
            return
        args = ''
        if not task.no_log and C.DISPLAY_ARGS_TO_STDOUT:
            args = u', '.join(u'%s=%s' % a for a in task.args.items())
            args = u' %s' % args

        prefix = self._task_type_cache.get(task._uuid, 'TASK')

        task_name = self._last_task_name
        if task_name is None:
            task_name = task.get_name().strip()

        checkmsg = " [CHECK MODE]" if task.check_mode and self.get_option('check_mode_markers') else ""
        self._display.banner(u"%s [%s%s]%s" % (prefix, task_name, args, checkmsg))

        if self._display.verbosity >= 2:
            self._print_task_path(task)

        self._last_task_banner = task._uuid

    def v2_playbook_on_cleanup_task_start(self, task):
        self._task_start(task, prefix='CLEANUP TASK')

    def v2_playbook_on_handler_task_start(self, task):
        self._task_start(task, prefix='RUNNING HANDLER')

    def v2_runner_on_start(self, host, task):
        if self._silent:
            return
        if self.get_option('show_per_host_start'):
            self._display.display(" [started %s on %s]" % (task, host), color=C.COLOR_OK)

    def v2_on_file_diff(self, result):
        if self._silent:
            return
        if result._task.loop and 'results' in result._result:
            for res in result._result['results']:
                if 'diff' in res and res['diff'] and res.get('changed', False):
                    diff = self._get_diff(res['diff'])
                    if diff:
                        if self._last_task_banner != result._task._uuid:
                            self._print_task_banner(result._task)
                        self._display.display(diff)
        elif 'diff' in result._result and result._result['diff'] and result._result.get('changed', False):
            diff = self._get_diff(result._result['diff'])
            if diff:
                if self._last_task_banner != result._task._uuid:
                    self._print_task_banner(result._task)
                self._display.display(diff)

    # -------------------------------------------------------------------------
    # ITEM callbacks (loops) - do NOT collect Kestra outputs here
    # because v2_runner_on_ok/failed already sends aggregated results.
    # This avoids duplicates and keeps unit tests passing.
    # -------------------------------------------------------------------------

    def v2_runner_item_on_ok(self, result):
        # Avoid duplicates on loops: only aggregated event is collected.
        if self._silent:
            return

        host_label = self.host_label(result)
        if isinstance(result._task, TaskInclude):
            return
        elif result._result.get('changed', False):
            if self._last_task_banner != result._task._uuid:
                self._print_task_banner(result._task)
            msg = 'changed'
            color = C.COLOR_CHANGED
        else:
            if not self.get_option('display_ok_hosts'):
                return
            if self._last_task_banner != result._task._uuid:
                self._print_task_banner(result._task)
            msg = 'ok'
            color = C.COLOR_OK

        msg = "%s: [%s] => (item=%s)" % (msg, host_label, self._get_item_label(result._result))
        self._clean_results(result._result, result._task.action)
        if self._run_is_verbose(result):
            msg += " => %s" % self._dump_results(result._result)
        self._display.display(msg, color=color)

    def v2_runner_item_on_failed(self, result):
        # Avoid duplicates on loops: only aggregated event is collected.
        if self._silent:
            return

        if self._last_task_banner != result._task._uuid:
            self._print_task_banner(result._task)

        host_label = self.host_label(result)
        self._clean_results(result._result, result._task.action)
        self._handle_exception(result._result, use_stderr=self.get_option('display_failed_stderr'))

        msg = "failed: [%s]" % (host_label,)
        self._handle_warnings(result._result)
        self._display.display(
            msg + " (item=%s) => %s" % (
                self._get_item_label(result._result),
                self._dump_results(result._result)
            ),
            color=C.COLOR_ERROR,
            stderr=self.get_option('display_failed_stderr')
        )

    def v2_runner_item_on_skipped(self, result):
        # Avoid duplicates on loops: only aggregated event is collected.
        if self._silent:
            return

        if self.get_option('display_skipped_hosts'):
            if self._last_task_banner != result._task._uuid:
                self._print_task_banner(result._task)

            self._clean_results(result._result, result._task.action)
            msg = "skipping: [%s] => (item=%s) " % (
                result._host.get_name(),
                self._get_item_label(result._result)
            )
            if self._run_is_verbose(result):
                msg += " => %s" % self._dump_results(result._result)
            self._display.display(msg, color=C.COLOR_SKIP)

    # -------------------------------------------------------------------------

    def v2_playbook_on_include(self, included_file):
        if self._silent:
            return
        msg = 'included: %s for %s' % (
            included_file._filename,
            ", ".join([h.name for h in included_file._hosts])
        )
        label = self._get_item_label(included_file._vars)
        if label:
            msg += " => (item=%s)" % label
        self._display.display(msg, color=C.COLOR_INCLUDED)

    def v2_runner_retry(self, result):
        if self._silent:
            return
        task_name = result.task_name or result._task
        host_label = self.host_label(result)
        msg = "FAILED - RETRYING: [%s]: %s (%d retries left)." % (
            host_label,
            task_name,
            result._result['retries'] - result._result['attempts']
        )
        if self._run_is_verbose(result, verbosity=2):
            msg += "Result was: %s" % self._dump_results(result._result)
        self._display.display(msg, color=C.COLOR_DEBUG)

    def v2_runner_on_async_poll(self, result):
        if self._silent:
            return
        host = result._host.get_name()
        jid = result._result.get('ansible_job_id')
        started = result._result.get('started')
        finished = result._result.get('finished')
        self._display.display(
            'ASYNC POLL on %s: jid=%s started=%s finished=%s' % (
                host, jid, started, finished
            ),
            color=C.COLOR_DEBUG
        )

    def v2_runner_on_async_ok(self, result):
        if self._silent:
            return
        host = result._host.get_name()
        jid = result._result.get('ansible_job_id')
        self._display.display(
            "ASYNC OK on %s: jid=%s" % (host, jid),
            color=C.COLOR_DEBUG
        )

    def v2_runner_on_async_failed(self, result):
        if self._silent:
            return
        host = result._host.get_name()

        jid = result._result.get('ansible_job_id')
        if not jid and 'async_result' in result._result:
            jid = result._result['async_result'].get('ansible_job_id')
        self._display.display(
            "ASYNC FAILED on %s: jid=%s" % (host, jid),
            color=C.COLOR_DEBUG
        )

    def v2_playbook_on_notify(self, handler, host):
        if self._silent:
            return
        if self._display.verbosity > 1:
            self._display.display(
                "NOTIFIED HANDLER %s for %s" % (handler.get_name(), host),
                color=C.COLOR_VERBOSE,
                screen_only=True
            )
