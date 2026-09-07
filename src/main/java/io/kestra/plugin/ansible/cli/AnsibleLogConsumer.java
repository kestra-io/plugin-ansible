package io.kestra.plugin.ansible.cli;

import java.time.Instant;
import java.util.List;

import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.runners.RunContext;

/**
 * Logs Ansible warnings at WARN instead of ERROR.
 *
 * <p>
 * Ansible writes all of its {@code [WARNING]:} text to stderr, and core's
 * {@link io.kestra.core.models.tasks.runners.DefaultLogConsumer} logs every stderr line at ERROR
 * without inspecting it, so a playbook that exits 0 still fills the execution logs with ERROR rows
 * (issue #123). Lines not recognised as a warning keep the default behaviour, so {@code ERROR!} and
 * {@code fatal:} stay at ERROR.
 */
public class AnsibleLogConsumer extends AbstractLogConsumer {
    // Prefixes ansible-core's Display uses for non-fatal messages, both written to stderr.
    private static final List<String> WARNING_PREFIXES = List.of("[WARNING]:", "[DEPRECATION WARNING]:");

    // ansible-core wraps warning text with textwrap at Display.columns, which is
    // max(79, tty_width - 1). No task runner gives Ansible a TTY, so it is always exactly 79.
    // Measured on core 2.15.13, 2.16.14 and 2.17.13 in cytopia/ansible:latest-tools.
    static final int WRAP_COLUMNS = 79;

    private final RunContext runContext;

    // A wrapped warning arrives as several stderr lines and only the first keeps the prefix, so a
    // warning is buffered until a line arrives that cannot be a continuation of it.
    private StringBuilder pendingWarning;
    private String pendingLastLine;

    public AnsibleLogConsumer(RunContext runContext) {
        this.runContext = runContext;
    }

    @Override
    public void accept(String line, Boolean isStdErr) {
        this.accept(line, isStdErr, null);
    }

    @Override
    public synchronized void accept(String line, Boolean isStdErr, Instant instant) {
        boolean stdErr = Boolean.TRUE.equals(isStdErr);

        if (stdErr) {
            this.stdErrCount.incrementAndGet();
        } else {
            this.stdOutCount.incrementAndGet();
        }

        if (stdErr && consumeAsWarning(line)) {
            return;
        }

        // A pending warning is emitted before this line so log order matches the run.
        flush();
        outputs.putAll(PluginUtilsService.parseOut(line, runContext.logger(), runContext, stdErr, instant));
    }

    /**
     * Emits the buffered warning, if any. Called by the task once a command has completed, so a
     * warning that was the last thing written to stderr is never dropped.
     */
    public synchronized void flush() {
        if (pendingWarning == null) {
            return;
        }

        runContext.logger().warn(pendingWarning.toString());
        pendingWarning = null;
        pendingLastLine = null;
    }

    private boolean consumeAsWarning(String line) {
        if (isWarningStart(line)) {
            flush();
            pendingWarning = new StringBuilder(line);
            pendingLastLine = line;
            return true;
        }

        if (pendingWarning == null || !isContinuation(pendingLastLine, line)) {
            return false;
        }

        if (needsSpace(pendingLastLine, line)) {
            pendingWarning.append(' ');
        }
        pendingWarning.append(line);
        pendingLastLine = line;

        return true;
    }

    static boolean isWarningStart(String line) {
        return WARNING_PREFIXES.stream().anyMatch(line::startsWith);
    }

    /**
     * A stderr line continues the previous warning when the wrapper had no room for this line's
     * first word on it. That is the invariant a continuation still carries once it has lost the
     * prefix: had the word fit, textwrap would have kept it on the previous line rather than start
     * a new one. A warning short enough never to have been wrapped therefore cannot absorb the
     * line after it, which is what keeps a real error out of a warning block.
     */
    static boolean isContinuation(String previous, String line) {
        if (line.isBlank() || isWarningStart(line)) {
            return false;
        }

        int gap = needsSpace(previous, line) ? 1 : 0;

        return previous.length() + gap + firstToken(line).length() > WRAP_COLUMNS;
    }

    /**
     * {@code Display.warning} wraps with textwrap's default {@code drop_whitespace}, so the space it
     * broke on is gone from both sides and has to be put back to rebuild the message.
     * {@code Display.deprecated} passes {@code drop_whitespace=False} and leaves it on one side.
     */
    private static boolean needsSpace(String previous, String line) {
        return !previous.endsWith(" ") && !line.startsWith(" ");
    }

    private static String firstToken(String line) {
        String text = line.stripLeading();
        int space = text.indexOf(' ');

        return space < 0 ? text : text.substring(0, space);
    }
}
