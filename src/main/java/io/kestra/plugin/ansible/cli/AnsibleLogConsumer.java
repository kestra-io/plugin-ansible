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

    // ansible-core wraps warning text at Display.columns, which is max(79, tty_width - 1). No task
    // runner gives Ansible a TTY, so the width is always exactly 79 here.
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

        if (pendingWarning != null && isContinuation(pendingLastLine, line)) {
            // The wrapper leaves the whitespace it broke on, on one side of the cut, so plain
            // concatenation restores the original text.
            pendingWarning.append(line);
            pendingLastLine = line;
            return true;
        }

        return false;
    }

    static boolean isWarningStart(String line) {
        return WARNING_PREFIXES.stream().anyMatch(line::startsWith);
    }

    /**
     * A stderr line continues the previous warning only when that line looks cut off by the
     * wrapper: it either filled the width or ends on the whitespace the wrap happened at. Anything
     * else is a new message, so an indented line following a short warning still reaches ERROR.
     */
    static boolean isContinuation(String previous, String line) {
        if (line.isBlank() || isWarningStart(line)) {
            return false;
        }

        return previous.length() >= WRAP_COLUMNS || previous.endsWith(" ");
    }
}
