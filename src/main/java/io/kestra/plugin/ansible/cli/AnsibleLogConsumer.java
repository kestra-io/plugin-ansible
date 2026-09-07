package io.kestra.plugin.ansible.cli;

import java.time.Instant;
import java.util.Map;

import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.models.tasks.runners.DefaultLogConsumer;
import io.kestra.core.runners.RunContext;

/**
 * Logs Ansible warnings at WARN instead of ERROR.
 *
 * <p>
 * Ansible writes all of its {@code [WARNING]:} text to stderr, and core's
 * {@link DefaultLogConsumer} logs every stderr line at ERROR without inspecting it, so a playbook
 * that exits 0 still fills the execution logs with ERROR rows (issue #123).
 *
 * <p>
 * Warning lines are logged here and every other line is handed to core's own consumer
 * untouched, so {@code ERROR!} and {@code fatal:} still reach ERROR and the rest of its behaviour
 * comes along for free: the {@code ##kestra:log:debug##} markers the runner wraps generated
 * before-commands in, the {@code ::{json}::} outputs matcher, and whatever it gains next.
 * Reimplementing it instead meant losing the markers, which showed up as bare INFO log rows.
 *
 * <p>
 * Each line is logged as it arrives. A wrapped warning stays several log entries rather than
 * being reassembled into one: reassembly means holding a line back until the next one decides its
 * fate, which loses ordering against stdout, races the runners that read the two streams on
 * separate threads, and has to guess whether the wrapper ate a space or split a hyphenated token.
 * The severity is the reported problem; the line count is cosmetic.
 */
public class AnsibleLogConsumer extends AbstractLogConsumer {
    // The prefixes ansible-core's Display uses for non-fatal messages, both written to stderr.
    private static final String WARNING_PREFIX = "[WARNING]:";
    private static final String DEPRECATION_PREFIX = "[DEPRECATION WARNING]:";

    // ansible-core wraps warning text with textwrap at Display.columns, which is
    // max(79, tty_width - 1). No task runner gives Ansible a TTY, so it is always exactly 79.
    // Measured on core 2.15.13, 2.16.14 and 2.17.13 in cytopia/ansible:latest-tools.
    static final int WRAP_COLUMNS = 79;

    private final RunContext runContext;
    private final DefaultLogConsumer delegate;

    // Warning lines never reach the delegate, so its stderr count has to be topped up with them.
    private int warningCount;

    // Only the previous stderr line matters: a wrapped warning's continuation lines lose the
    // prefix, so they can only be recognised from the line they continue. stdout never touches
    // this state, which is what keeps a runner's second reader thread out of it.
    private String lastStdErrLine;
    private boolean lastStdErrWasWarning;

    public AnsibleLogConsumer(RunContext runContext) {
        this.runContext = runContext;
        this.delegate = new DefaultLogConsumer(runContext);
    }

    @Override
    public void accept(String line, Boolean isStdErr) {
        this.accept(line, isStdErr, null);
    }

    @Override
    public synchronized void accept(String line, Boolean isStdErr, Instant instant) {
        if (!Boolean.TRUE.equals(isStdErr)) {
            delegate.accept(line, false, instant);
            return;
        }

        boolean warning = isWarningStart(line)
            || (lastStdErrWasWarning && isContinuation(lastStdErrLine, line));

        lastStdErrLine = line;
        lastStdErrWasWarning = warning;

        if (warning) {
            warningCount++;
            runContext.logger().warn(line);
            return;
        }

        delegate.accept(line, true, instant);
    }

    /**
     * Forgets the last stderr line. Called by the task between commands: each command is its own
     * process, so a warning left mid-wrap by one cannot be continued by the next one's first line.
     */
    public synchronized void reset() {
        lastStdErrLine = null;
        lastStdErrWasWarning = false;
    }

    @Override
    public Map<String, Object> getOutputs() {
        return delegate.getOutputs();
    }

    @Override
    public int getStdOutCount() {
        return delegate.getStdOutCount();
    }

    @Override
    public int getStdErrCount() {
        return delegate.getStdErrCount() + warningCount;
    }

    static boolean isWarningStart(String line) {
        return line.startsWith(WARNING_PREFIX) || line.startsWith(DEPRECATION_PREFIX);
    }

    /**
     * A stderr line continues the previous warning when the wrapper had no room for this line's
     * first word on it. That is the invariant a continuation still carries once it has lost the
     * prefix: had the word fit, textwrap would have kept it on the previous line rather than start
     * a new one. A warning with room left on it cannot have been wrapped, so the line after it is
     * judged on its own, which is what keeps a real error out of WARN.
     *
     * <p>
     * Accepted limitation: a complete warning that happens to sit close to the wrap width cannot
     * be told apart from a wrapped one, so an unrelated stderr line whose first word would have
     * overflowed the width is read as its continuation and logged at WARN. stderr carries no
     * severity of its own, so distinguishing the two needs a signal that is not on the wire. See
     * {@code unrelatedStderrAfterANearFullWarning_isMisreadAsAContinuation}.
     */
    static boolean isContinuation(String previous, String line) {
        if (line.isBlank() || isWarningStart(line)) {
            return false;
        }

        // `Display.warning` wraps with textwrap's default `drop_whitespace`, so the space it broke
        // on is gone from both sides and the width has to account for it. `Display.deprecated`
        // passes `drop_whitespace=False`, and a break inside a hyphenated word keeps the hyphen,
        // so in those cases the previous line already carries the separator.
        int gap = previous.endsWith(" ") || previous.endsWith("-") || line.startsWith(" ") ? 0 : 1;

        return previous.length() + gap + firstToken(line).length() > WRAP_COLUMNS;
    }

    private static String firstToken(String line) {
        String text = line.stripLeading();
        int space = text.indexOf(' ');

        return space < 0 ? text : text.substring(0, space);
    }
}
