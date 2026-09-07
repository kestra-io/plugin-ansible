package io.kestra.plugin.ansible.cli;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import reactor.core.publisher.Flux;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Issue #123: Ansible writes every {@code [WARNING]:} to stderr, and core logs stderr at ERROR
 * without looking at it, so a playbook exiting 0 filled the execution logs with ERROR rows.
 *
 * <p>
 * Every multi-line input below is captured output, not a reconstruction. It comes from
 * ansible-core 2.15.13 (the version in the customer report) in the plugin's default image, via:
 *
 * <pre>
 * docker run --rm --entrypoint bash cytopia/ansible:latest-tools -c '
 *   pip install -q "ansible-core==2.15.13"
 *   python3 -c "from ansible.utils.display import Display; Display().warning(\"...\")" 2&gt;&amp;1'
 * </pre>
 *
 * <p>
 * Re-capture rather than hand-edit these if the wrapping ever needs revisiting. An earlier
 * version of this test derived them from a reading of the ansible source instead, got
 * {@code drop_whitespace} backwards, and passed against an implementation with the same mistake.
 */
@KestraTest
class AnsibleLogConsumerTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> logQueue;

    @Test
    void warning_isLoggedAtWarn() {
        String line = "[WARNING]: No inventory was parsed, only implicit localhost is available";

        List<LogEntry> logs = consume(1, c -> c.accept(line, true));

        assertThat(logs, hasSize(1));
        assertThat(logs.getFirst().getLevel(), is(Level.WARN));
        assertThat(logs.getFirst().getMessage(), is(line));
    }

    // The customer's fourth warning, and the one their env-var workarounds could not silence. Its
    // first line is 72 characters, well short of the 79-column wrap width, so a rule keyed on the
    // line looking "full" misses the continuation and leaves `2.15.13` on an ERROR row.
    @Test
    void wrappedWarning_shortFirstLine_continuationIsAlsoWarn() {
        String first = "[WARNING]: Collection community.general does not support Ansible version";
        String second = "2.15.13";

        List<LogEntry> logs = consume(2, c ->
        {
            c.accept(first, true);
            c.accept(second, true);
        });

        assertThat(logs, hasSize(2));
        assertThat(logs.stream().allMatch(l -> l.getLevel() == Level.WARN), is(true));
    }

    @Test
    void wrappedWarning_firstLineAtFullWidth_continuationIsAlsoWarn() {
        String first = "[WARNING]: provided hosts list is empty, only localhost is available. Note that";
        String second = "the implicit localhost does not match 'all'";

        List<LogEntry> logs = consume(2, c ->
        {
            c.accept(first, true);
            c.accept(second, true);
        });

        assertThat(logs.stream().allMatch(l -> l.getLevel() == Level.WARN), is(true));
    }

    @Test
    void wrappedWarning_acrossThreeLines_everyLineIsWarn() {
        String first = "[WARNING]: Host 'localhost' is using the discovered Python interpreter at";
        String second = "'/usr/bin/python3.11', but future installation of another Python interpreter";
        String third = "could change the meaning of that path";

        List<LogEntry> logs = consume(3, c ->
        {
            c.accept(first, true);
            c.accept(second, true);
            c.accept(third, true);
        });

        assertThat(logs, hasSize(3));
        assertThat(logs.stream().allMatch(l -> l.getLevel() == Level.WARN), is(true));
    }

    // Display.deprecated wraps with drop_whitespace=False, so unlike Display.warning it leaves the
    // break space at the end of each line. The width bookkeeping has to allow for that.
    @Test
    void wrappedDeprecationWarning_everyLineIsWarn() {
        String first = "[DEPRECATION WARNING]: The connection plugin future is deprecated and will be ";
        String second = "removed in a future release of ansible-core. This feature will be removed in ";
        String third = "version 2.19. Deprecation warnings can be disabled by setting ";
        String fourth = "deprecation_warnings=False in ansible.cfg.";

        List<LogEntry> logs = consume(4, c ->
        {
            c.accept(first, true);
            c.accept(second, true);
            c.accept(third, true);
            c.accept(fourth, true);
        });

        assertThat(logs, hasSize(4));
        assertThat(logs.stream().allMatch(l -> l.getLevel() == Level.WARN), is(true));
    }

    // textwrap breaks on hyphens by default, so a URL in a warning splits mid-token and the
    // previous line ends on the hyphen instead of losing a space.
    @Test
    void warningBrokenInsideAHyphenatedToken_continuationIsAlsoWarn() {
        String first = "[WARNING]: see https://docs.ansible.com/ansible-";
        String second = "core/2.15/porting_guides/porting_guide_core_2.16.html for the details";

        List<LogEntry> logs = consume(2, c ->
        {
            c.accept(first, true);
            c.accept(second, true);
        });

        assertThat(logs, hasSize(2));
        assertThat(logs.stream().allMatch(l -> l.getLevel() == Level.WARN), is(true));
        // the text is passed through untouched, so a URL in a warning stays copy-pasteable
        assertThat(byMessage(logs, second).getMessage(), is(second));
    }

    // The Process runner reads stdout and stderr on separate threads, so an unrelated stdout line
    // can land in the middle of a wrapped warning. It must not cost the continuation its level.
    @Test
    void stdoutArrivingMidWarning_doesNotBreakTheContinuation() {
        String first = "[WARNING]: Collection community.general does not support Ansible version";
        String second = "2.15.13";
        String stdout = "ok: [localhost]";

        List<LogEntry> logs = consume(3, c ->
        {
            c.accept(first, true);
            c.accept(stdout, false);
            c.accept(second, true);
        });

        assertThat(byMessage(logs, first).getLevel(), is(Level.WARN));
        assertThat(byMessage(logs, second).getLevel(), is(Level.WARN));
        assertThat(byMessage(logs, stdout).getLevel(), is(Level.INFO));
    }

    @Test
    void stderrThatIsNotAWarning_staysAtError() {
        String line = "ERROR! the playbook: missing.yml could not be found";

        List<LogEntry> logs = consume(1, c -> c.accept(line, true));

        assertThat(logs.getFirst().getLevel(), is(Level.ERROR));
        assertThat(logs.getFirst().getMessage(), is(line));
    }

    // A warning with room left on it cannot have been wrapped, so the line after it is judged on
    // its own. This is what stops the reclassification from spreading to a real failure.
    @Test
    void lineFollowingAWarningWithRoomLeft_staysAtError() {
        String warning = "[WARNING]: something short";
        String detail = "  File \"/usr/lib/python3/site.py\", line 1";

        List<LogEntry> logs = consume(2, c ->
        {
            c.accept(warning, true);
            c.accept(detail, true);
        });

        assertThat(logs, hasSize(2));
        assertThat(byMessage(logs, warning).getLevel(), is(Level.WARN));
        assertThat(byMessage(logs, detail).getLevel(), is(Level.ERROR));
    }

    /**
     * Documents an accepted limitation rather than asserting desired behaviour. stderr carries no
     * severity, so a complete warning that happens to sit close to the wrap width cannot be told
     * apart from a wrapped one, and an unrelated line whose first word would have overflowed the
     * width is read as its continuation and logged at WARN. Change this test only alongside a real
     * signal to distinguish the two, not to paper over a regression.
     */
    @Test
    void unrelatedStderrAfterANearFullWarning_isMisreadAsAContinuation() {
        String warning = "[WARNING]: Collection community.general does not support Ansible version";
        String unrelated = "Traceback (most recent call last):";

        List<LogEntry> logs = consume(2, c ->
        {
            c.accept(warning, true);
            c.accept(unrelated, true);
        });

        assertThat(byMessage(logs, unrelated).getLevel(), is(Level.WARN));
    }

    // A blank line closes the block, so a later stderr line is judged on its own again.
    @Test
    void blankLineClosesTheWarningBlock() {
        String warning = "[WARNING]: provided hosts list is empty, only localhost is available. Note that";
        String detail = "the connection to the host was reset by peer during the handshake exchange";

        List<LogEntry> logs = consume(3, c ->
        {
            c.accept(warning, true);
            c.accept("", true);
            c.accept(detail, true);
        });

        assertThat(byMessage(logs, warning).getLevel(), is(Level.WARN));
        assertThat(byMessage(logs, detail).getLevel(), is(Level.ERROR));
    }

    @Test
    void stdoutIsUnaffected() {
        String line = "TASK [step one] ****";

        List<LogEntry> logs = consume(1, c -> c.accept(line, false));

        assertThat(logs.getFirst().getLevel(), is(Level.INFO));
    }

    @Test
    void lineCountsCoverWarnings() {
        AnsibleLogConsumer consumer = new AnsibleLogConsumer(runContext());

        consumer.accept("[WARNING]: Collection community.general does not support Ansible version", true);
        consumer.accept("2.15.13", true);
        consumer.accept("PLAY RECAP ****", false);

        assertThat(consumer.getStdErrCount(), is(2));
        assertThat(consumer.getStdOutCount(), is(1));
    }

    private RunContext runContext() {
        return TestsUtils.mockRunContext(
            runContextFactory,
            AnsibleCLI.builder().id(IdUtils.create()).type(AnsibleCLI.class.getName()).build(),
            Map.of()
        );
    }

    /**
     * Feeds the consumer and returns the logs it produced, in emission order.
     */
    private List<LogEntry> consume(int expectedCount, Consumer<AnsibleLogConsumer> feed) {
        RunContext runContext = runContext();

        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        Flux<LogEntry> receive = TestsUtils.receive(logQueue, l -> logs.add(l.getLeft()));

        AnsibleLogConsumer consumer = new AnsibleLogConsumer(runContext);
        feed.accept(consumer);

        List<LogEntry> matched = TestsUtils.awaitLogs(logs, l -> l.getMessage() != null, expectedCount);
        receive.blockLast();

        return matched;
    }

    private static LogEntry byMessage(List<LogEntry> logs, String message) {
        return logs.stream()
            .filter(l -> message.equals(l.getMessage()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no log with message: " + message));
    }
}
