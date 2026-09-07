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
 *   python3 -c "from ansible.utils.display import Display; Display().warning(\\"...\\")" 2&gt;&amp;1'
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
    // line looking "full" misses it and leaves `2.15.13` alone on an ERROR row.
    @Test
    void wrappedWarning_shortFirstLine_isRejoinedIntoOneWarning() {
        String first = "[WARNING]: Collection community.general does not support Ansible version";
        String second = "2.15.13";

        List<LogEntry> logs = consume(1, c ->
        {
            c.accept(first, true);
            c.accept(second, true);
        });

        assertThat(logs, hasSize(1));
        assertThat(logs.getFirst().getLevel(), is(Level.WARN));
        assertThat(
            logs.getFirst().getMessage(),
            is("[WARNING]: Collection community.general does not support Ansible version 2.15.13")
        );
    }

    @Test
    void wrappedWarning_firstLineAtFullWidth_isRejoinedIntoOneWarning() {
        String first = "[WARNING]: provided hosts list is empty, only localhost is available. Note that";
        String second = "the implicit localhost does not match 'all'";

        List<LogEntry> logs = consume(1, c ->
        {
            c.accept(first, true);
            c.accept(second, true);
        });

        assertThat(logs, hasSize(1));
        assertThat(
            logs.getFirst().getMessage(),
            is("[WARNING]: provided hosts list is empty, only localhost is available. Note that the implicit localhost does not match 'all'")
        );
    }

    @Test
    void wrappedWarning_acrossThreeLines_isRejoinedIntoOneWarning() {
        String first = "[WARNING]: Host 'localhost' is using the discovered Python interpreter at";
        String second = "'/usr/bin/python3.11', but future installation of another Python interpreter";
        String third = "could change the meaning of that path";

        List<LogEntry> logs = consume(1, c ->
        {
            c.accept(first, true);
            c.accept(second, true);
            c.accept(third, true);
        });

        assertThat(logs, hasSize(1));
        assertThat(
            logs.getFirst().getMessage(),
            is(
                "[WARNING]: Host 'localhost' is using the discovered Python interpreter at '/usr/bin/python3.11', but future installation of another Python interpreter could change the meaning of that path"
            )
        );
    }

    // Display.deprecated wraps with drop_whitespace=False, so unlike Display.warning it leaves the
    // break space at the end of each line and the rejoin must not add another.
    @Test
    void wrappedDeprecationWarning_keepsItsOwnSpacing() {
        String first = "[DEPRECATION WARNING]: The connection plugin future is deprecated and will be ";
        String second = "removed in a future release of ansible-core. This feature will be removed in ";
        String third = "version 2.19. Deprecation warnings can be disabled by setting ";
        String fourth = "deprecation_warnings=False in ansible.cfg.";

        List<LogEntry> logs = consume(1, c ->
        {
            c.accept(first, true);
            c.accept(second, true);
            c.accept(third, true);
            c.accept(fourth, true);
        });

        assertThat(logs, hasSize(1));
        assertThat(logs.getFirst().getLevel(), is(Level.WARN));
        assertThat(
            logs.getFirst().getMessage(),
            is(
                "[DEPRECATION WARNING]: The connection plugin future is deprecated and will be removed in a future release of ansible-core. This feature will be removed in version 2.19. Deprecation warnings can be disabled by setting deprecation_warnings=False in ansible.cfg."
            )
        );
    }

    @Test
    void stderrThatIsNotAWarning_staysAtError() {
        String line = "ERROR! the playbook: missing.yml could not be found";

        List<LogEntry> logs = consume(1, c -> c.accept(line, true));

        assertThat(logs.getFirst().getLevel(), is(Level.ERROR));
        assertThat(logs.getFirst().getMessage(), is(line));
    }

    // A warning with room left on it cannot have been wrapped, so the line after it is judged on
    // its own. This is what stops the rejoin from swallowing a real failure.
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

    // A blank line closes the block, so the next stderr line is never folded into the warning.
    @Test
    void blankLineClosesTheWarningBlock() {
        String warning = "[WARNING]: provided hosts list is empty, only localhost is available. Note that";
        String detail = "the connection to the host was reset";

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
    void lineCountsCoverBufferedWarnings() {
        AnsibleLogConsumer consumer = new AnsibleLogConsumer(runContext());

        consumer.accept("[WARNING]: Collection community.general does not support Ansible version", true);
        consumer.accept("2.15.13", true);
        consumer.accept("PLAY RECAP ****", false);
        consumer.flush();

        assertThat(consumer.getStdErrCount(), is(2));
        assertThat(consumer.getStdOutCount(), is(1));
    }

    @Test
    void flushIsIdempotent() {
        List<LogEntry> logs = consume(1, c ->
        {
            c.accept("[WARNING]: something short", true);
            c.flush();
            c.flush();
        });

        assertThat(logs, hasSize(1));
    }

    private RunContext runContext() {
        return TestsUtils.mockRunContext(
            runContextFactory,
            AnsibleCLI.builder().id(IdUtils.create()).type(AnsibleCLI.class.getName()).build(),
            Map.of()
        );
    }

    /**
     * Feeds the consumer and returns the logs it produced, in emission order. `expectedCount` is
     * the number of log lines the consumer should end up emitting, which is what the assertions are
     * really about: a rejoined warning is one line, not the several it arrived as.
     */
    private List<LogEntry> consume(int expectedCount, Consumer<AnsibleLogConsumer> feed) {
        RunContext runContext = runContext();

        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        Flux<LogEntry> receive = TestsUtils.receive(logQueue, l -> logs.add(l.getLeft()));

        AnsibleLogConsumer consumer = new AnsibleLogConsumer(runContext);
        feed.accept(consumer);
        consumer.flush();

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
