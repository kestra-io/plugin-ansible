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
 * Issue #123: Ansible writes every `[WARNING]:` to stderr, and core logs stderr at ERROR without
 * looking at it, so a playbook exiting 0 filled the execution logs with ERROR rows. These feed the
 * consumer the exact stderr lines ansible-core produces, so no Docker or Ansible is involved.
 *
 * <p>
 * The wrapped inputs below are not invented: they are what
 * {@code textwrap.wrap("[WARNING]: " + msg, 79, drop_whitespace=False)} emits, which is how
 * ansible-core's Display formats a warning when it has no TTY.
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

    @Test
    void deprecationWarning_isLoggedAtWarn() {
        String line = "[DEPRECATION WARNING]: Ansible will require Python 3.8 or newer on the target";

        List<LogEntry> logs = consume(1, c -> c.accept(line, true));

        assertThat(logs.getFirst().getLevel(), is(Level.WARN));
    }

    // The wrap fell on a space that stayed at the start of the second line, so the first line
    // filled the width. Reported as two ERROR rows, the second one prefix-less.
    @Test
    void wrappedWarning_brokenOnLeadingSpace_isRejoinedIntoOneWarning() {
        String first = "[WARNING]: provided hosts list is empty, only localhost is available. Note that";
        String second = " the implicit localhost does not match 'all'";

        List<LogEntry> logs = consume(1, c ->
        {
            c.accept(first, true);
            c.accept(second, true);
        });

        assertThat(logs, hasSize(1));
        assertThat(logs.getFirst().getLevel(), is(Level.WARN));
        assertThat(logs.getFirst().getMessage(), is(first + second));
    }

    // The wrap left its space at the end of the first line, which is therefore short of the width.
    // This is the `2.15.13` fragment the customer saw as a standalone ERROR row.
    @Test
    void wrappedWarning_brokenOnTrailingSpace_isRejoinedIntoOneWarning() {
        String first = "[WARNING]: Collection community.general does not support Ansible version ";
        String second = "2.15.13";

        List<LogEntry> logs = consume(1, c ->
        {
            c.accept(first, true);
            c.accept(second, true);
        });

        assertThat(logs, hasSize(1));
        assertThat(logs.getFirst().getMessage(), is("[WARNING]: Collection community.general does not support Ansible version 2.15.13"));
    }

    @Test
    void wrappedWarning_acrossThreeLines_isRejoinedIntoOneWarning() {
        String first = "[WARNING]: Host 'localhost' is using the discovered Python interpreter at ";
        String second = "'/usr/bin/python3.11', but future installation of another Python interpreter ";
        String third = "could change the meaning of that path";

        List<LogEntry> logs = consume(1, c ->
        {
            c.accept(first, true);
            c.accept(second, true);
            c.accept(third, true);
        });

        assertThat(logs, hasSize(1));
        assertThat(logs.getFirst().getMessage(), is(first + second + third));
    }

    @Test
    void stderrThatIsNotAWarning_staysAtError() {
        String line = "ERROR! the playbook: missing.yml could not be found";

        List<LogEntry> logs = consume(1, c -> c.accept(line, true));

        assertThat(logs.getFirst().getLevel(), is(Level.ERROR));
        assertThat(logs.getFirst().getMessage(), is(line));
    }

    // The continuation rule must not swallow the next message: a warning that fit within the width
    // cannot have been wrapped, so whatever follows it is its own line even when indented.
    @Test
    void lineFollowingAnUnwrappedWarning_staysAtError() {
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

    @Test
    void stdoutIsUnaffected() {
        String line = "TASK [step one] ****";

        List<LogEntry> logs = consume(1, c -> c.accept(line, false));

        assertThat(logs.getFirst().getLevel(), is(Level.INFO));
    }

    @Test
    void lineCountsCoverBufferedWarnings() {
        AnsibleLogConsumer consumer = new AnsibleLogConsumer(runContext());

        consumer.accept("[WARNING]: Collection community.general does not support Ansible version ", true);
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
     * the number of log lines the consumer should end up emitting, which is what the assertions
     * are really about: a rejoined warning is one line, not the several it arrived as.
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
