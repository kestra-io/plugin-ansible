package io.kestra.plugin.ansible.cli;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.event.Level;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.DynamicTaskRunLog;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Fast, deterministic unit tests for the Java-side helpers introduced to fix issue #126: rebuilding
 * the flat "outputs" list from the structured playbooks (instead of the callback duplicating every
 * per-host result on stdout), the outputs-file size guard, the per-host log verbosity modes, and
 * graceful degradation when the outputs file the callback writes is missing or unreadable.
 * None of these exercise Docker/Ansible, so they run in milliseconds.
 */
@KestraTest
class AnsibleCLIOutputsBehaviorTest {

    @Inject
    private RunContextFactory runContextFactory;

    private static AnsibleCLI newTask() {
        return AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .build();
    }

    private static AnsibleCLI.AnsibleOutput.HostResult host(String host, String status, Map<String, Object> result) {
        return AnsibleCLI.AnsibleOutput.HostResult.builder()
            .host(host)
            .status(status)
            .result(result)
            .build();
    }

    // -------------------------------------------------------------------------
    // flattenHostResults: ordering across playbooks -> plays -> tasks -> hosts
    // -------------------------------------------------------------------------

    @Test
    void flattenHostResults_preservesPlaybookPlayTaskHostOrder() {
        var task1 = AnsibleCLI.AnsibleOutput.TaskOutput.builder()
            .name("task1")
            .hosts(List.of(
                host("h1", "ok", Map.of("k", "t1h1")),
                host("h2", "ok", Map.of("k", "t1h2"))
            ))
            .build();
        var task2 = AnsibleCLI.AnsibleOutput.TaskOutput.builder()
            .name("task2")
            .hosts(List.of(host("h1", "ok", Map.of("k", "t2h1"))))
            .build();
        var play1 = AnsibleCLI.AnsibleOutput.PlayOutput.builder().name("play1").tasks(List.of(task1, task2)).build();
        var playbook1 = AnsibleCLI.AnsibleOutput.PlaybookOutput.builder().plays(List.of(play1)).build();

        var task3 = AnsibleCLI.AnsibleOutput.TaskOutput.builder()
            .name("task3")
            .hosts(List.of(host("h1", "ok", Map.of("k", "t3h1"))))
            .build();
        var play2 = AnsibleCLI.AnsibleOutput.PlayOutput.builder().name("play2").tasks(List.of(task3)).build();
        var playbook2 = AnsibleCLI.AnsibleOutput.PlaybookOutput.builder().plays(List.of(play2)).build();

        List<Object> flat = AnsibleCLI.flattenHostResults(List.of(playbook1, playbook2));

        assertThat(
            flat,
            contains(Map.of("k", "t1h1"), Map.of("k", "t1h2"), Map.of("k", "t2h1"), Map.of("k", "t3h1"))
        );
    }

    @Test
    void flattenHostResults_nullOrEmptyPlaybooks_returnsEmptyList() {
        assertThat(AnsibleCLI.flattenHostResults(null), is(empty()));
        assertThat(AnsibleCLI.flattenHostResults(List.of()), is(empty()));
    }

    @Test
    void flattenHostResults_skipsPlaysAndTasksWithoutHosts() {
        var emptyTask = AnsibleCLI.AnsibleOutput.TaskOutput.builder().name("noop").hosts(null).build();
        var play = AnsibleCLI.AnsibleOutput.PlayOutput.builder().name("play").tasks(List.of(emptyTask)).build();
        var playbook = AnsibleCLI.AnsibleOutput.PlaybookOutput.builder().plays(List.of(play)).build();

        assertThat(AnsibleCLI.flattenHostResults(List.of(playbook)), is(empty()));
    }

    // -------------------------------------------------------------------------
    // checkOutputsSize: the queue/task-output guard (not the hang fix)
    // -------------------------------------------------------------------------

    @Test
    void checkOutputsSize_underLimit_doesNotThrow() {
        AnsibleCLI task = newTask();
        Map<String, Object> vars = new HashMap<>();
        vars.put("outputs", List.of(Map.of("msg", "small")));
        vars.put("playbooks", List.of());

        assertDoesNotThrow(() -> task.checkOutputsSize(vars, 10_000_000L));
    }

    @Test
    void checkOutputsSize_overLimit_throwsActionableError() {
        AnsibleCLI task = newTask();
        Map<String, Object> vars = new HashMap<>();
        vars.put("outputs", List.of(Map.of("msg", "x".repeat(1000))));
        vars.put("playbooks", List.of());

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> task.checkOutputsSize(vars, 100L));

        // actionable: names the offending property and points at the escape hatch
        assertThat(e.getMessage(), containsString("maxOutputsSize"));
        assertThat(e.getMessage(), containsString("outputsMode: EXPLICIT"));
    }

    // -------------------------------------------------------------------------
    // taskLogs: logsMode SUMMARY (default) vs FULL
    // -------------------------------------------------------------------------

    @Test
    void taskLogs_summaryMode_okHostsHaveNoResultDetail() {
        var task = AnsibleCLI.AnsibleOutput.TaskOutput.builder()
            .hosts(List.of(host("h1", "ok", Map.of("stdout", "lots of sensitive detail"))))
            .build();

        List<DynamicTaskRunLog> logs = AnsibleCLI.taskLogs(task, AnsibleCLI.LogsMode.SUMMARY);

        assertThat(logs.size(), is(1));
        assertThat(logs.getFirst().message(), is("[h1] ok"));
        assertThat(logs.getFirst().level(), is(Level.INFO));
    }

    @Test
    void taskLogs_summaryMode_failedHostsKeepErrorReasonOnly() {
        var task = AnsibleCLI.AnsibleOutput.TaskOutput.builder()
            .hosts(List.of(host("h1", "failed", Map.of("msg", "boom", "stdout", "lots of detail"))))
            .build();

        List<DynamicTaskRunLog> logs = AnsibleCLI.taskLogs(task, AnsibleCLI.LogsMode.SUMMARY);

        assertThat(logs.getFirst().message(), is("[h1] failed => boom"));
        assertThat(logs.getFirst().level(), is(Level.ERROR));
        assertThat(logs.getFirst().message(), not(containsString("lots of detail")));
    }

    @Test
    void taskLogs_fullMode_logsEntireResultPayload() {
        var task = AnsibleCLI.AnsibleOutput.TaskOutput.builder()
            .hosts(List.of(host("h1", "ok", Map.of("stdout", "full detail here"))))
            .build();

        List<DynamicTaskRunLog> logs = AnsibleCLI.taskLogs(task, AnsibleCLI.LogsMode.FULL);

        assertThat(logs.getFirst().message(), containsString("full detail here"));
    }

    @Test
    void taskLogs_truncatesOverlyLongLines_andPointsAtOutputs() {
        String longMsg = "x".repeat(10_000);
        var task = AnsibleCLI.AnsibleOutput.TaskOutput.builder()
            .hosts(List.of(host("h1", "failed", Map.of("msg", longMsg))))
            .build();

        List<DynamicTaskRunLog> logs = AnsibleCLI.taskLogs(task, AnsibleCLI.LogsMode.SUMMARY);

        assertThat(logs.getFirst().message().length(), lessThan(longMsg.length()));
        assertThat(logs.getFirst().message(), containsString("truncated"));
        assertThat(logs.getFirst().message(), containsString("outputs"));
    }

    // -------------------------------------------------------------------------
    // readOutputsFile: graceful degradation when the callback's file is missing/unreadable
    // -------------------------------------------------------------------------

    @Test
    void readOutputsFile_missingFile_returnsEmptyWithoutThrowing(@TempDir Path tempDir) {
        AnsibleCLI task = newTask();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Path missing = tempDir.resolve("does-not-exist.json");

        AnsibleCLI.OutputsFileRead result = task.readOutputsFile(runContext, missing, true, 10_000_000L);

        assertThat(result.payload().isEmpty(), is(true));
        assertThat(result.oversizedBytes(), is(0L));
    }

    @Test
    void readOutputsFile_malformedJson_returnsEmptyWithoutThrowing(@TempDir Path tempDir) throws Exception {
        AnsibleCLI task = newTask();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Path malformed = tempDir.resolve("bad.json");
        Files.writeString(malformed, "{not valid json");

        AnsibleCLI.OutputsFileRead result = task.readOutputsFile(runContext, malformed, true, 10_000_000L);

        assertThat(result.payload().isEmpty(), is(true));
        assertThat(result.oversizedBytes(), is(0L));
    }

    @Test
    void readOutputsFile_validFile_parsesPayloadWithoutSlurpingAString(@TempDir Path tempDir) throws Exception {
        AnsibleCLI task = newTask();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Path valid = tempDir.resolve("good.json");
        Files.writeString(valid, "{\"playbooks\":[{\"plays\":[]}]}");

        AnsibleCLI.OutputsFileRead result = task.readOutputsFile(runContext, valid, true, 10_000_000L);

        assertThat(result.payload().isPresent(), is(true));
        assertThat(result.payload().get().get("playbooks"), is(instanceOf(List.class)));
    }

    // The payload can be hundreds of MB (the volume that caused #126): it must be rejected on the
    // file's own size, never deserialized first, or the hang is traded for a worker OOM.
    @Test
    void readOutputsFile_fileLargerThanBound_isRejectedWithoutBeingParsed(@TempDir Path tempDir) throws Exception {
        AnsibleCLI task = newTask();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Path oversized = tempDir.resolve("oversized.json");
        // deliberately unparseable: a parse attempt would fail the test instead of returning the size
        Files.writeString(oversized, "x".repeat(2_000));

        AnsibleCLI.OutputsFileRead result = task.readOutputsFile(runContext, oversized, true, 1_000L);

        assertThat(result.payload().isEmpty(), is(true));
        assertThat(result.oversizedBytes(), is(2_000L));
    }

    @Test
    void readOutputsFile_deletesTheFileOnceConsumed(@TempDir Path tempDir) throws Exception {
        AnsibleCLI task = newTask();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Path valid = tempDir.resolve("good.json");
        Files.writeString(valid, "{\"playbooks\":[{\"plays\":[]}]}");

        task.readOutputsFile(runContext, valid, true, 10_000_000L);

        // in ALL mode this file holds raw per-host results; it must not linger in plaintext
        assertThat(Files.exists(valid), is(false));
    }

    @Test
    void failOnOversizedOutputsFile_throwsSameActionableError() {
        IllegalStateException e = assertThrows(
            IllegalStateException.class,
            () -> AnsibleCLI.failOnOversizedOutputsFile(2_000L, 1_000L)
        );

        assertThat(e.getMessage(), containsString("maxOutputsSize"));
        assertThat(e.getMessage(), containsString("outputsMode: EXPLICIT"));
        assertThat(e.getMessage(), containsString("2000"));
    }

    @Test
    void failOnOversizedOutputsFile_nothingOversized_doesNotThrow() {
        assertDoesNotThrow(() -> AnsibleCLI.failOnOversizedOutputsFile(0L, 1_000L));
    }
}
