package io.kestra.plugin.ansible.cli;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
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
 * Fast, deterministic unit tests for the Java-side output helpers: the outputs size guard, the
 * byte-level merge of the stored results files, the per-host log verbosity modes, and graceful
 * degradation when the outputs file the callback writes is missing or unreadable.
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
    // checkOutputsSize: the queue/task-output guard (not the hang fix)
    // -------------------------------------------------------------------------

    private static AnsibleCLI.AnsibleOutput outputWith(String stdout) {
        var task = AnsibleCLI.AnsibleOutput.TaskOutput.builder()
            .hosts(List.of(host("h1", "ok", Map.of("stdout", stdout))))
            .build();
        var play = AnsibleCLI.AnsibleOutput.PlayOutput.builder().tasks(List.of(task)).build();
        var playbook = AnsibleCLI.AnsibleOutput.PlaybookOutput.builder().plays(List.of(play)).build();
        return AnsibleCLI.AnsibleOutput.builder()
            .vars(Map.of("outputs", Map.of()))
            .playbooks(List.of(playbook))
            .build();
    }

    @Test
    void checkOutputsSize_underLimit_doesNotThrow() {
        assertDoesNotThrow(
            () -> newTask().checkOutputsSize(outputWith("small"), 10_000_000L, AnsibleCLI.ResultsStorage.STORE)
        );
    }

    // the typed `playbooks` field is what is emitted, so it is what the bound must measure
    @Test
    void checkOutputsSize_countsTypedPlaybooks_inInlineMode() {
        var output = outputWith("x".repeat(1000));

        IllegalStateException e = assertThrows(
            IllegalStateException.class,
            () -> newTask().checkOutputsSize(output, 100L, AnsibleCLI.ResultsStorage.INLINE)
        );

        assertThat(e.getMessage(), containsString("maxOutputsSize"));
        assertThat(e.getMessage(), containsString("resultsStorage: STORE"));
        assertThat(e.getMessage(), containsString("outputsMode: EXPLICIT"));
        assertThat(e.getMessage().indexOf("resultsStorage: STORE"), lessThan(e.getMessage().indexOf("outputsMode: EXPLICIT")));
    }

    @Test
    void checkOutputsSize_overLimit_inStoreMode_doesNotSuggestStoringResults() {
        var output = outputWith("x".repeat(1000));

        IllegalStateException e = assertThrows(
            IllegalStateException.class,
            () -> newTask().checkOutputsSize(output, 100L, AnsibleCLI.ResultsStorage.STORE)
        );

        assertThat(e.getMessage(), containsString("outputsMode: EXPLICIT"));
        assertThat(e.getMessage(), not(containsString("resultsStorage: STORE")));
    }

    // -------------------------------------------------------------------------
    // mergeResultsFiles: byte-level merge of the per-command results files
    // -------------------------------------------------------------------------

    private String merge(Path target, Path... sources) throws Exception {
        var merged = AnsibleCLI.mergeResultsFiles(TestsUtils.mockRunContext(runContextFactory, newTask(), Map.of()), List.of(sources), target);
        return merged ? Files.readString(target) : null;
    }

    @Test
    void mergeResultsFiles_joinsSeveralPlaybooksAndCommands(@TempDir Path tempDir) throws Exception {
        var first = Files.writeString(tempDir.resolve("r0.json"), "[{\"plays\":[1]},{\"plays\":[2]}]");
        var second = Files.writeString(tempDir.resolve("r1.json"), "[{\"plays\":[3]}]");

        var merged = merge(tempDir.resolve("merged.json"), first, second);

        assertThat(merged, is("[{\"plays\":[1]},{\"plays\":[2]},{\"plays\":[3]}]"));
    }

    @Test
    void mergeResultsFiles_skipsEmptyListsEmptyAndMissingFiles(@TempDir Path tempDir) throws Exception {
        var emptyList = Files.writeString(tempDir.resolve("r0.json"), "[]");
        var placeholder = Files.writeString(tempDir.resolve("r1.json"), "");
        var missing = tempDir.resolve("r2.json");
        var real = Files.writeString(tempDir.resolve("r3.json"), "[{\"plays\":[]}]");

        var merged = merge(tempDir.resolve("merged.json"), emptyList, placeholder, missing, real);

        assertThat(merged, is("[{\"plays\":[]}]"));
    }

    @Test
    void mergeResultsFiles_nothingToMerge_reportsNoResults(@TempDir Path tempDir) throws Exception {
        var emptyList = Files.writeString(tempDir.resolve("r0.json"), "[]");

        assertThat(merge(tempDir.resolve("merged.json"), emptyList, tempDir.resolve("missing.json")), is(nullValue()));
    }

    @Test
    void mergeResultsFiles_skipsFileThatIsNotAJsonArray(@TempDir Path tempDir) throws Exception {
        var truncated = Files.writeString(tempDir.resolve("r0.json"), "[{\"plays\":[1]}");
        var real = Files.writeString(tempDir.resolve("r1.json"), "[{\"plays\":[2]}]");

        assertThat(merge(tempDir.resolve("merged.json"), truncated, real), is("[{\"plays\":[2]}]"));
    }

    // -------------------------------------------------------------------------
    // taskLogs: logsMode SUMMARY (default) vs FULL
    // -------------------------------------------------------------------------

    @Test
    void taskLogs_summaryMode_okHostsHaveNoResultDetail() {
        var task = AnsibleCLI.AnsibleOutput.TaskOutput.builder()
            .hosts(List.of(host("h1", "ok", Map.of("stdout", "lots of sensitive detail"))))
            .build();

        List<DynamicTaskRunLog> logs = AnsibleCLI.taskLogs(task, AnsibleCLI.LogsMode.SUMMARY, AnsibleCLI.ResultsStorage.INLINE);

        assertThat(logs.size(), is(1));
        assertThat(logs.getFirst().message(), is("[h1] ok"));
        assertThat(logs.getFirst().level(), is(Level.INFO));
    }

    @Test
    void taskLogs_summaryMode_failedHostsKeepErrorReasonOnly() {
        var task = AnsibleCLI.AnsibleOutput.TaskOutput.builder()
            .hosts(List.of(host("h1", "failed", Map.of("msg", "boom", "stdout", "lots of detail"))))
            .build();

        List<DynamicTaskRunLog> logs = AnsibleCLI.taskLogs(task, AnsibleCLI.LogsMode.SUMMARY, AnsibleCLI.ResultsStorage.INLINE);

        assertThat(logs.getFirst().message(), is("[h1] failed => boom"));
        assertThat(logs.getFirst().level(), is(Level.ERROR));
        assertThat(logs.getFirst().message(), not(containsString("lots of detail")));
    }

    @Test
    void taskLogs_fullMode_logsEntireResultPayload() {
        var task = AnsibleCLI.AnsibleOutput.TaskOutput.builder()
            .hosts(List.of(host("h1", "ok", Map.of("stdout", "full detail here"))))
            .build();

        List<DynamicTaskRunLog> logs = AnsibleCLI.taskLogs(task, AnsibleCLI.LogsMode.FULL, AnsibleCLI.ResultsStorage.INLINE);

        assertThat(logs.getFirst().message(), containsString("full detail here"));
    }

    @Test
    void taskLogs_truncatesOverlyLongLines_andPointsAtPlaybooks() {
        String longMsg = "x".repeat(10_000);
        var task = AnsibleCLI.AnsibleOutput.TaskOutput.builder()
            .hosts(List.of(host("h1", "failed", Map.of("msg", longMsg))))
            .build();

        List<DynamicTaskRunLog> logs = AnsibleCLI.taskLogs(task, AnsibleCLI.LogsMode.SUMMARY, AnsibleCLI.ResultsStorage.INLINE);

        assertThat(logs.getFirst().message().length(), lessThan(longMsg.length()));
        assertThat(logs.getFirst().message(), containsString("truncated"));
        assertThat(logs.getFirst().message(), containsString("`playbooks`"));
    }

    @Test
    void taskLogs_truncatedLine_inStoreMode_pointsAtResultsUri() {
        var task = AnsibleCLI.AnsibleOutput.TaskOutput.builder()
            .hosts(List.of(host("h1", "failed", Map.of("msg", "x".repeat(10_000)))))
            .build();

        var logs = AnsibleCLI.taskLogs(task, AnsibleCLI.LogsMode.SUMMARY, AnsibleCLI.ResultsStorage.STORE);

        assertThat(logs.getFirst().message(), containsString("`resultsUri`"));
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
            () -> AnsibleCLI.failOnOversizedOutputsFile(2_000L, 1_000L, AnsibleCLI.ResultsStorage.STORE)
        );

        assertThat(e.getMessage(), containsString("maxOutputsSize"));
        assertThat(e.getMessage(), containsString("outputsMode: EXPLICIT"));
        assertThat(e.getMessage(), containsString("2000"));
    }

    @Test
    void failOnOversizedOutputsFile_nothingOversized_doesNotThrow() {
        assertDoesNotThrow(() -> AnsibleCLI.failOnOversizedOutputsFile(0L, 1_000L, AnsibleCLI.ResultsStorage.STORE));
    }

    // -------------------------------------------------------------------------
    // readOutputsFile / createContainerWritableFile: issue #131, non-root container users
    // -------------------------------------------------------------------------

    @Test
    void readOutputsFile_emptyFile_isTreatedAsMissingWithoutThrowing(@TempDir Path tempDir) throws Exception {
        AnsibleCLI task = newTask();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Path empty = tempDir.resolve("kestra-outputs-0.json");
        Files.createFile(empty);

        AnsibleCLI.OutputsFileRead result = task.readOutputsFile(runContext, empty, true, 10_000_000L);

        assertThat(result.payload().isEmpty(), is(true));
        assertThat(result.oversizedBytes(), is(0L));
        // the placeholder is never written to by the callback on this path (crash, or a non-playbook
        // command); it must not linger in the working directory, e.g. to be swept up by an
        // outputFiles glob such as "*.json"
        assertThat(Files.exists(empty), is(false));
    }

    @Test
    void createContainerWritableFile_createsFileWritableButNotReadableByOthers() throws Exception {
        AnsibleCLI task = newTask();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        // createContainerWritableFile goes through runContext.workingDir(), same as AnsibleCLI.run()
        Path outputsFile = runContext.workingDir().path().resolve("kestra-outputs-0.json");

        AnsibleCLI.createContainerWritableFile(runContext, outputsFile);

        assertThat(Files.exists(outputsFile), is(true));

        // POSIX permissions only apply on filesystems that support them (e.g. not Windows)
        if (outputsFile.getFileSystem().supportedFileAttributeViews().contains("posix")) {
            // 0622: others can write (non-root container user opens with O_TRUNC) but not read,
            // since the file may end up holding secrets a playbook fetched
            assertThat(
                Files.getPosixFilePermissions(outputsFile),
                hasItem(PosixFilePermission.OTHERS_WRITE)
            );
            assertThat(
                Files.getPosixFilePermissions(outputsFile),
                not(hasItem(PosixFilePermission.OTHERS_READ))
            );
        }
    }

    @Test
    void createContainerWritableFile_calledTwice_isIdempotent() throws Exception {
        AnsibleCLI task = newTask();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        // "log" resolves to the same path on every command of a multi-command task
        Path logFile = runContext.workingDir().path().resolve("log");

        AnsibleCLI.createContainerWritableFile(runContext, logFile);
        assertDoesNotThrow(() -> AnsibleCLI.createContainerWritableFile(runContext, logFile));

        assertThat(Files.exists(logFile), is(true));
    }
}
