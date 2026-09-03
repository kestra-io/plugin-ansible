package io.kestra.plugin.ansible.cli;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Fast, deterministic unit tests for the Java-side helpers introduced to fix issue #126: rebuilding
 * the flat "outputs" list from the structured playbooks (instead of the callback duplicating every
 * per-host result on stdout), and graceful degradation when the outputs file the callback writes is
 * missing or unreadable. None of these exercise Docker/Ansible, so they run in milliseconds.
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
    // readOutputsFile: graceful degradation when the callback's file is missing/unreadable
    // -------------------------------------------------------------------------

    @Test
    void readOutputsFile_missingFile_returnsEmptyWithoutThrowing(@TempDir Path tempDir) {
        AnsibleCLI task = newTask();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Path missing = tempDir.resolve("does-not-exist.json");

        Optional<Map<String, Object>> result = task.readOutputsFile(runContext, missing, true);

        assertThat(result.isEmpty(), is(true));
    }

    @Test
    void readOutputsFile_malformedJson_returnsEmptyWithoutThrowing(@TempDir Path tempDir) throws Exception {
        AnsibleCLI task = newTask();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Path malformed = tempDir.resolve("bad.json");
        Files.writeString(malformed, "{not valid json");

        Optional<Map<String, Object>> result = task.readOutputsFile(runContext, malformed, true);

        assertThat(result.isEmpty(), is(true));
    }

    @Test
    void readOutputsFile_validFile_parsesPayloadWithoutSlurpingAString(@TempDir Path tempDir) throws Exception {
        AnsibleCLI task = newTask();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Path valid = tempDir.resolve("good.json");
        Files.writeString(valid, "{\"playbooks\":[{\"plays\":[]}]}");

        Optional<Map<String, Object>> result = task.readOutputsFile(runContext, valid, true);

        assertThat(result.isPresent(), is(true));
        assertThat(result.get().get("playbooks"), is(instanceOf(List.class)));
    }
}
