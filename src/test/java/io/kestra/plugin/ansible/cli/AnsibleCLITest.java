package io.kestra.plugin.ansible.cli;

import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.assets.AssetIdentifier;
import io.kestra.core.models.assets.AssetsDeclaration;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.DynamicTaskRunLog;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.PullPolicy;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import reactor.core.publisher.Flux;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class AnsibleCLITest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storage;

    @Inject
    private TestAssetManagerFactory assetManagerFactory;

    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> logQueue;

    @Test
    void extractInventoryAssetInputs_shouldParseHostsAsInputsOnly() {
        var inventory = """
            [webservers]
            web1.example.com ansible_user=ubuntu
            web2.example.com

            [webservers:vars]
            ansible_port=22

            [parents:children]
            webservers

            [ungrouped]
            standalone-host # inline comment
            ; full line comment
            invalid:host
            """;

        var inputs = AnsibleCLI.extractInventoryAssetInputs(inventory);

        assertThat(inputs.size(), is(3));
        assertThat(
            inputs.stream().map(AssetIdentifier::id).toList(), contains(
                "web1.example.com",
                "web2.example.com",
                "standalone-host"
            )
        );
        assertThat(
            inputs.stream().map(AssetIdentifier::type).distinct().toList(), contains(
                "io.kestra.plugin.ee.assets.VM"
            )
        );
    }

    @Test
    void extractInventoryAssetInputs_shouldIgnoreEmptyOrVarsOnlyInventory() {
        var inventory = """
            [all:vars]
            ansible_user=ubuntu
            """;

        var inputs = AnsibleCLI.extractInventoryAssetInputs(inventory);

        assertThat(inputs, is(empty()));
    }

    @Test
    @SuppressWarnings("unchecked")
    void run() throws Exception {
        String envKey = "MY_KEY";
        String envValue = "MY_VALUE";

        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .env(Property.ofExpression(JacksonMapper.ofJson().writeValueAsString(Map.of("{{ inputs.envKey }}", "{{ inputs.envValue }}"))))
            .beforeCommands(Property.ofExpression(JacksonMapper.ofJson().writeValueAsString(List.of("echo {{ workingDir }}"))))
            .commands(
                Property.ofExpression(
                    JacksonMapper.ofJson().writeValueAsString(
                        List.of(
                            "echo \"::{\\\"outputs\\\":{" +
                                "\\\"customEnv\\\":\\\"$" + envKey + "\\\"" +
                                "}}::\"",
                            "ansible --version",
                            "ansible-galaxy collection list | tr -d ' \n' | xargs -0 -I {} echo '::{\"outputs\":{}}::'",
                            "echo {{ workingDir }}"
                        )
                    )
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of("envKey", envKey, "envValue", envValue));

        ScriptOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        assertThat(runOutput.getVars().get("customEnv"), is(envValue));
    }

    @Test
    @SuppressWarnings("unchecked")
    void run_withPlugin() throws Exception {
        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .inputFiles(
                Map.of(
                    "playbooks/playbook.yml",
                    storage.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), this.getClass().getClassLoader().getResourceAsStream("playbooks/playbook.yml"))
                        .toString()
                )
            )
            .commands(
                Property.ofValue(
                    List.of(
                        "ansible --version",
                        "ansible-playbook -i localhost -c local playbooks/playbook.yml"
                    )
                )
            )
            .outputLogFile(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        ScriptOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        assertThat(runOutput.getOutputFiles().size(), is(1));
        assertThat(runOutput.getOutputFiles().get("log"), is(notNullValue()));

        // Get outputs for 6 tasks
        List<Map<String, Object>> outputs = ((List<Map<String, Object>>) runOutput.getVars().get("outputs"));
        assertThat(outputs.size(), is(6));

        // Verify output via 'withItems' (First task)
        List<String> resultMessage = ((List<Map<String, Object>>) outputs.getFirst().get("results"))
            .stream()
            .map(map -> (String) map.get("msg"))
            .toList();
        assertThat(resultMessage.size(), is(2));
        assertThat(resultMessage, containsInAnyOrder("another_variable", "a_variable"));

        // Verify output via variable (Fourth task)
        String resultFromVar = (String) ((Map<String, Object>) outputs.get(3).get("myOutput")).get("stdout");
        assertThat(resultFromVar, is("Test output"));

        // Verify output list message (5th task)
        List<String> messages = (List<String>) outputs.get(4).get("msg");
        assertThat(messages.size(), is(2));
        assertThat(messages, containsInAnyOrder("Multiline message : line 1", "Multiline message : line 2"));

        // Verify output list message (6th task)
        List<String> additionalMessages = (List<String>) outputs.get(5).get("msg");
        assertThat(additionalMessages.size(), is(2));
        assertThat(additionalMessages, containsInAnyOrder("Multiline message : line 3", "Multiline message : line 4"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void run_withExplicitOutputs() throws Exception {
        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .outputsMode(Property.ofValue(AnsibleCLI.OutputsMode.EXPLICIT))
            .inputFiles(
                Map.of(
                    "playbooks/playbook-explicit-outputs.yml", storage.put(
                        TenantService.MAIN_TENANT,
                        null,
                        URI.create("/" + IdUtils.create() + ".ion"),
                        this.getClass().getClassLoader().getResourceAsStream("playbooks/playbook-explicit-outputs.yml")
                    ).toString()
                )
            )
            .commands(
                Property.ofValue(
                    List.of(
                        "ansible-playbook -i localhost -c local playbooks/playbook-explicit-outputs.yml"
                    )
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        AnsibleCLI.AnsibleOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));

        // only the values declared via the kestra module are exposed
        Object outputs = runOutput.getVars().get("outputs");
        assertThat(outputs, is(instanceOf(Map.class)));
        Map<String, Object> declared = (Map<String, Object>) outputs;
        assertThat(declared.keySet(), containsInAnyOrder("records_updated", "skipped_status"));
        assertThat(declared.get("records_updated"), is(3));
        assertThat(declared.get("skipped_status"), is("skipped"));

        // the sensitive value never reaches the outputs
        assertThat(JacksonMapper.ofJson().writeValueAsString(runOutput.getVars()), not(containsString("super-secret-value")));

        // structured playbooks keep names and statuses, payloads are redacted
        List<AnsibleCLI.AnsibleOutput.PlaybookOutput> playbooks = runOutput.getPlaybooks();
        assertThat(playbooks.size(), is(1));
        List<AnsibleCLI.AnsibleOutput.TaskOutput> tasks = playbooks.getFirst().getPlays().getFirst().getTasks();
        assertThat(tasks.size(), is(4));
        assertThat(tasks.get(2).getHosts().getFirst().getStatus(), is("skipped"));
        for (AnsibleCLI.AnsibleOutput.TaskOutput t : tasks) {
            Map<String, Object> result = (Map<String, Object>) t.getHosts().getFirst().getResult();
            assertThat(result.keySet(), contains("changed"));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void run_withExplicitOutputs_loopIsNotCollected() throws Exception {
        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .outputsMode(Property.ofValue(AnsibleCLI.OutputsMode.EXPLICIT))
            .inputFiles(
                Map.of(
                    "playbooks/playbook-explicit-loop.yml", storage.put(
                        TenantService.MAIN_TENANT,
                        null,
                        URI.create("/" + IdUtils.create() + ".ion"),
                        this.getClass().getClassLoader().getResourceAsStream("playbooks/playbook-explicit-loop.yml")
                    ).toString()
                )
            )
            .commands(
                Property.ofValue(
                    List.of(
                        "ansible-playbook -i localhost -c local playbooks/playbook-explicit-loop.yml"
                    )
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        AnsibleCLI.AnsibleOutput runOutput = execute.run(runContext);

        // a looped kestra task runs fine, but declared outputs are not collected
        assertThat(runOutput.getExitCode(), is(0));
        Object outputs = runOutput.getVars().get("outputs");
        assertThat(outputs, is(instanceOf(Map.class)));
        assertThat(((Map<String, Object>) outputs).isEmpty(), is(true));
    }

    @Test
    @SuppressWarnings("unchecked")
    void run_withExplicitOutputs_keepsErrorMessageOnFailure() throws Exception {
        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .outputsMode(Property.ofValue(AnsibleCLI.OutputsMode.EXPLICIT))
            .inputFiles(
                Map.of(
                    "playbooks/playbook-explicit-failure.yml", storage.put(
                        TenantService.MAIN_TENANT,
                        null,
                        URI.create("/" + IdUtils.create() + ".ion"),
                        this.getClass().getClassLoader().getResourceAsStream("playbooks/playbook-explicit-failure.yml")
                    ).toString()
                )
            )
            .commands(
                Property.ofValue(
                    List.of(
                        "ansible-playbook -i localhost -c local playbooks/playbook-explicit-failure.yml"
                    )
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        AnsibleCLI.AnsibleOutput runOutput = execute.run(runContext);

        // ignore_errors keeps the run green so the structured output can be inspected
        assertThat(runOutput.getExitCode(), is(0));

        AnsibleCLI.AnsibleOutput.TaskOutput failedTask = runOutput.getPlaybooks().getFirst()
            .getPlays().getFirst().getTasks().getFirst();
        Map<String, Object> failedResult = (Map<String, Object>) failedTask.getHosts().getFirst().getResult();

        // the error reason is preserved for debugging, the rest of the payload is redacted
        assertThat(failedTask.getHosts().getFirst().getStatus(), is("failed"));
        assertThat(failedResult.keySet(), containsInAnyOrder("changed", "msg"));
        assertThat(failedResult.containsKey("cmd"), is(false));
        assertThat(failedResult.containsKey("stdout"), is(false));
    }

    @Test
    @SuppressWarnings("unchecked")
    void run_withStructuredOutputs() throws Exception {
        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .inputFiles(
                Map.of(
                    "playbooks/playbook.yml", storage.put(
                        TenantService.MAIN_TENANT,
                        null,
                        URI.create("/" + IdUtils.create() + ".ion"),
                        this.getClass().getClassLoader().getResourceAsStream("playbooks/playbook.yml")
                    ).toString()
                )
            )
            .commands(
                Property.ofValue(
                    List.of(
                        "ansible-playbook -i localhost -c local playbooks/playbook.yml"
                    )
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        AnsibleCLI.AnsibleOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));

        List<AnsibleCLI.AnsibleOutput.PlaybookOutput> playbooks = runOutput.getPlaybooks();
        assertThat(playbooks, is(notNullValue()));
        assertThat(playbooks.size(), is(1));

        AnsibleCLI.AnsibleOutput.PlaybookOutput pb0 = playbooks.getFirst();
        assertThat(pb0.getPlays(), is(notNullValue()));
        assertThat(pb0.getPlays().size(), is(1));

        AnsibleCLI.AnsibleOutput.PlayOutput play0 = pb0.getPlays().getFirst();
        // no explicit name in playbook.yml => ansible uses hosts pattern as play name
        assertThat(
            play0.getName(), anyOf(
                is("localhost"),
                is("unnamed_play"),
                is("implicit_play")
            )
        );

        assertThat(play0.getTasks(), is(notNullValue()));
        assertThat(play0.getTasks().size(), is(6));

        List<AnsibleCLI.AnsibleOutput.TaskOutput> tasks = play0.getTasks();
        assertThat(tasks.get(0).getName(), is("Print items"));
        assertThat(tasks.get(1).getName(), is("Create file"));
        assertThat(tasks.get(2).getName(), is("Register output file to var"));
        assertThat(tasks.get(3).getName(), is("Print return information from the previous task"));
        assertThat(tasks.get(4).getName(), is("Prints two lines of messages"));
        assertThat(tasks.get(5).getName(), is("Prints two other lines of messages"));

        for (AnsibleCLI.AnsibleOutput.TaskOutput t : tasks) {
            assertThat(t.getHosts(), is(notNullValue()));
            assertThat(t.getHosts().size(), is(1));
            assertThat(t.getHosts().getFirst().getHost(), is("localhost"));
            assertThat(t.getHosts().getFirst().getStatus(), is("ok"));
            assertThat(t.getHosts().getFirst().getResult(), is(instanceOf(Map.class)));
        }

        // Task 1: loop -> results msgs
        Map<String, Object> t1res = (Map<String, Object>) tasks.get(0).getHosts().getFirst().getResult();
        List<Map<String, Object>> results = (List<Map<String, Object>>) t1res.get("results");
        List<String> resultMessage = results.stream()
            .map(m -> (String) m.get("msg"))
            .toList();
        assertThat(resultMessage.size(), is(2));
        assertThat(resultMessage, containsInAnyOrder("another_variable", "a_variable"));

        // Task 4: debug var myOutput -> myOutput.stdout
        Map<String, Object> t4res = (Map<String, Object>) tasks.get(3).getHosts().getFirst().getResult();
        String resultFromVar = (String) ((Map<String, Object>) t4res.get("myOutput")).get("stdout");
        assertThat(resultFromVar, is("Test output"));

        // Task 5: debug msg list
        Map<String, Object> t5res = (Map<String, Object>) tasks.get(4).getHosts().getFirst().getResult();
        List<String> messages = (List<String>) t5res.get("msg");
        assertThat(messages.size(), is(2));
        assertThat(messages, containsInAnyOrder("Multiline message : line 1", "Multiline message : line 2"));

        // Task 6: debug msg list
        Map<String, Object> t6res = (Map<String, Object>) tasks.get(5).getHosts().getFirst().getResult();
        List<String> additionalMessages = (List<String>) t6res.get("msg");
        assertThat(additionalMessages.size(), is(2));
        assertThat(additionalMessages, containsInAnyOrder("Multiline message : line 3", "Multiline message : line 4"));
    }

    @Test
    void run_emitsLogsUnderDynamicTaskRuns() throws Exception {
        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .inputFiles(
                Map.of(
                    "playbooks/playbook.yml", storage.put(
                        TenantService.MAIN_TENANT,
                        null,
                        URI.create("/" + IdUtils.create() + ".ion"),
                        this.getClass().getClassLoader().getResourceAsStream("playbooks/playbook.yml")
                    ).toString()
                )
            )
            .commands(
                Property.ofValue(
                    List.of("ansible-playbook -i localhost -c local playbooks/playbook.yml")
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        Flux<LogEntry> receive = TestsUtils.receive(logQueue, l -> logs.add(l.getLeft()));

        AnsibleCLI.AnsibleOutput runOutput = execute.run(runContext);
        assertThat(runOutput.getExitCode(), is(0));

        // Each Ansible task produces a dynamic taskrun (issue kestra-ee#8520): its host results
        // must be emitted as logs tagged with the dynamic taskrun id, not the parent root.
        Set<String> dynamicTaskRunIds = runContext.dynamicWorkerResults().stream()
            .map(r -> r.getTaskRun().getId())
            .collect(Collectors.toSet());
        assertThat(dynamicTaskRunIds, is(not(empty())));

        TestsUtils.awaitLog(logs, l -> l.getTaskRunId() != null && dynamicTaskRunIds.contains(l.getTaskRunId()));
        receive.blockLast();

        List<LogEntry> dynamicLogs = List.copyOf(logs).stream()
            .filter(l -> l.getTaskRunId() != null && dynamicTaskRunIds.contains(l.getTaskRunId()))
            .toList();

        // logs are attributed to each task's dynamic taskrun, all on the single localhost host
        assertThat(dynamicLogs, is(not(empty())));
        assertThat(dynamicLogs.stream().allMatch(l -> l.getMessage() != null && l.getMessage().contains("[localhost]")), is(true));
        assertThat(dynamicLogs.stream().anyMatch(l -> l.getMessage().contains("[localhost] ok")), is(true));
        // attemptNumber MUST be 0: logs are grouped per taskrun by (taskRunId, attemptNumber) and a
        // single-attempt taskrun's logs live under attempt 0
        assertThat(dynamicLogs.stream().allMatch(l -> l.getAttemptNumber() != null && l.getAttemptNumber() == 0), is(true));
    }

    // issue #126 (follow-up): taskLogs() used to inline the full per-host result (facts, full
    // stdout/stderr) into an INFO log line unconditionally, for every host of every task in ALL
    // mode - as much log volume as the whole outputs payload. Mirror Ansible's own default
    // verbosity: concise by default, full detail kept only for failures.
    @Test
    void taskLogs_summarizesOkAndSkippedHosts_withoutFullPayload() {
        AnsibleCLI execute = AnsibleCLI.builder().id(IdUtils.create()).type(AnsibleCLI.class.getName()).build();

        String irrelevantDetail = "a lot of facts/stdout that should not be logged by default";
        AnsibleCLI.AnsibleOutput.HostResult ok = AnsibleCLI.AnsibleOutput.HostResult.builder()
            .host("host1").status("ok").result(Map.of("changed", true, "stdout", irrelevantDetail)).build();
        AnsibleCLI.AnsibleOutput.HostResult skipped = AnsibleCLI.AnsibleOutput.HostResult.builder()
            .host("host2").status("skipped").result(Map.of("changed", false, "skip_reason", irrelevantDetail)).build();

        AnsibleCLI.AnsibleOutput.TaskOutput task = AnsibleCLI.AnsibleOutput.TaskOutput.builder()
            .uid("play|Install nginx").name("Install nginx").hosts(List.of(ok, skipped)).build();

        List<DynamicTaskRunLog> logs = execute.taskLogs(task);

        assertThat(logs.size(), is(2));

        assertThat(logs.get(0).level(), is(Level.INFO));
        assertThat(logs.get(0).message(), containsString("[host1] ok"));
        assertThat(logs.get(0).message(), containsString("changed=true"));
        assertThat(logs.get(0).message(), containsString("Install nginx"));
        assertThat(logs.get(0).message(), not(containsString(irrelevantDetail)));

        assertThat(logs.get(1).level(), is(Level.INFO));
        assertThat(logs.get(1).message(), containsString("[host2] skipped"));
        assertThat(logs.get(1).message(), containsString("changed=false"));
        assertThat(logs.get(1).message(), not(containsString(irrelevantDetail)));
    }

    @Test
    void taskLogs_keepsFullDetailForFailedAndUnreachableHosts() {
        AnsibleCLI execute = AnsibleCLI.builder().id(IdUtils.create()).type(AnsibleCLI.class.getName()).build();

        AnsibleCLI.AnsibleOutput.HostResult failed = AnsibleCLI.AnsibleOutput.HostResult.builder()
            .host("host1").status("failed")
            .result(Map.of("changed", false, "msg", "Destination directory does not exist", "rc", 2))
            .build();
        AnsibleCLI.AnsibleOutput.HostResult unreachable = AnsibleCLI.AnsibleOutput.HostResult.builder()
            .host("host2").status("unreachable")
            .result(Map.of("msg", "Failed to connect to the host via ssh"))
            .build();

        AnsibleCLI.AnsibleOutput.TaskOutput task = AnsibleCLI.AnsibleOutput.TaskOutput.builder()
            .uid("play|Copy file").name("Copy file").hosts(List.of(failed, unreachable)).build();

        List<DynamicTaskRunLog> logs = execute.taskLogs(task);

        assertThat(logs.get(0).level(), is(Level.ERROR));
        assertThat(logs.get(0).message(), containsString("Destination directory does not exist"));

        assertThat(logs.get(1).level(), is(Level.ERROR));
        assertThat(logs.get(1).message(), containsString("Failed to connect to the host via ssh"));
    }

    @Test
    void taskLogs_capsLineLength_withTruncationMarker() {
        AnsibleCLI execute = AnsibleCLI.builder().id(IdUtils.create()).type(AnsibleCLI.class.getName()).build();

        String hugeMessage = "e".repeat(AnsibleCLI.MAX_LOG_LINE_LENGTH * 5);
        AnsibleCLI.AnsibleOutput.HostResult failed = AnsibleCLI.AnsibleOutput.HostResult.builder()
            .host("host1").status("failed").result(Map.of("msg", hugeMessage)).build();

        AnsibleCLI.AnsibleOutput.TaskOutput task = AnsibleCLI.AnsibleOutput.TaskOutput.builder()
            .uid("play|Big failure").name("Big failure").hosts(List.of(failed)).build();

        String message = execute.taskLogs(task).getFirst().message();

        assertThat(message.length(), lessThan(AnsibleCLI.MAX_LOG_LINE_LENGTH + 200));
        assertThat(message, containsString("truncated"));
        // nothing is actually lost: point users at where the full result still lives
        assertThat(message, containsString("outputs"));
        assertThat(message, containsString("playbooks"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void run_withStructuredOutputs_multipleHosts() throws Exception {
        // inventory with 2 hosts
        String inventory = """
            [local_servers]
            localhost1 ansible_connection=local
            localhost2 ansible_connection=local
            """;

        // playbook from test resources
        URI inventoryUri = storage.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new java.io.ByteArrayInputStream(inventory.getBytes(java.nio.charset.StandardCharsets.UTF_8))
        );

        URI playbookUri = storage.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            this.getClass().getClassLoader().getResourceAsStream("playbooks/playbook_with_multiple_hosts.yml")
        );

        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .inputFiles(
                Map.of(
                    "inventory.ini", inventoryUri.toString(),
                    "playbook.yml", playbookUri.toString()
                )
            )
            .commands(
                Property.ofValue(
                    List.of(
                        "ansible-playbook -i inventory.ini -c local playbook.yml"
                    )
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        AnsibleCLI.AnsibleOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));

        List<AnsibleCLI.AnsibleOutput.PlaybookOutput> playbooks = runOutput.getPlaybooks();
        assertThat(playbooks, is(notNullValue()));
        assertThat(playbooks.size(), is(1));

        AnsibleCLI.AnsibleOutput.PlaybookOutput pb0 = playbooks.getFirst();
        assertThat(pb0.getPlays(), is(notNullValue()));
        assertThat(pb0.getPlays().size(), is(1));

        AnsibleCLI.AnsibleOutput.PlayOutput play0 = pb0.getPlays().getFirst();
        assertThat(play0.getName(), is("Hello World Playbook"));

        assertThat(play0.getTasks(), is(notNullValue()));
        assertThat(play0.getTasks().size(), is(2));

        List<AnsibleCLI.AnsibleOutput.TaskOutput> tasks = play0.getTasks();
        assertThat(tasks.get(0).getName(), is("Task 1"));
        assertThat(tasks.get(1).getName(), is("Task 2"));

        // Each task should have 2 host results
        for (AnsibleCLI.AnsibleOutput.TaskOutput t : tasks) {
            assertThat(t.getHosts(), is(notNullValue()));
            assertThat(t.getHosts().size(), is(2));

            List<String> hosts = t.getHosts().stream()
                .map(AnsibleCLI.AnsibleOutput.HostResult::getHost)
                .toList();
            assertThat(hosts, containsInAnyOrder("localhost1", "localhost2"));

            for (AnsibleCLI.AnsibleOutput.HostResult hr : t.getHosts()) {
                assertThat(hr.getStatus(), is("ok"));
                assertThat(hr.getResult(), is(instanceOf(Map.class)));
            }
        }

        // Task 1 message per host
        Map<String, Object> t1h0 = (Map<String, Object>) tasks.get(0).getHosts().get(0).getResult();
        Map<String, Object> t1h1 = (Map<String, Object>) tasks.get(0).getHosts().get(1).getResult();
        assertThat(
            List.of(t1h0.get("msg"), t1h1.get("msg")),
            everyItem(is("Hello from task 1"))
        );

        // Task 2 message per host
        Map<String, Object> t2h0 = (Map<String, Object>) tasks.get(1).getHosts().get(0).getResult();
        Map<String, Object> t2h1 = (Map<String, Object>) tasks.get(1).getHosts().get(1).getResult();
        assertThat(
            List.of(t2h0.get("msg"), t2h1.get("msg")),
            everyItem(is("Hello from task 2"))
        );
    }

    // issue #126: in ALL mode, the callback used to emit every per-host result twice on the
    // single stdout line printed by _log_kestra_outputs() in kestra_logger.py — once as a flat
    // "outputs" list (_add_results_to_kestra_outputs) and once nested under "playbooks"
    // (_add_host_result). On large host sets that oversized single line could hang execution
    // or push the final task outputs over the queue message size limit, with no error surfaced.
    // This drives the real production callback through a real ansible-playbook run, bypassing
    // AnsibleCLI's higher-level merge, to observe the raw payload it actually prints on stdout.
    @Test
    @SuppressWarnings("unchecked")
    void run_allMode_doesNotDuplicatePerHostResults_issue126() throws Exception {
        String marker = "issue126-" + IdUtils.create() + "-" + "m".repeat(2000);

        String inventory = """
            [all]
            host1 ansible_connection=local
            host2 ansible_connection=local
            host3 ansible_connection=local
            """;

        String playbook = """
            ---
            - hosts: all
              gather_facts: false
              tasks:
                - name: Emit marker
                  ansible.builtin.debug:
                    msg: "%s"
            """.formatted(marker);

        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .commands(Property.ofValue(List.of("ansible-playbook -i inventory.ini playbook.yml")))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        CommandsWrapper baseWrapper = new CommandsWrapper(runContext)
            .withWarningOnStdErr(false)
            .withDockerOptions(execute.getDocker())
            .withTaskRunner(execute.getTaskRunner())
            .withContainerImage(runContext.render(execute.getContainerImage()).as(String.class).orElseThrow())
            .withInterpreter(Property.ofValue(List.of("/bin/bash", "-c")))
            .withNamespaceFiles(null)
            .withOutputFiles(List.of())
            .withEnableOutputDirectory(true);

        Path workingDir = baseWrapper.getWorkingDirectory();

        // Reuse AnsibleCLI's own bundled ansible.cfg + callback + kestra module, so this drives
        // the exact production callback rather than a hand-rolled copy of it.
        Map<String, String> inputFiles = new HashMap<>(execute.finalInputFiles(runContext, workingDir));
        inputFiles.put("inventory.ini", inventory);
        inputFiles.put("playbook.yml", playbook);
        PluginUtilsService.createInputFilesRaw(runContext, workingDir, inputFiles);

        CommandsWrapper commandWrapper = baseWrapper
            .withEnv(Map.of(AnsibleCLI.OUTPUTS_MODE_ENV, "all"))
            .withCommands(Property.ofValue(List.of("ansible-playbook -i inventory.ini playbook.yml")));

        ScriptOutput out = commandWrapper.run();

        assertThat(out.getExitCode(), is(0));

        Map<String, Object> vars = out.getVars();
        assertThat(vars, is(notNullValue()));

        // The flat "outputs" list must no longer carry per-host results: they are only sent
        // once, nested under "playbooks".
        Object outputs = vars.get("outputs");
        assertThat(outputs, is(instanceOf(List.class)));
        assertThat((List<?>) outputs, is(empty()));

        // The marker must appear exactly once per host in the raw payload printed by the
        // callback, not twice.
        String json = JacksonMapper.ofJson().writeValueAsString(vars);
        assertThat(countOccurrences(json, marker), is(3L));
    }

    private static long countOccurrences(String haystack, String needle) {
        long count = 0;
        int index = 0;
        while ((index = haystack.indexOf(needle, index)) != -1) {
            count++;
            index += needle.length();
        }
        return count;
    }

    // issue #126 (follow-up): the previous test only proves the callback no longer duplicates
    // per-host results on stdout. This exercises AnsibleCLI's own flattenHostResults() through
    // the full execute.run() path, proving the flat "outputs" list is correctly rebuilt from the
    // structured "playbooks" instead of silently staying empty.
    @Test
    @SuppressWarnings("unchecked")
    void run_allMode_rebuildsFlatOutputsFromStructuredPlaybooks_issue126() throws Exception {
        String marker = "issue126-full-" + IdUtils.create();

        String inventory = """
            [all]
            host1 ansible_connection=local
            host2 ansible_connection=local
            host3 ansible_connection=local
            """;

        String playbook = """
            ---
            - hosts: all
              gather_facts: false
              tasks:
                - name: Emit marker
                  ansible.builtin.debug:
                    msg: "%s"
            """.formatted(marker);

        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .inputFiles(
                Map.of(
                    "inventory.ini", inventory,
                    "playbook.yml", playbook
                )
            )
            .commands(Property.ofValue(List.of("ansible-playbook -i inventory.ini playbook.yml")))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        AnsibleCLI.AnsibleOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));

        // One entry per host, in execution order, rebuilt from "playbooks" rather than received a
        // second time from the callback.
        List<Map<String, Object>> outputs = (List<Map<String, Object>>) runOutput.getVars().get("outputs");
        assertThat(outputs, is(notNullValue()));
        assertThat(outputs.size(), is(3));
        assertThat(outputs.stream().map(m -> m.get("msg")).toList(), everyItem(is(marker)));
    }

    // issue #126 (follow-up): flattenHostResults() is what rebuilds the flat "outputs" list;
    // verify it preserves execution order across multiple playbooks/plays/tasks/hosts without
    // needing a real Ansible run.
    @Test
    void flattenHostResults_preservesOrder_acrossPlaybooksPlaysTasksHosts() {
        AnsibleCLI.AnsibleOutput.HostResult pb1Task1Host1 = AnsibleCLI.AnsibleOutput.HostResult.builder()
            .host("host1").status("ok").result(Map.of("marker", "pb1-task1-host1")).build();
        AnsibleCLI.AnsibleOutput.HostResult pb1Task1Host2 = AnsibleCLI.AnsibleOutput.HostResult.builder()
            .host("host2").status("ok").result(Map.of("marker", "pb1-task1-host2")).build();
        AnsibleCLI.AnsibleOutput.TaskOutput pb1Task1 = AnsibleCLI.AnsibleOutput.TaskOutput.builder()
            .uid("pb1-task1").hosts(List.of(pb1Task1Host1, pb1Task1Host2)).build();

        AnsibleCLI.AnsibleOutput.HostResult pb1Task2Host1 = AnsibleCLI.AnsibleOutput.HostResult.builder()
            .host("host1").status("ok").result(Map.of("marker", "pb1-task2-host1")).build();
        AnsibleCLI.AnsibleOutput.TaskOutput pb1Task2 = AnsibleCLI.AnsibleOutput.TaskOutput.builder()
            .uid("pb1-task2").hosts(List.of(pb1Task2Host1)).build();

        AnsibleCLI.AnsibleOutput.PlayOutput pb1Play = AnsibleCLI.AnsibleOutput.PlayOutput.builder()
            .name("Play 1").tasks(List.of(pb1Task1, pb1Task2)).build();
        AnsibleCLI.AnsibleOutput.PlaybookOutput pb1 = AnsibleCLI.AnsibleOutput.PlaybookOutput.builder()
            .plays(List.of(pb1Play)).build();

        AnsibleCLI.AnsibleOutput.HostResult pb2Task1Host1 = AnsibleCLI.AnsibleOutput.HostResult.builder()
            .host("host1").status("ok").result(Map.of("marker", "pb2-task1-host1")).build();
        AnsibleCLI.AnsibleOutput.TaskOutput pb2Task1 = AnsibleCLI.AnsibleOutput.TaskOutput.builder()
            .uid("pb2-task1").hosts(List.of(pb2Task1Host1)).build();
        AnsibleCLI.AnsibleOutput.PlayOutput pb2Play = AnsibleCLI.AnsibleOutput.PlayOutput.builder()
            .name("Play 1").tasks(List.of(pb2Task1)).build();
        AnsibleCLI.AnsibleOutput.PlaybookOutput pb2 = AnsibleCLI.AnsibleOutput.PlaybookOutput.builder()
            .plays(List.of(pb2Play)).build();

        // a playbook/play/task with no hosts must not raise
        AnsibleCLI.AnsibleOutput.PlaybookOutput empty = AnsibleCLI.AnsibleOutput.PlaybookOutput.builder()
            .plays(List.of(AnsibleCLI.AnsibleOutput.PlayOutput.builder().name("Empty").tasks(List.of()).build()))
            .build();

        List<Map<String, Object>> flattened = AnsibleCLI.flattenHostResults(List.of(pb1, empty, pb2));

        assertThat(
            flattened.stream().map(m -> m.get("marker")).toList(),
            contains("pb1-task1-host1", "pb1-task1-host2", "pb1-task2-host1", "pb2-task1-host1")
        );
        // rebuilt entries are the same Map instances, not re-copied
        assertThat(flattened.getFirst(), sameInstance(pb1Task1Host1.getResult()));
    }

    // issue #126: without a bound, ALL mode's raw per-host results (duplicated ~3x in the final
    // task outputs between the flat list, vars.playbooks, and the top-level playbooks field) can
    // grow without limit. Verify the guard triggers with an actionable message, and stays silent
    // under the limit, without needing a multi-megabyte real Ansible run.
    @Test
    void enforceAllModeOutputsBound_throwsActionableError_whenOverLimit() {
        String bigValue = "x".repeat(AnsibleCLI.MAX_ALL_MODE_OUTPUTS_BYTES + 1024);
        List<Map<String, Object>> rawOutputs = List.of(Map.of("stdout", bigValue));
        List<AnsibleCLI.AnsibleOutput.PlaybookOutput> playbooks = List.of();

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> AnsibleCLI.enforceAllModeOutputsBound(rawOutputs, playbooks)
        );

        assertThat(exception.getMessage(), containsString("outputsMode: EXPLICIT"));
        assertThat(exception.getMessage(), containsString("safety limit"));
    }

    @Test
    void enforceAllModeOutputsBound_doesNotThrow_whenUnderLimit() {
        List<Map<String, Object>> rawOutputs = List.of(Map.of("changed", false, "msg", "ok"));
        List<AnsibleCLI.AnsibleOutput.PlaybookOutput> playbooks = List.of();

        assertDoesNotThrow(() -> AnsibleCLI.enforceAllModeOutputsBound(rawOutputs, playbooks));
    }

    // issue #126: the size bound must trip before any per-task dynamic taskrun/log is emitted -
    // emitDynamicTaskRuns() (and the per-host log lines it builds via taskLogs()) is itself a
    // source of log volume, so if it ran before the bound check, the bound would not actually
    // protect against a hang caused by log fan-out. enforceAllModeOutputsBound() runs inside the
    // per-command merge loop, strictly before emitDynamicTaskRuns() which only runs after that
    // loop completes, so an over-limit run must fail with zero dynamic worker results emitted.
    @Test
    void run_allMode_exceedsSizeBound_failsBeforeEmittingDynamicLogs_issue126() throws Exception {
        // ~6 MB; ends up counted twice by enforceAllModeOutputsBound (once via the flat outputs
        // list, once via the structured playbooks), comfortably crossing the 10 MB bound.
        String bigValue = "x".repeat(6 * 1024 * 1024);

        String inventory = """
            [all]
            host1 ansible_connection=local
            """;

        String playbook = """
            ---
            - hosts: all
              gather_facts: false
              tasks:
                - name: Emit big value
                  ansible.builtin.debug:
                    msg: "%s"
            """.formatted(bigValue);

        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .inputFiles(Map.of("inventory.ini", inventory, "playbook.yml", playbook))
            .commands(Property.ofValue(List.of("ansible-playbook -i inventory.ini playbook.yml")))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> execute.run(runContext));
        assertThat(exception.getMessage(), containsString("outputsMode: EXPLICIT"));

        assertThat(runContext.dynamicWorkerResults(), is(empty()));
    }

    @Test
    @SuppressWarnings("unchecked")
    void run_withStructuredOutputs_multipleCommands_mergesPlaybooksAndLogs() throws Exception {
        // inventory for the multi-host playbook
        String inventory = """
            [local_servers]
            localhost1 ansible_connection=local
            localhost2 ansible_connection=local
            """;

        URI inventoryUri = storage.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new java.io.ByteArrayInputStream(inventory.getBytes(java.nio.charset.StandardCharsets.UTF_8))
        );

        // reuse existing playbooks already used by other tests
        URI pb1Uri = storage.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            this.getClass().getClassLoader().getResourceAsStream("playbooks/playbook.yml")
        );

        URI pb2Uri = storage.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            this.getClass().getClassLoader().getResourceAsStream("playbooks/playbook_with_multiple_hosts.yml")
        );

        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .inputFiles(
                Map.of(
                    // first playbook (single host)
                    "playbooks/playbook.yml", pb1Uri.toString(),
                    // second playbook (multi-host)
                    "playbooks/playbook_with_multiple_hosts.yml", pb2Uri.toString(),
                    // inventory for second playbook
                    "inventory.ini", inventoryUri.toString()
                )
            )
            // Two distinct ansible-playbook commands => triggers multi-command behavior
            .commands(
                Property.ofValue(
                    List.of(
                        "ansible-playbook -i localhost -c local playbooks/playbook.yml",
                        "ansible-playbook -i inventory.ini -c local playbooks/playbook_with_multiple_hosts.yml"
                    )
                )
            )
            .outputLogFile(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        AnsibleCLI.AnsibleOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));

        // ---------------------------
        // Verify merged "playbooks" in vars
        // ---------------------------
        Object maybePlaybooksVar = runOutput.getVars().get("playbooks");
        assertThat(maybePlaybooksVar, is(instanceOf(List.class)));

        List<Object> playbooksVar = (List<Object>) maybePlaybooksVar;
        assertThat(playbooksVar.size(), is(2));

        // ---------------------------
        // Verify merged raw outputs (backward compatible)
        // ---------------------------
        // playbook.yml => 6 tasks * 1 host = 6 outputs
        // playbook_with_multiple_hosts.yml => 2 tasks * 2 hosts = 4 outputs
        // total = 10
        List<Map<String, Object>> outputs = (List<Map<String, Object>>) runOutput.getVars().get("outputs");
        assertThat(outputs, is(notNullValue()));
        assertThat(outputs.size(), is(10));

        // ---------------------------
        // Verify merged structured playbooks
        // ---------------------------
        List<AnsibleCLI.AnsibleOutput.PlaybookOutput> playbooks = runOutput.getPlaybooks();
        assertThat(playbooks, is(notNullValue()));
        assertThat(playbooks.size(), is(2));

        // Playbook 1 (playbooks/playbook.yml)
        AnsibleCLI.AnsibleOutput.PlaybookOutput pb0 = playbooks.get(0);
        assertThat(pb0.getPlays(), is(notNullValue()));
        assertThat(pb0.getPlays().size(), is(1));
        assertThat(pb0.getPlays().getFirst().getTasks().size(), is(6));

        // Playbook 2 (playbooks/playbook_with_multiple_hosts.yml)
        AnsibleCLI.AnsibleOutput.PlaybookOutput pb1 = playbooks.get(1);
        assertThat(pb1.getPlays(), is(notNullValue()));
        assertThat(pb1.getPlays().size(), is(1));
        assertThat(pb1.getPlays().getFirst().getName(), is("Hello World Playbook"));
        assertThat(pb1.getPlays().getFirst().getTasks().size(), is(2));

        // Verify log file content contains traces of BOTH playbooks
        URI logUri = runOutput.getOutputFiles().get("log");
        assertThat(logUri, is(notNullValue()));

        String logContent;
        try (java.io.InputStream is = storage.get(TenantService.MAIN_TENANT, null, logUri)) {
            logContent = new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }

        // From playbook.yml we expect to see task "Print items"
        assertThat(logContent, containsString("\"task\":\"Print items\""));

        // From playbook_with_multiple_hosts.yml we expect to see play "Hello World Playbook"
        assertThat(logContent, containsString("\"play\":\"Hello World Playbook\""));
    }

    @Test
    void run_withAutoAssets_inventory() throws Exception {
        assetManagerFactory.reset();

        var inventory = """
            [webservers]
            web1.example.com
            web2.example.com

            [databases]
            db1.example.com
            """;

        var inventoryUri = storage.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new java.io.ByteArrayInputStream(inventory.getBytes(java.nio.charset.StandardCharsets.UTF_8))
        );

        var execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .assets(new AssetsDeclaration(true, null, null))
            .inputFiles(
                Map.of(
                    "inventory.ini", inventoryUri.toString()
                )
            )
            .commands(Property.ofValue(List.of("echo noop")))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        execute.run(runContext);

        var emitted = assetManagerFactory.emitter().emitted();
        assertThat(emitted.size(), is(1));

        var firstEmit = emitted.getFirst();
        assertThat(firstEmit.outputs(), empty());
        assertThat(firstEmit.inputs().size(), is(3));

        var inputIds = firstEmit.inputs().stream()
            .map(AssetIdentifier::id)
            .toList();
        assertThat(
            inputIds, containsInAnyOrder(
                "web1.example.com",
                "web2.example.com",
                "db1.example.com"
            )
        );

        for (var input : firstEmit.inputs()) {
            assertThat(input.type(), is("io.kestra.plugin.ee.assets.VM"));
        }
    }

    @Test
    void run_withAutoAssets_inventory_shouldNotReEmitExistingHosts() throws Exception {
        assetManagerFactory.reset();

        var firstInventory = """
            [webservers]
            web1.example.com ansible_user=ubuntu
            web2.example.com

            [webservers:vars]
            ansible_port=22

            [ungrouped]
            standalone-host
            web1.example.com # duplicate should be deduped
            invalid:host
            """;

        var secondInventory = """
            [webservers]
            web1.example.com
            web2.example.com

            [ungrouped]
            standalone-host
            """;

        var firstInventoryUri = storage.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new java.io.ByteArrayInputStream(firstInventory.getBytes(java.nio.charset.StandardCharsets.UTF_8))
        );
        var secondInventoryUri = storage.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new java.io.ByteArrayInputStream(secondInventory.getBytes(java.nio.charset.StandardCharsets.UTF_8))
        );

        var firstTask = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .assets(new AssetsDeclaration(true, null, null))
            .inputFiles(
                Map.of(
                    "inventory.ini", firstInventoryUri.toString()
                )
            )
            .commands(Property.ofValue(List.of("echo first run")))
            .build();

        var secondTask = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .assets(new AssetsDeclaration(true, null, null))
            .inputFiles(
                Map.of(
                    "inventory.ini", secondInventoryUri.toString()
                )
            )
            .commands(Property.ofValue(List.of("echo second run")))
            .build();

        firstTask.run(TestsUtils.mockRunContext(runContextFactory, firstTask, Map.of()));
        secondTask.run(TestsUtils.mockRunContext(runContextFactory, secondTask, Map.of()));

        var emitted = assetManagerFactory.emitter().emitted();
        assertThat(emitted.size(), is(1));
        assertThat(emitted.getFirst().inputs().size(), is(3));
    }

    @Test
    void run_withRequirementsTxt_autoInstalls() throws Exception {
        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .inputFiles(
                Map.of(
                    "requirements.txt", "proxmoxer==2.0.1\n"
                )
            )
            .commands(
                Property.ofValue(
                    List.of(
                        // Will fail if proxmoxer was not pip-installed beforehand.
                        "python -c \"import proxmoxer; print('proxmoxer imported')\""
                    )
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        ScriptOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
    }

    @Test
    void run_withRequirementsYml_autoInstalls() throws Exception {
        // Use a Galaxy role rather than a collection: roles are never bundled with Ansible,
        // so a successful `ansible-galaxy role list` for it proves the auto-install ran.
        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .inputFiles(
                Map.of(
                    "requirements.yml", """
                        ---
                        roles:
                          - name: geerlingguy.docker
                        """
                )
            )
            .commands(
                Property.ofValue(
                    List.of(
                        // Will fail (non-zero exit) if the role was not installed.
                        "ansible-galaxy role list geerlingguy.docker"
                    )
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        ScriptOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
    }

    @Test
    void run_autoInstallGalaxyDisabled_skipsRoleInstall() throws Exception {
        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .autoInstallGalaxyRequirements(Property.ofValue(false))
            .inputFiles(
                Map.of(
                    "requirements.yml", """
                        ---
                        roles:
                          - name: geerlingguy.docker
                        """
                )
            )
            .commands(
                Property.ofValue(
                    List.of(
                        // With auto-install disabled, the role must not be present.
                        // `ansible-galaxy role list <role>` only warns (exit 0) when a role
                        // is missing, so grep the listing to force a non-zero exit on absence.
                        "ansible-galaxy role list 2>/dev/null | grep -q geerlingguy.docker"
                    )
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        try {
            ScriptOutput runOutput = execute.run(runContext);
            // Some task runners surface failures via non-zero exitCode rather than throwing.
            // AssertionError is an Error (not Exception), so a passing 0 still fails the test.
            assertThat(runOutput.getExitCode(), is(not(0)));
        } catch (Exception e) {
            // Expected: command failed because the role is missing.
        }
    }

    @Test
    void run_autoInstallDisabled_skipsPipInstall() throws Exception {
        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .autoInstallPythonRequirements(Property.ofValue(false))
            .inputFiles(
                Map.of(
                    "requirements.txt", "proxmoxer==2.0.1\n"
                )
            )
            .commands(
                Property.ofValue(
                    List.of(
                        // With auto-install disabled, proxmoxer is not installed and import must fail.
                        "python -c \"import proxmoxer\""
                    )
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        try {
            ScriptOutput runOutput = execute.run(runContext);
            // Some task runners surface failures via non-zero exitCode rather than throwing.
            // AssertionError is an Error (not Exception), so a passing 0 still fails the test.
            assertThat(runOutput.getExitCode(), is(not(0)));
        } catch (Exception e) {
            // Expected: command failed because proxmoxer is missing.
        }
    }

    @Test
    void shouldReproduceVaultSerializationBug() throws Exception {

        AnsibleCLI task = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .beforeCommands(
                Property.ofValue(
                    List.of(
                        "pip install --default-timeout=60 \"ansible>=9,<10\""
                    )
                )
            )
            .inputFiles(
                Map.of(

                    "inventory.ini",
                    """
                        [target]
                        localhost ansible_connection=local
                        """,

                    "vault_vars.yml",
                    """
                        vault_secret_value: !vault |
                          $ANSIBLE_VAULT;1.1;AES256
                          35363665656162366638396161616466313965383938313366633734306266633433333265313862
                          3633646532336664623966666663386531363262336638360a363033373931376432393761613163
                          35346336383665626335346134613638663561373230616631623538313636306332383431363637
                          6665623938653066390a396563323430326335303164626661623064313234333633313431613666
                          65666131633031653630393066383663373630666532383164303837663735393030
                        """,

                    "playbook.yml",
                    """
                        ---
                        - name: Reproduce vault serialization bug
                          hosts: target
                          gather_facts: false
                          tasks:
                            - name: Hello world
                              ansible.builtin.debug:
                                msg: Hello!!

                            - name: Load vault-encrypted vars at runtime
                              ansible.builtin.include_vars:
                                file: vault_vars.yml

                            - name: Use vault-encrypted variable in a task
                              ansible.builtin.debug:
                                msg: "Vault value is {{ '{{' }} vault_secret_value {{ '}}' }}"

                            - name: Set fact from vault-encrypted variable
                              ansible.builtin.set_fact:
                                derived_value: "{{ '{{' }} vault_secret_value {{ '}}' }}"

                            - name: Use derived vault fact
                              ansible.builtin.debug:
                                msg: "Derived value is {{ '{{' }} derived_value {{ '}}' }}"
                        """
                )
            )
            .commands(
                Property.ofValue(
                    List.of(
                        "echo \"bugreport\" > /tmp/vault_pass.txt && ansible-playbook -i inventory.ini --vault-password-file /tmp/vault_pass.txt playbook.yml"
                    )
                )
            )

            .taskRunner(
                io.kestra.plugin.scripts.runner.docker.Docker.builder()
                    .type(io.kestra.plugin.scripts.runner.docker.Docker.class.getName())
                    .image("python:3.12-trixie")
                    .pullPolicy(Property.ofValue(PullPolicy.IF_NOT_PRESENT))
                    .build()
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        AnsibleCLI.AnsibleOutput output = task.run(runContext);

        assertThat(output.getExitCode(), is(0));
        assertThat(output.getPlaybooks(), is(notNullValue()));
        assertThat(output.getPlaybooks().getFirst().getPlays(), is(notNullValue()));

    }

    // issue #120: subdirectory playbook + image-set ANSIBLE_CALLBACK_PLUGINS must still capture outputs.
    @Test
    @SuppressWarnings("unchecked")
    void run_capturesOutputs_whenPlaybookInSubdir_andImageOverridesCallbackPath_issue120() throws Exception {
        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .env(Property.ofValue(Map.of("ANSIBLE_CALLBACK_PLUGINS", "/tmp/othercb")))
            .beforeCommands(Property.ofValue(List.of("mkdir -p /tmp/othercb")))
            .inputFiles(
                Map.of(
                    "inventory.ini",
                    "localhost ansible_connection=local",

                    "ansible/playbook.yml",
                    """
                        ---
                        - hosts: localhost
                          gather_facts: false
                          tasks:
                            - name: Report serial number
                              ansible.builtin.debug:
                                msg:
                                  serial_number: "SN-12345"
                        """
                )
            )
            .commands(
                Property.ofValue(
                    List.of(
                        "ansible-playbook -i inventory.ini ansible/playbook.yml"
                    )
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        AnsibleCLI.AnsibleOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));

        List<Map<String, Object>> outputs = (List<Map<String, Object>>) runOutput.getVars().get("outputs");
        assertThat(outputs, is(not(empty())));

        Map<String, Object> msg = (Map<String, Object>) outputs.getFirst().get("msg");
        assertThat(msg.get("serial_number"), is("SN-12345"));

        assertThat(runOutput.getPlaybooks(), is(not(empty())));
    }

    // issue #120: defining assets must not affect output capture; both must work in the same run.
    @Test
    @SuppressWarnings("unchecked")
    void run_capturesOutputs_andEmitsAssets_together_issue120() throws Exception {
        assetManagerFactory.reset();

        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .assets(new AssetsDeclaration(true, null, null))
            .inputFiles(
                Map.of(
                    "inventory.ini",
                    "[all]\nlocalhost ansible_connection=local\n",

                    "playbook.yml",
                    """
                        ---
                        - hosts: all
                          gather_facts: false
                          tasks:
                            - name: Report serial number
                              ansible.builtin.debug:
                                msg:
                                  serial_number: "SN-12345"
                        """
                )
            )
            .commands(Property.ofValue(List.of("ansible-playbook -i inventory.ini playbook.yml")))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        AnsibleCLI.AnsibleOutput runOutput = execute.run(runContext);

        // outputs are captured even though assets are defined
        assertThat(runOutput.getExitCode(), is(0));
        List<Map<String, Object>> outputs = (List<Map<String, Object>>) runOutput.getVars().get("outputs");
        assertThat(outputs, is(not(empty())));
        Map<String, Object> msg = (Map<String, Object>) outputs.getFirst().get("msg");
        assertThat(msg.get("serial_number"), is("SN-12345"));

        // and the asset is emitted alongside, in the same run
        var emitted = assetManagerFactory.emitter().emitted();
        assertThat(emitted.size(), is(1));
        assertThat(
            emitted.getFirst().inputs().stream().map(AssetIdentifier::id).toList(),
            contains("localhost")
        );
    }
}
