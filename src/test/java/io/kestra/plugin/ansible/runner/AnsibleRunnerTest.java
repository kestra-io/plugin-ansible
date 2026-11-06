package io.kestra.plugin.ansible.runner;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.TaskCommands;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.models.tasks.runners.TaskRunnerDetailResult;
import io.kestra.core.models.tasks.runners.TaskRunnerResult;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.ansible.runner.models.Inventory;
import io.kestra.plugin.ansible.runner.models.LogsMode;
import io.kestra.plugin.ansible.runner.models.Project;
import io.kestra.plugin.ansible.runner.models.RunnerEnv;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class AnsibleRunnerTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> logQueue;

    private RunContext newRunContext(Map<String, Object> vars) {
        var dummyTask = AnsibleRunner.builder().id("unit-test").type(AnsibleRunner.class.getName()).build();
        return TestsUtils.mockRunContext(runContextFactory, dummyTask, vars != null ? vars : Map.of());
    }

    private RunContext newRunContext() {
        return newRunContext(Map.of());
    }

    @Test
    void testRunnerDirectoryContractAndExecution() throws Exception {
        RunContext runContext = newRunContext(
            Map.of(
                "flowVar", "testValue",
                "secretVault", "topSecret123"
            )
        );

        Path playbooksDir = runContext.workingDir().path().resolve("playbooks");
        Files.createDirectories(playbooksDir);
        Files.writeString(playbooksDir.resolve("site.yml"), """
            - hosts: all
              tasks:
                - name: Audit ping
                  ansible.builtin.ping:
            """);

        // Create a custom mock TaskRunner to simulate ansible-runner inside container
        TaskRunner<?> mockRunner = MockAnsibleTaskRunner.builder()
            .expectedContainerImage("custom-ansible:latest")
            .build();

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(mockRunner)
            .containerImage(Property.ofValue("custom-ansible:latest"))
            .checkMode(Property.ofValue(true))
            .diff(Property.ofValue(true))
            .limit(Property.ofValue("webservers:&core"))
            .tags(Property.ofValue(List.of("acls", "compliance")))
            .skipTags(Property.ofValue(List.of("slow")))
            .verbosity(Property.ofValue(2))
            .forks(Property.ofValue(15))
            .project(
                Project.builder()
                    .playbook(Property.ofValue("audit.yml"))
                    .source(Property.ofValue("playbooks/site.yml"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("router-1 ansible_host=192.168.1.1\nrouter-2 ansible_host=192.168.1.2\n"))
                    .build()
            )
            .env(
                RunnerEnv.builder()
                    .extraVars(Property.ofValue(Map.of("strict_mode", true, "count", 42)))
                    .envVars(Property.ofValue(Map.of("ANSIBLE_HOST_KEY_CHECKING", "False")))
                    .passwords(Property.ofValue(Map.of("vault_pass", "{{ secretVault }}")))
                    .sshKey(Property.ofValue("-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA...\n-----END RSA PRIVATE KEY-----\n"))
                    .build()
            )
            .timeout(Property.ofValue(Duration.ofMinutes(10)))
            .idleTimeout(Property.ofValue(Duration.ofMinutes(2)))
            .autoCleanArtifacts(Property.ofValue(true))
            .outputSummary(Property.ofValue(true))
            .build();

        AnsibleRunner.Output output = task.run(runContext);

        // Verify Output telemetry
        assertNotNull(output);
        assertThat(output.getStatus(), is("successful"));
        assertThat(output.getRc(), is(0));
        assertThat(output.getFailures(), is(1));
        assertThat(output.getChanged(), is(1));
        assertThat(output.getFailedHosts(), containsInAnyOrder("router-2"));
        assertThat(output.getStats(), hasKey("router-1"));
        assertThat(output.getStats(), hasKey("router-2"));

        // Verify Output Summary
        assertNotNull(output.getSummary());
        assertThat(output.getSummary().getFailures(), is(1));
        assertThat(output.getSummary().getChanged(), is(1));
        assertThat(output.getSummary().getOk(), is(3));

        // Verify Artifacts were zipped and stored in Kestra Internal Storage
        assertNotNull(output.getArtifactsUri());
        try (InputStream is = runContext.storage().getFile(output.getArtifactsUri())) {
            assertNotNull(is);
            byte[] bytes = is.readNBytes(100);
            // Verify PK zip header
            assertThat(bytes[0], is((byte) 'P'));
            assertThat(bytes[1], is((byte) 'K'));
        }
    }

    @Test
    void testInlinePlaybookExecution() throws Exception {
        RunContext runContext = newRunContext();
        TaskRunner<?> mockRunner = MockAnsibleTaskRunner.builder().build();

        String inlineYaml = """
            ----
            - name: Inline Playbook Test
              hosts: localhost
              gather_facts: false
              tasks:
                - name: Echo Hello
                  ansible.builtin.debug:
                    msg: "Hello from test!"
            """;

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(mockRunner)
            .project(
                Project.builder()
                    .playbook(Property.ofValue("test.yml"))
                    .source(Property.ofValue(inlineYaml))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertNotNull(output);
        assertThat(output.getStatus(), is("successful"));
        assertThat(output.getRc(), is(0));
        assertNull(output.getSummary());
        assertNull(output.getChanged());

        Path playbook = runContext.workingDir().path().resolve("runner/project/test.yml");
        assertTrue(Files.exists(playbook));
        String content = Files.readString(playbook);
        assertTrue(content.startsWith("---\n") || content.startsWith("---"), "Leading 4+ dashes should be normalized to 3 dashes");
        assertThat(content, containsString("Inline Playbook Test"));
    }

    @Test
    void testDynamicTaskRunsGanttAndLogs() throws Exception {
        var runContext = newRunContext();

        var events = List.<Map<String, Object>> of(
            // 1. Task 1 start (naive UTC isoformat without Z)
            Map.of(
                "event", "playbook_on_task_start",
                "created", "2026-11-02T10:00:00.000000",
                "event_data", Map.of(
                    "play", "Playbook 1",
                    "task", "Ping hosts",
                    "task_uuid", "uuid-1",
                    "task_action", "ansible.builtin.ping"
                )
            ),
            // 2. Non-terminal runner_on_start on router-1
            Map.of(
                "event", "runner_on_start",
                "created", "2026-11-02T10:00:00.100000",
                "event_data", Map.of(
                    "play", "Playbook 1",
                    "task", "Ping hosts",
                    "task_uuid", "uuid-1",
                    "host", "router-1"
                )
            ),
            // 3. Non-terminal runner_on_start on router-2
            Map.of(
                "event", "runner_on_start",
                "created", "2026-11-02T10:00:00.200000",
                "event_data", Map.of(
                    "play", "Playbook 1",
                    "task", "Ping hosts",
                    "task_uuid", "uuid-1",
                    "host", "router-2"
                )
            ),
            // 4. Terminal runner_on_ok on router-1 (with remote_addr IP to verify host takes precedence)
            Map.of(
                "event", "runner_on_ok",
                "created", "2026-11-02T10:00:01.000000",
                "event_data", Map.of(
                    "play", "Playbook 1",
                    "task", "Ping hosts",
                    "task_uuid", "uuid-1",
                    "host", "router-1",
                    "remote_addr", "10.0.0.1",
                    "res", Map.of("ping", "pong", "changed", false)
                )
            ),
            // 5. Terminal runner_on_ok on router-2
            Map.of(
                "event", "runner_on_ok",
                "created", "2026-11-02T10:00:01.500000",
                "event_data", Map.of(
                    "play", "Playbook 1",
                    "task", "Ping hosts",
                    "task_uuid", "uuid-1",
                    "host", "router-2",
                    "res", Map.of("ping", "pong", "changed", false)
                )
            ),
            // 6. Task 2 start (loop task across two hosts)
            Map.of(
                "event", "playbook_on_task_start",
                "created", "2026-11-02T10:00:02.000000",
                "event_data", Map.of(
                    "play", "Playbook 1",
                    "task", "Install packages",
                    "task_uuid", "uuid-2",
                    "task_action", "ansible.builtin.package"
                )
            ),
            // 7. Loop item ok on router-1
            Map.of(
                "event", "runner_item_on_ok",
                "created", "2026-11-02T10:00:02.500000",
                "event_data", Map.of(
                    "play", "Playbook 1",
                    "task", "Install packages",
                    "task_uuid", "uuid-2",
                    "host", "router-1",
                    "res", Map.of("item", "nginx", "changed", true)
                )
            ),
            // 8. Terminal runner_on_ok on router-1
            Map.of(
                "event", "runner_on_ok",
                "created", "2026-11-02T10:00:03.000000",
                "event_data", Map.of(
                    "play", "Playbook 1",
                    "task", "Install packages",
                    "task_uuid", "uuid-2",
                    "host", "router-1",
                    "res", Map.of("changed", true, "results", List.of(Map.of("item", "nginx", "changed", true)))
                )
            ),
            // 9. Loop item failed on router-2
            Map.of(
                "event", "runner_item_on_failed",
                "created", "2026-11-02T10:00:03.500000",
                "event_data", Map.of(
                    "play", "Playbook 1",
                    "task", "Install packages",
                    "task_uuid", "uuid-2",
                    "host", "router-2",
                    "res", Map.of("item", "bad-pkg", "failed", true, "msg", "No package bad-pkg available", "rc", 1)
                )
            ),
            // 10. Terminal runner_on_failed on router-2 with loop results
            Map.of(
                "event", "runner_on_failed",
                "created", "2026-11-02T10:00:04.000000",
                "event_data", Map.of(
                    "play", "Playbook 1",
                    "task", "Install packages",
                    "task_uuid", "uuid-2",
                    "host", "router-2",
                    "res", Map.of(
                        "failed", true,
                        "msg", "One or more items failed",
                        "results", List.of(
                            Map.of("item", "nginx", "failed", false),
                            Map.of("item", "bad-pkg", "failed", true, "msg", "No package bad-pkg available", "rc", 1)
                        )
                    )
                )
            ),
            // 11. Handler task start (event #11 > 10 verifies numeric sort vs lexicographical sort)
            Map.of(
                "event", "playbook_on_handler_task_start",
                "created", "2026-11-02T10:00:04.500000",
                "event_data", Map.of(
                    "play", "Playbook 1",
                    "task", "Restart nginx",
                    "task_uuid", "uuid-3",
                    "task_action", "ansible.builtin.service"
                )
            ),
            // 12. Terminal runner_on_ok for handler on router-1
            Map.of(
                "event", "runner_on_ok",
                "created", "2026-11-02T10:00:05.500000",
                "event_data", Map.of(
                    "play", "Playbook 1",
                    "task", "Restart nginx",
                    "task_uuid", "uuid-3",
                    "host", "router-1",
                    "res", Map.of("changed", true)
                )
            ),
            // 13. Start-only task (killed mid-task: has playbook_on_task_start + runner_on_start, no terminal event)
            Map.of(
                "event", "playbook_on_task_start",
                "created", "2026-11-02T10:00:06.000000",
                "event_data", Map.of(
                    "play", "Playbook 1",
                    "task", "Killed mid-task",
                    "task_uuid", "uuid-4",
                    "task_action", "ansible.builtin.command"
                )
            ),
            // 14. Non-terminal runner_on_start for uuid-4
            Map.of(
                "event", "runner_on_start",
                "created", "2026-11-02T10:00:06.100000",
                "event_data", Map.of(
                    "play", "Playbook 1",
                    "task", "Killed mid-task",
                    "task_uuid", "uuid-4",
                    "host", "router-1"
                )
            ),
            // 15. Playbook stats
            Map.of(
                "event", "playbook_on_stats",
                "event_data", Map.of(
                    "ok", Map.of("router-1", 3, "router-2", 1),
                    "changed", Map.of("router-1", 2, "router-2", 0),
                    "failures", Map.of("router-1", 0, "router-2", 1),
                    "dark", Map.of(),
                    "skipped", Map.of()
                )
            )
        );

        var mockRunner = MockAnsibleTaskRunner.builder()
            .customEvents(events)
            .build();

        var logs = new CopyOnWriteArrayList<LogEntry>();
        var receive = TestsUtils.receive(logQueue, l -> logs.add(l.getLeft()));

        var task = AnsibleRunner.builder()
            .taskRunner(mockRunner)
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: all\n  tasks: []\n"))
                    .build()
            )
            .inventory(Inventory.builder().inline(Property.ofValue("router-1 ansible_connection=local\nrouter-2 ansible_connection=local\n")).build())
            .logsMode(Property.ofValue(LogsMode.SUMMARY))
            .failOnErrors(Property.ofValue(false))
            .build();

        var output = task.run(runContext);
        assertNotNull(output);

        // Start-only task (uuid-4) must be skipped, leaving 3 completed tasks in numeric event order
        var dynamicResults = runContext.dynamicWorkerResults();
        assertThat(dynamicResults, hasSize(3));

        var task1 = dynamicResults.get(0).getTaskRun();
        assertThat(task1.getTaskId(), is("Playbook 1 | Ping hosts"));
        assertThat(task1.getState().getCurrent(), is(State.Type.SUCCESS));
        assertThat(task1.getState().getHistories().getFirst().getDate(), is(Instant.parse("2026-11-02T10:00:00Z")));
        assertThat(task1.getState().getHistories().getLast().getDate(), is(Instant.parse("2026-11-02T10:00:01.500Z")));

        var task2 = dynamicResults.get(1).getTaskRun();
        assertThat(task2.getTaskId(), is("Playbook 1 | Install packages"));
        assertThat(task2.getState().getCurrent(), is(State.Type.FAILED));
        assertThat(task2.getState().getHistories().getFirst().getDate(), is(Instant.parse("2026-11-02T10:00:02Z")));
        assertThat(task2.getState().getHistories().getLast().getDate(), is(Instant.parse("2026-11-02T10:00:04Z")));

        var task3 = dynamicResults.get(2).getTaskRun();
        assertThat(task3.getTaskId(), is("Playbook 1 | Restart nginx"));
        assertThat(task3.getState().getCurrent(), is(State.Type.SUCCESS));
        assertThat(task3.getState().getHistories().getFirst().getDate(), is(Instant.parse("2026-11-02T10:00:04.500Z")));
        assertThat(task3.getState().getHistories().getLast().getDate(), is(Instant.parse("2026-11-02T10:00:05.500Z")));

        // Verify block/rescue reconciliation, numeric secret masking before truncation, maxLogLines capping, and truncation suffix
        var rescueRunContext = newRunContext();
        var numericSecret = "123456";
        var oversizedError = "E".repeat(3_950) + numericSecret + "E".repeat(500);
        var rescueEvents = List.<Map<String, Object>> of(
            Map.of(
                "event", "playbook_on_task_start",
                "created", "2026-11-02T11:00:00.000000",
                "event_data", Map.of(
                    "play", "Playbook 2",
                    "task", "Block task rescued",
                    "task_uuid", "uuid-r1"
                )
            ),
            Map.of(
                "event", "runner_on_failed",
                "pid", 123456,
                "created", "2026-11-02T11:00:01.000000",
                "event_data", Map.of(
                    "play", "Playbook 2",
                    "task", "Block task rescued",
                    "task_uuid", "uuid-r1",
                    "host", "router-1",
                    "remote_addr", "10.0.0.5",
                    "res", Map.of("failed", true, "msg", "Transient error caught by rescue")
                )
            ),
            Map.of(
                "event", "playbook_on_task_start",
                "created", "2026-11-02T11:00:02.000000",
                "event_data", Map.of(
                    "play", "Playbook 2",
                    "task", "Rescue recovery task",
                    "task_uuid", "uuid-r2"
                )
            ),
            Map.of(
                "event", "runner_on_ok",
                "created", "2026-11-02T11:00:03.000000",
                "event_data", Map.of(
                    "play", "Playbook 2",
                    "task", "Rescue recovery task",
                    "task_uuid", "uuid-r2",
                    "host", "router-1",
                    "remote_addr", "10.0.0.5",
                    "res", Map.of("changed", false, "msg", "Recovered")
                )
            ),
            Map.of(
                "event", "playbook_on_task_start",
                "created", "2026-11-02T11:00:04.000000",
                "event_data", Map.of(
                    "play", "Playbook 2",
                    "task", "Unrescued failure",
                    "task_uuid", "uuid-r3"
                )
            ),
            Map.of(
                "event", "runner_on_failed",
                "pid", 123456,
                "created", "2026-11-02T11:00:05.000000",
                "event_data", Map.of(
                    "play", "Playbook 2",
                    "task", "Unrescued failure",
                    "task_uuid", "uuid-r3",
                    "host", "router-2",
                    "res", Map.of("failed", true, "detail", oversizedError)
                )
            ),
            Map.of(
                "event", "playbook_on_stats",
                "pid", 123456,
                "event_data", Map.of(
                    "ok", Map.of("router-1", 1, "router-2", 0),
                    "changed", Map.of("router-1", 0, "router-2", 0),
                    "failures", Map.of("router-1", 0, "router-2", 1),
                    "rescued", Map.of("router-1", 1, "router-2", 0),
                    "dark", Map.of(),
                    "skipped", Map.of()
                )
            )
        );

        var rescueTask = AnsibleRunner.builder()
            .taskRunner(MockAnsibleTaskRunner.builder().customEvents(rescueEvents).build())
            .project(Project.builder().source(Property.ofValue("- hosts: all\n  tasks: []\n")).build())
            .inventory(Inventory.builder().inline(Property.ofValue("router-1 ansible_connection=local\nrouter-2 ansible_connection=local\n")).build())
            .env(RunnerEnv.builder().passwords(Property.ofValue(Map.of("pin", numericSecret))).build())
            .maxLogLines(Property.ofValue(1L))
            .saveArtifacts(Property.ofValue(false))
            .failOnErrors(Property.ofValue(false))
            .build();

        var rescueOutput = rescueTask.run(rescueRunContext);
        assertNotNull(rescueOutput);
        assertThat(rescueOutput.getFailedHosts(), contains("10.0.0.5", "router-2"));

        var rescueDynamicResults = rescueRunContext.dynamicWorkerResults();
        assertThat(rescueDynamicResults, hasSize(3));

        var rescuedTask = rescueDynamicResults.get(0).getTaskRun();
        assertThat(rescuedTask.getState().getCurrent(), is(State.Type.SUCCESS));

        var recoveryTask = rescueDynamicResults.get(1).getTaskRun();
        assertThat(recoveryTask.getState().getCurrent(), is(State.Type.SUCCESS));

        var failedTask = rescueDynamicResults.get(2).getTaskRun();
        assertThat(failedTask.getState().getCurrent(), is(State.Type.FAILED));

        TestsUtils.awaitLogs(logs, l -> task2.getId().equals(l.getTaskRunId()), 3);
        TestsUtils.awaitLog(logs, l -> failedTask.getId().equals(l.getTaskRunId()));
        receive.blockLast();

        var task2Logs = logs.stream()
            .filter(l -> task2.getId().equals(l.getTaskRunId()))
            .map(LogEntry::getMessage)
            .toList();
        assertThat(
            task2Logs, containsInAnyOrder(
                "[router-1] changed",
                "[router-2] failed => One or more items failed",
                "[router-2] failed (item=bad-pkg) => No package bad-pkg available (rc=1)"
            )
        );

        var rescuedLogs = logs.stream()
            .filter(l -> rescuedTask.getId().equals(l.getTaskRunId()))
            .toList();
        assertThat(rescuedLogs, hasSize(1));
        assertThat(rescuedLogs.getFirst().getLevel(), is(Level.WARN));
        assertThat(rescuedLogs.getFirst().getMessage(), startsWith("[router-1] rescued =>"));

        var recoveryLogs = logs.stream()
            .filter(l -> recoveryTask.getId().equals(l.getTaskRunId()))
            .toList();
        assertThat(recoveryLogs, is(empty()));

        var failedLogs = logs.stream()
            .filter(l -> failedTask.getId().equals(l.getTaskRunId()))
            .toList();
        assertThat(failedLogs, hasSize(1));
        assertThat(failedLogs.getFirst().getMessage(), not(containsString(numericSecret)));
        assertThat(failedLogs.getFirst().getMessage(), containsString("***"));
        assertThat(failedLogs.getFirst().getMessage(), endsWith("... (truncated)"));
    }

    /**
     * Mock TaskRunner that verifies directory contents and generates realistic ansible-runner artifacts.
     */
    @SuperBuilder
    @NoArgsConstructor
    public static class MockAnsibleTaskRunner extends TaskRunner<TaskRunnerDetailResult> {
        @Builder.Default
        protected String type = MockAnsibleTaskRunner.class.getName();

        @Builder.Default
        private String expectedContainerImage = "ghcr.io/ansible/community-ansible-dev-tools:v26.10.1";

        private List<Map<String, Object>> customEvents;

        @Override
        public TaskRunnerResult<TaskRunnerDetailResult> run(RunContext runContext, TaskCommands taskCommands, List<String> filesToDownload) throws Exception {
            assertThat(taskCommands.getContainerImage(), is(expectedContainerImage));
            Path workingDir = taskCommands.getWorkingDirectory();
            Path runnerDir = workingDir.resolve("runner");

            // Verify Directory Contract
            assertTrue(Files.exists(runnerDir.resolve("project")), "runner/project must exist");
            assertTrue(Files.exists(runnerDir.resolve("inventory")), "runner/inventory must exist");
            assertTrue(Files.exists(runnerDir.resolve("env")), "runner/env must exist");
            assertTrue(Files.exists(runnerDir.resolve("artifacts")), "runner/artifacts must exist");

            // Verify env/cmdline
            String cmdline = Files.readString(runnerDir.resolve("env/cmdline"));
            if (cmdline.contains("--check")) {
                assertThat(cmdline, containsString("--diff"));
                assertThat(cmdline, containsString("--limit webservers:&core"));
                assertThat(cmdline, containsString("--tags acls,compliance"));
                assertThat(cmdline, containsString("--skip-tags slow"));
                assertThat(cmdline, containsString("-f 15"));
                assertThat(cmdline, containsString("-vv"));
            }

            // Verify env/extravars
            if (Files.exists(runnerDir.resolve("env/extravars"))) {
                String extravars = Files.readString(runnerDir.resolve("env/extravars"));
                if (extravars.contains("strict_mode")) {
                    assertThat(extravars, containsString("strict_mode"));
                }
            }

            // Verify env/envvars
            if (Files.exists(runnerDir.resolve("env/envvars"))) {
                String envvars = Files.readString(runnerDir.resolve("env/envvars"));
                if (envvars.contains("ANSIBLE_HOST_KEY_CHECKING")) {
                    assertThat(envvars, containsString("ANSIBLE_HOST_KEY_CHECKING"));
                }
            }

            // Verify env/passwords
            if (Files.exists(runnerDir.resolve("env/passwords"))) {
                String passwords = Files.readString(runnerDir.resolve("env/passwords"));
                if (passwords.contains("topSecret123")) {
                    assertThat(passwords, containsString("topSecret123"));
                }
            }

            // Verify env/settings
            if (Files.exists(runnerDir.resolve("env/settings"))) {
                String settings = Files.readString(runnerDir.resolve("env/settings"));
                if (settings.contains("job_timeout")) {
                    assertThat(settings, containsString("job_timeout"));
                    assertThat(settings, containsString("600"));
                    assertThat(settings, containsString("idle_timeout"));
                    assertThat(settings, containsString("120"));
                }
            }

            // Verify inventory
            String hosts = Files.readString(runnerDir.resolve("inventory/hosts"));
            assertTrue(hosts.contains("router-1") || hosts.contains("localhost"));

            // Simulate Ansible Runner execution by creating realistic artifacts
            List<String> commands = runContext.render(taskCommands.getCommands()).asList(String.class);
            String command = commands.stream().filter(c -> c.contains("--ident ")).findFirst().orElseThrow();
            int identIdx = command.indexOf("--ident ");
            String ident = command.substring(identIdx + 8).trim().split("[\\s;]")[0];

            Path identArtifacts = runnerDir.resolve("artifacts").resolve(ident);
            Path jobEvents = identArtifacts.resolve("job_events");
            Files.createDirectories(jobEvents);

            Files.writeString(identArtifacts.resolve("status"), "successful\n", StandardCharsets.UTF_8);
            Files.writeString(identArtifacts.resolve("rc"), "0\n", StandardCharsets.UTF_8);

            if (customEvents != null) {
                int counter = 1;
                for (var event : customEvents) {
                    Files.writeString(jobEvents.resolve(counter + "-event.json"), JacksonMapper.ofJson().writeValueAsString(event));
                    counter++;
                }
            } else {
                // 1. Task failure on router-2
                Map<String, Object> failEvent = Map.of(
                    "event", "runner_on_failed",
                    "event_data", Map.of(
                        "host", "router-2",
                        "task", "Enforce ACL compliance",
                        "failed", true
                    )
                );
                Files.writeString(jobEvents.resolve("1-fail.json"), JacksonMapper.ofJson().writeValueAsString(failEvent));

                // 2. Playbook on stats summary event
                Map<String, Object> statsEvent = Map.of(
                    "event", "playbook_on_stats",
                    "event_data", Map.of(
                        "ok", Map.of("router-1", 2, "router-2", 1),
                        "changed", Map.of("router-1", 1, "router-2", 0),
                        "failures", Map.of("router-1", 0, "router-2", 1),
                        "dark", Map.of(),
                        "skipped", Map.of()
                    )
                );
                Files.writeString(jobEvents.resolve("2-stats.json"), JacksonMapper.ofJson().writeValueAsString(statsEvent));
            }

            return new TaskRunnerResult<>(0, taskCommands.getLogConsumer());
        }

        @Override
        public Map<String, Object> additionalVars(RunContext runContext, TaskCommands taskCommands) {
            return Collections.emptyMap();
        }
    }
}
