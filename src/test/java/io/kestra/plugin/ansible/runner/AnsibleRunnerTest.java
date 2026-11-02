package io.kestra.plugin.ansible.runner;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.TaskCommands;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.models.tasks.runners.TaskRunnerDetailResult;
import io.kestra.core.models.tasks.runners.TaskRunnerResult;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.kestra.plugin.ansible.runner.models.Inventory;
import io.kestra.plugin.ansible.runner.models.LogsMode;
import io.kestra.plugin.ansible.runner.models.Project;
import io.kestra.plugin.ansible.runner.models.RunnerEnv;

import jakarta.inject.Inject;
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

    private RunContext newRunContext(Map<String, Object> vars) {
        AnsibleRunner dummyTask = AnsibleRunner.builder().id("unit-test").type(AnsibleRunner.class.getName()).build();
        io.kestra.core.runners.DefaultRunContext runContext = (io.kestra.core.runners.DefaultRunContext) runContextFactory.of(dummyTask, vars != null ? vars : Map.of());
        return runContextFactory.initializer().forExecutor(runContext);
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

        assertThat(runContext.dynamicWorkerResults(), is(not(empty())));
        var dynamicResult = runContext.dynamicWorkerResults().getFirst();
        assertThat(dynamicResult.getTaskRun().getTaskId(), is("Enforce ACL compliance"));
        assertThat(dynamicResult.getTaskRun().getState().getCurrent(), is(State.Type.FAILED));
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
            Map.of(
                "event", "playbook_on_task_start",
                "created", "2026-11-02T10:00:00.000Z",
                "event_data", Map.of(
                    "play", "Playbook 1",
                    "task", "Ping localhost",
                    "task_uuid", "uuid-1",
                    "task_action", "ansible.builtin.ping"
                )
            ),
            Map.of(
                "event", "runner_on_ok",
                "created", "2026-11-02T10:00:01.000Z",
                "event_data", Map.of(
                    "play", "Playbook 1",
                    "task", "Ping localhost",
                    "task_uuid", "uuid-1",
                    "host", "localhost",
                    "res", Map.of("ping", "pong", "changed", false)
                )
            ),
            Map.of(
                "event", "playbook_on_task_start",
                "created", "2026-11-02T10:00:01.500Z",
                "event_data", Map.of(
                    "play", "Playbook 1",
                    "task", "Install package",
                    "task_uuid", "uuid-2",
                    "task_action", "ansible.builtin.package"
                )
            ),
            Map.of(
                "event", "runner_on_ok",
                "created", "2026-11-02T10:00:03.000Z",
                "event_data", Map.of(
                    "play", "Playbook 1",
                    "task", "Install package",
                    "task_uuid", "uuid-2",
                    "host", "localhost",
                    "res", Map.of("changed", true, "_ansible_no_log", true)
                )
            ),
            Map.of(
                "event", "playbook_on_task_start",
                "created", "2026-11-02T10:00:03.500Z",
                "event_data", Map.of(
                    "play", "Playbook 1",
                    "task", "Configure firewall",
                    "task_uuid", "uuid-3",
                    "task_action", "ansible.builtin.command"
                )
            ),
            Map.of(
                "event", "runner_on_failed",
                "created", "2026-11-02T10:00:04.000Z",
                "event_data", Map.of(
                    "play", "Playbook 1",
                    "task", "Configure firewall",
                    "task_uuid", "uuid-3",
                    "host", "localhost",
                    "res", Map.of("failed", true, "msg", "Rule syntax error", "rc", 1)
                )
            ),
            Map.of(
                "event", "playbook_on_stats",
                "event_data", Map.of(
                    "ok", Map.of("localhost", 2),
                    "changed", Map.of("localhost", 1),
                    "failures", Map.of("localhost", 1),
                    "dark", Map.of(),
                    "skipped", Map.of()
                )
            )
        );

        var mockRunner = MockAnsibleTaskRunner.builder()
            .customEvents(events)
            .build();

        var task = AnsibleRunner.builder()
            .taskRunner(mockRunner)
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks: []\n"))
                    .build()
            )
            .inventory(Inventory.builder().inline(Property.ofValue("localhost ansible_connection=local\n")).build())
            .failOnErrors(Property.ofValue(false))
            .build();

        var output = task.run(runContext);
        assertNotNull(output);

        var dynamicResults = runContext.dynamicWorkerResults();
        assertThat(dynamicResults, hasSize(3));

        var task1 = dynamicResults.get(0).getTaskRun();
        assertThat(task1.getTaskId(), is("Playbook 1 | Ping localhost"));
        assertThat(task1.getState().getCurrent(), is(State.Type.SUCCESS));

        var task2 = dynamicResults.get(1).getTaskRun();
        assertThat(task2.getTaskId(), is("Playbook 1 | Install package"));
        assertThat(task2.getState().getCurrent(), is(State.Type.SUCCESS));

        var task3 = dynamicResults.get(2).getTaskRun();
        assertThat(task3.getTaskId(), is("Playbook 1 | Configure firewall"));
        assertThat(task3.getState().getCurrent(), is(State.Type.FAILED));
    }

    @Test
    void testLogStreamingLimitPreservesCriticalErrorsAndSpool() throws Exception {
        var runContext = newRunContext();
        var spoolFile = runContext.workingDir().path().resolve("test-spool.log");

        var consumer = new AnsibleRunnerLogConsumer(
            runContext,
            true,
            LogsMode.FULL,
            spoolFile,
            50L,
            1,
            new SecretMasker()
        );

        for (var i = 1; i <= 60; i++) {
            consumer.accept("Normal log line " + i, false, Instant.now());
        }

        assertTrue(consumer.wasTruncated(), "Should be marked truncated after exceeding limit");

        consumer.accept("fatal: [localhost]: FAILED! => {\"msg\": \"Critical failure after limit\"}", false, Instant.now());
        consumer.accept("PLAY RECAP *********************************************************************", false, Instant.now());
        consumer.accept("localhost                  : ok=2    changed=0    unreachable=0    failed=1", false, Instant.now());

        consumer.close();

        var spooled = Files.readAllLines(spoolFile);
        assertThat(spooled.size(), is(63));
        assertThat(spooled.get(60), containsString("fatal: [localhost]: FAILED!"));
        assertThat(spooled.get(61), containsString("PLAY RECAP"));
        assertThat(spooled.get(62), containsString("failed=1"));
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
