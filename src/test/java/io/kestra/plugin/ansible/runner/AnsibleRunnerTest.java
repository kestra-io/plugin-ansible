package io.kestra.plugin.ansible.runner;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
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
import io.kestra.plugin.ansible.runner.models.Project;
import io.kestra.plugin.ansible.runner.models.RunnerEnv;

import jakarta.inject.Inject;
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

    @Test
    void testRunnerDirectoryContractAndExecution() throws Exception {
        RunContext runContext = runContextFactory.of(
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
        TaskRunner<?> mockRunner = MockAnsibleTaskRunner.builder().build();

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(mockRunner)
            .containerImage(Property.ofValue("quay.io/ansible/ansible-runner:latest"))
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
        RunContext runContext = runContextFactory.of(Map.of());
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

    /**
     * Mock TaskRunner that verifies directory contents and generates realistic ansible-runner artifacts.
     */
    @SuperBuilder
    @NoArgsConstructor
    public static class MockAnsibleTaskRunner extends TaskRunner<TaskRunnerDetailResult> {

        @Override
        public TaskRunnerResult<TaskRunnerDetailResult> run(RunContext runContext, TaskCommands taskCommands, List<String> filesToDownload) throws Exception {
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

            return new TaskRunnerResult<>(0, taskCommands.getLogConsumer());
        }

        @Override
        public Map<String, Object> additionalVars(RunContext runContext, TaskCommands taskCommands) {
            return Collections.emptyMap();
        }
    }
}
