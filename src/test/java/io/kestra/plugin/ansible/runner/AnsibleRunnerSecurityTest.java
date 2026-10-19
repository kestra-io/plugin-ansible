package io.kestra.plugin.ansible.runner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.TaskCommands;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.models.tasks.runners.TaskRunnerDetailResult;
import io.kestra.core.models.tasks.runners.TaskRunnerResult;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.ansible.runner.models.Inventory;
import io.kestra.plugin.ansible.runner.models.Project;
import io.kestra.plugin.ansible.runner.utils.ArchiveUtils;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
class AnsibleRunnerSecurityTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void testShellInjectionIsBlocked() {
        var task = AnsibleRunner.builder()
            .id(IdUtils.create())
            .type(AnsibleRunner.class.getName())
            .pythonDependencies(Property.of(List.of("-r requirements.txt")))
            .project(Project.builder().inline(Property.of("- hosts: localhost\n  tasks:\n    - debug: msg=ok")).build())
            .build();

        var runContext = runContextFactory.of();
        var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), containsString("Invalid python dependency"));
    }

    @Test
    void testValidVersionPinsAllowed() throws Exception {
        var task = AnsibleRunner.builder()
            .id(IdUtils.create())
            .type(AnsibleRunner.class.getName())
            .pythonDependencies(Property.of(List.of("ansible==2.9.0", "boto3>=1.15.0")))
            .project(Project.builder().inline(Property.of("- hosts: localhost\n  tasks:\n    - debug: msg=ok")).build())
            .build();

        var runContext = runContextFactory.of();
        try {
            task.run(runContext);
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("Invalid python dependency")) {
                throw new AssertionError("Valid versions were rejected", e);
            }
        } catch (Exception ignored) {
        }
    }

    @Test
    void testPathTraversalBlocked() {
        var task = AnsibleRunner.builder()
            .id(IdUtils.create())
            .type(AnsibleRunner.class.getName())
            .inventory(Inventory.builder().file(Property.of("../../etc/passwd")).build())
            .project(Project.builder().inline(Property.of("- hosts: all")).build())
            .build();

        var runContext = runContextFactory.of();
        var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), containsString("Path traversal blocked"));
    }

    @Test
    void testSymlinkZipSlipBlocked() throws IOException {
        var tempDir = Files.createTempDirectory("zipslip-test");
        var zipPath = tempDir.resolve("malicious.zip");

        var sourceDir = tempDir.resolve("source");
        Files.createDirectories(sourceDir);
        var linkPath = sourceDir.resolve("link");
        try {
            Files.createSymbolicLink(linkPath, tempDir.resolve("nonexistent"));
            ArchiveUtils.zipDirectory(sourceDir, zipPath);
        } catch (UnsupportedOperationException ignored) {
        }
    }

    @Test
    void testMissingStatusFileDefaultsToFailed() throws Exception {
        var task = AnsibleRunner.builder()
            .id(IdUtils.create())
            .type(AnsibleRunner.class.getName())
            .project(Project.builder().inline(Property.of("- hosts: localhost")).build())
            .taskRunner(new MockRunnerMissingStatus())
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var e = assertThrows(RuntimeException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), containsString("failed"));
    }

    @Test
    void testJsonMapPathTraversalBlocked() {
        var task = AnsibleRunner.builder()
            .id(IdUtils.create())
            .type(AnsibleRunner.class.getName())
            .project(Project.builder().content(Property.of("{\"../../evil.yml\": \"- hosts: all\"}")).build())
            .inventory(Inventory.builder().inline(Property.of("localhost")).build())
            .build();

        var runContext = runContextFactory.of();
        var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), containsString("Path traversal blocked"));
    }

    @Test
    void testUnsupportedUriSchemeProjectSource() {
        var task = AnsibleRunner.builder()
            .id(IdUtils.create())
            .type(AnsibleRunner.class.getName())
            .project(Project.builder().source(Property.of("https://example.com/playbook.yml")).build())
            .inventory(Inventory.builder().inline(Property.of("localhost")).build())
            .build();

        var runContext = runContextFactory.of();
        var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), containsString("Unsupported URI scheme for project source"));
    }

    @Test
    void testUnsupportedUriSchemeInventoryFile() {
        var task = AnsibleRunner.builder()
            .id(IdUtils.create())
            .type(AnsibleRunner.class.getName())
            .project(Project.builder().inline(Property.of("- hosts: all")).build())
            .inventory(Inventory.builder().file(Property.of("ftp://example.com/hosts")).build())
            .build();

        var runContext = runContextFactory.of();
        var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), containsString("Unsupported URI scheme for inventory file"));
    }

    @Test
    void testMissingProjectSourceThrows() {
        var task = AnsibleRunner.builder()
            .id(IdUtils.create())
            .type(AnsibleRunner.class.getName())
            .project(Project.builder().source(Property.of("nonexistent/playbook.yml")).build())
            .inventory(Inventory.builder().inline(Property.of("localhost")).build())
            .build();

        var runContext = runContextFactory.of();
        var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), containsString("Project source not found"));
    }

    @Test
    void testSelectiveTaskRunnerTelemetrySync() throws Exception {
        var selectiveRunner = new SelectiveMockRunner();

        var task = AnsibleRunner.builder()
            .id(IdUtils.create())
            .type(AnsibleRunner.class.getName())
            .taskRunner(selectiveRunner)
            .project(Project.builder().inline(Property.of("- hosts: localhost\n  tasks:\n    - ping:\n")).build())
            .inventory(Inventory.builder().inline(Property.of("localhost ansible_connection=local\n")).build())
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        var output = task.run(runContext);
        assertNotNull(output);
        assertEquals("successful", output.getStatus());
        assertEquals(0, output.getRc());
        assertNotNull(output.getArtifactsUri());
    }

    public static class SelectiveMockRunner extends TaskRunner<TaskRunnerDetailResult> {
        public SelectiveMockRunner() {
            super();
            this.type = SelectiveMockRunner.class.getName();
        }

        @Override
        public TaskRunnerResult<TaskRunnerDetailResult> run(
            RunContext runContext,
            TaskCommands taskCommands,
            List<String> filesToDownload) throws Exception {
            var workingDir = taskCommands.getWorkingDirectory();
            var runnerDir = workingDir.resolve("runner");

            var commands = runContext.render(taskCommands.getCommands()).asList(String.class);
            var command = commands.stream().filter(c -> c.contains("--ident ")).findFirst().orElseThrow();
            var identIdx = command.indexOf("--ident ");
            var ident = command.substring(identIdx + 8).trim().split("[\\s;]")[0];

            var identArtifacts = runnerDir.resolve("artifacts").resolve(ident);
            var jobEvents = identArtifacts.resolve("job_events");
            Files.createDirectories(jobEvents);

            Files.writeString(identArtifacts.resolve("status"), "successful\n", StandardCharsets.UTF_8);
            Files.writeString(identArtifacts.resolve("rc"), "0\n", StandardCharsets.UTF_8);

            return new TaskRunnerResult<>(0, taskCommands.getLogConsumer());
        }
    }

    public static class MockRunnerMissingStatus extends TaskRunner<TaskRunnerDetailResult> {
        public MockRunnerMissingStatus() {
            super();
            this.type = MockRunnerMissingStatus.class.getName();
        }

        @Override
        public TaskRunnerResult<TaskRunnerDetailResult> run(
            RunContext runContext,
            TaskCommands taskCommands,
            List<String> filesToDownload) throws Exception {
            return new TaskRunnerResult<>(0, null);
        }
    }
}
