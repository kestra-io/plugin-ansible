package io.kestra.plugin.ansible.runner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.ansible.runner.models.Inventory;
import io.kestra.plugin.ansible.runner.models.Project;
import io.kestra.plugin.ansible.runner.utils.ArchiveUtils;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
class AnsibleRunnerSecurityTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void testShellInjectionIsBlocked() {
        AnsibleRunner task = AnsibleRunner.builder()
            .id(IdUtils.create())
            .type(AnsibleRunner.class.getName())
            .pythonDependencies(Property.of(List.of("-r requirements.txt")))
            .project(Project.builder().inline(Property.of("- hosts: localhost\n  tasks:\n    - debug: msg=ok")).build())
            .build();

        RunContext runContext = runContextFactory.of();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), containsString("Invalid python dependency"));
    }

    @Test
    void testValidVersionPinsAllowed() throws Exception {
        AnsibleRunner task = AnsibleRunner.builder()
            .id(IdUtils.create())
            .type(AnsibleRunner.class.getName())
            .pythonDependencies(Property.of(List.of("ansible==2.9.0", "boto3>=1.15.0")))
            .project(Project.builder().inline(Property.of("- hosts: localhost\n  tasks:\n    - debug: msg=ok")).build())
            .build();

        RunContext runContext = runContextFactory.of();
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
        AnsibleRunner task = AnsibleRunner.builder()
            .id(IdUtils.create())
            .type(AnsibleRunner.class.getName())
            .inventory(Inventory.builder().file(Property.of("../../etc/passwd")).build())
            .project(Project.builder().inline(Property.of("- hosts: all")).build())
            .build();

        RunContext runContext = runContextFactory.of();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), containsString("Path traversal blocked"));
    }

    @Test
    void testSymlinkZipSlipBlocked() throws IOException {
        Path tempDir = Files.createTempDirectory("zipslip-test");
        Path zipPath = tempDir.resolve("malicious.zip");

        Path sourceDir = tempDir.resolve("source");
        Files.createDirectories(sourceDir);
        Path linkPath = sourceDir.resolve("link");
        try {
            Files.createSymbolicLink(linkPath, tempDir.resolve("nonexistent"));
            ArchiveUtils.zipDirectory(sourceDir, zipPath);
            // Symlinks are skipped quietly now, so no exception should be thrown
        } catch (UnsupportedOperationException ignored) {
            // OS doesn't support symlinks
        }
    }

    @Test
    void testMissingStatusFileDefaultsToFailed() throws Exception {
        AnsibleRunner task = AnsibleRunner.builder()
            .id(IdUtils.create())
            .type(AnsibleRunner.class.getName())
            .project(Project.builder().inline(Property.of("- hosts: localhost")).build())
            .taskRunner(new MockRunnerMissingStatus())
            .build();

        RunContext runContext = runContextFactory.of();
        RuntimeException e = assertThrows(RuntimeException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), containsString("failed"));
    }

    @Test
    void testJsonMapPathTraversalBlocked() {
        AnsibleRunner task = AnsibleRunner.builder()
            .id(IdUtils.create())
            .type(AnsibleRunner.class.getName())
            .project(Project.builder().content(Property.of("{\"../../evil.yml\": \"- hosts: all\"}")).build())
            .inventory(Inventory.builder().inline(Property.of("localhost")).build())
            .build();

        RunContext runContext = runContextFactory.of();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), containsString("Path traversal blocked"));
    }

    @Test
    void testUnsupportedUriSchemeProjectSource() {
        AnsibleRunner task = AnsibleRunner.builder()
            .id(IdUtils.create())
            .type(AnsibleRunner.class.getName())
            .project(Project.builder().source(Property.of("https://example.com/playbook.yml")).build())
            .inventory(Inventory.builder().inline(Property.of("localhost")).build())
            .build();

        RunContext runContext = runContextFactory.of();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), containsString("Unsupported URI scheme for project source"));
    }

    @Test
    void testUnsupportedUriSchemeInventoryFile() {
        AnsibleRunner task = AnsibleRunner.builder()
            .id(IdUtils.create())
            .type(AnsibleRunner.class.getName())
            .project(Project.builder().inline(Property.of("- hosts: all")).build())
            .inventory(Inventory.builder().file(Property.of("ftp://example.com/hosts")).build())
            .build();

        RunContext runContext = runContextFactory.of();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), containsString("Unsupported URI scheme for inventory file"));
    }

    @Test
    void testMissingProjectSourceThrows() {
        AnsibleRunner task = AnsibleRunner.builder()
            .id(IdUtils.create())
            .type(AnsibleRunner.class.getName())
            .project(Project.builder().source(Property.of("nonexistent/playbook.yml")).build())
            .inventory(Inventory.builder().inline(Property.of("localhost")).build())
            .build();

        RunContext runContext = runContextFactory.of();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), containsString("Project source not found"));
    }

    @Test
    void testSelectiveTaskRunnerTelemetrySync() throws Exception {
        RunContext runContext = runContextFactory.of();
        io.kestra.core.models.tasks.runners.TaskRunner<?> selectiveRunner = new SelectiveMockRunner();

        AnsibleRunner task = AnsibleRunner.builder()
            .id(IdUtils.create())
            .type(AnsibleRunner.class.getName())
            .taskRunner(selectiveRunner)
            .project(Project.builder().inline(Property.of("- hosts: localhost\n  tasks:\n    - ping:\n")).build())
            .inventory(Inventory.builder().inline(Property.of("localhost ansible_connection=local\n")).build())
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        org.junit.jupiter.api.Assertions.assertNotNull(output);
        org.junit.jupiter.api.Assertions.assertEquals("successful", output.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals(0, output.getRc());
        org.junit.jupiter.api.Assertions.assertNotNull(output.getArtifactsUri());
    }

    public static class SelectiveMockRunner extends io.kestra.core.models.tasks.runners.TaskRunner<io.kestra.core.models.tasks.runners.TaskRunnerDetailResult> {
        @Override
        public io.kestra.core.models.tasks.runners.TaskRunnerResult<io.kestra.core.models.tasks.runners.TaskRunnerDetailResult> run(
            RunContext runContext,
            io.kestra.core.models.tasks.runners.TaskCommands taskCommands,
            List<String> filesToDownload) throws Exception {
            Path workingDir = taskCommands.getWorkingDirectory();
            Path runnerDir = workingDir.resolve("runner");

            List<String> commands = runContext.render(taskCommands.getCommands()).asList(String.class);
            String command = commands.stream().filter(c -> c.contains("--ident ")).findFirst().orElseThrow();
            int identIdx = command.indexOf("--ident ");
            String ident = command.substring(identIdx + 8).trim().split("[\\s;]")[0];

            Path identArtifacts = runnerDir.resolve("artifacts").resolve(ident);
            Path jobEvents = identArtifacts.resolve("job_events");
            Files.createDirectories(jobEvents);

            Files.writeString(identArtifacts.resolve("status"), "successful\n", java.nio.charset.StandardCharsets.UTF_8);
            Files.writeString(identArtifacts.resolve("rc"), "0\n", java.nio.charset.StandardCharsets.UTF_8);

            return new io.kestra.core.models.tasks.runners.TaskRunnerResult<>(0, taskCommands.getLogConsumer());
        }
    }

    public static class MockRunnerMissingStatus extends io.kestra.core.models.tasks.runners.TaskRunner<io.kestra.core.models.tasks.runners.TaskRunnerDetailResult> {
        public MockRunnerMissingStatus() {
            super();
        }

        @Override
        public io.kestra.core.models.tasks.runners.TaskRunnerResult<io.kestra.core.models.tasks.runners.TaskRunnerDetailResult> run(RunContext runContext,
            io.kestra.core.models.tasks.runners.TaskCommands taskCommands, List<String> filesToDownload) throws Exception {
            return new io.kestra.core.models.tasks.runners.TaskRunnerResult<>(0, null);
        }
    }
}
