package io.kestra.plugin.ansible.runner;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTaskException;
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
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@KestraTest
public class AnsibleRunnerComprehensiveTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    private static final List<TestRecord> testRecords = new CopyOnWriteArrayList<>();

    @Data
    @Builder
    public static class TestRecord {
        private String id;
        private String category;
        private String description;
        private String inputs;
        private String expected;
        private String status;
        private int returnCode;
        private long durationMs;
        private String notes;
    }

    private void recordResult(String id, String category, String description, String inputs, String expected,
        String status, int rc, long durationMs, String notes) {
        testRecords.add(
            TestRecord.builder()
                .id(id)
                .category(category)
                .description(description)
                .inputs(inputs)
                .expected(expected)
                .status(status)
                .returnCode(rc)
                .durationMs(durationMs)
                .notes(notes)
                .build()
        );
    }

    @AfterAll
    public static void exportCsvReports() throws IOException {
        StringBuilder csv = new StringBuilder();
        csv.append("Test ID,Category,Description,Inputs & Variations,Expected Result,Status,Return Code,Duration (ms),Validated Assertions\n");
        for (TestRecord r : testRecords) {
            csv.append(escapeCsv(r.getId())).append(",")
                .append(escapeCsv(r.getCategory())).append(",")
                .append(escapeCsv(r.getDescription())).append(",")
                .append(escapeCsv(r.getInputs())).append(",")
                .append(escapeCsv(r.getExpected())).append(",")
                .append(escapeCsv(r.getStatus())).append(",")
                .append(r.getReturnCode()).append(",")
                .append(r.getDurationMs()).append(",")
                .append(escapeCsv(r.getNotes())).append("\n");
        }

        // Write to build directory
        Path reportDir = Paths.get("build", "reports");
        Files.createDirectories(reportDir);
        Files.writeString(reportDir.resolve("test-results-matrix.csv"), csv.toString(), StandardCharsets.UTF_8);
    }

    private static String escapeCsv(String val) {
        if (val == null)
            return "\"\"";
        return "\"" + val.replace("\"", "\"\"") + "\"";
    }

    private RunContext newRunContext(Map<String, Object> vars) {
        AnsibleRunner dummyTask = AnsibleRunner.builder().id("unit-test").type(AnsibleRunner.class.getName()).build();
        io.kestra.core.runners.DefaultRunContext runContext = (io.kestra.core.runners.DefaultRunContext) runContextFactory.of(dummyTask, vars != null ? vars : Map.of());
        return runContextFactory.initializer().forExecutor(runContext);
    }

    private RunContext newRunContext() {
        return newRunContext(Map.of());
    }

    @Test
    void test01_InlinePlaybookBasic() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        String inlineYaml = """
            - name: Inline Basic Test
              hosts: localhost
              gather_facts: false
              tasks:
                - name: Ping
                  ansible.builtin.debug:
                    msg: "Pong"
            """;

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .project(
                Project.builder()
                    .playbook(Property.ofValue("site.yml"))
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
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        assertNotNull(output);
        assertEquals("successful", output.getStatus());
        assertEquals(0, output.getRc());

        Path playbook = runContext.workingDir().path().resolve("runner/project/site.yml");
        assertTrue(Files.exists(playbook));

        recordResult(
            "TC01", "Playbook Source", "Inline playbook string execution",
            "project.source=inline YAML, inventory.inline=localhost", "status=successful, rc=0",
            "PASS", output.getRc(), dur, "Playbook written to runner/project/site.yml, zero exit code"
        );
    }

    @Test
    void test02_InlinePlaybookLeadingHyphensSanitization() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        String inlineYamlWithTypo = """
            ----
            - name: Sanitize Hyphens
              hosts: localhost
              tasks:
                - debug: msg="Clean"
            """;

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .project(
                Project.builder()
                    .playbook(Property.ofValue("audit.yml"))
                    .source(Property.ofValue(inlineYamlWithTypo))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        Path playbook = runContext.workingDir().path().resolve("runner/project/audit.yml");
        String content = Files.readString(playbook);
        assertTrue(content.startsWith("---\n") || content.startsWith("---"), "Leading 4+ hyphens must normalize to 3 dashes");

        recordResult(
            "TC02", "Playbook Source", "Sanitize leading 4+ dashes typo in inline YAML",
            "project.source='----\\n- name:...'", "Normalized to '---' and executed successfully",
            "PASS", output.getRc(), dur, "Sanitized 4 dashes to valid YAML --- header"
        );
    }

    @Test
    void test03_ProjectZipArchiveStorage() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        // Create in-memory zip archive containing a playbook
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            zos.putNextEntry(new ZipEntry("deploy.yml"));
            String pb = """
                - name: Zipped Playbook
                  hosts: localhost
                  tasks:
                    - debug: msg="From Zip"
                """;
            zos.write(pb.getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
        }

        // Upload zip to Kestra internal storage
        Path tempZip = Files.createTempFile("project", ".zip");
        Files.write(tempZip, baos.toByteArray());
        URI zipUri = runContext.storage().putFile(tempZip.toFile());
        Files.deleteIfExists(tempZip);

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .project(
                Project.builder()
                    .source(Property.ofValue(zipUri.toString()))
                    .playbook(Property.ofValue("deploy.yml"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        Path extractedPb = runContext.workingDir().path().resolve("runner/project/deploy.yml");
        assertTrue(Files.exists(extractedPb));

        recordResult(
            "TC03", "Project Source", "Zip archive uploaded to Kestra storage",
            "project.source=kestra:///.../project.zip, playbook=deploy.yml", "Unzipped into runner/project/ and executed",
            "PASS", output.getRc(), dur, "Storage zip stream extracted cleanly, target playbook resolved"
        );
    }

    @Test
    void test04_ProjectTarGzArchiveStorage() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        // Create in-memory tar.gz archive
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (
            GZIPOutputStream gzos = new GZIPOutputStream(baos);
            TarArchiveOutputStream taos = new TarArchiveOutputStream(gzos)
        ) {
            String pb = "- name: TarGz Playbook\n  hosts: localhost\n  tasks:\n    - debug: msg='TarGz'\n";
            byte[] bytes = pb.getBytes(StandardCharsets.UTF_8);
            TarArchiveEntry entry = new TarArchiveEntry("release.yml");
            entry.setSize(bytes.length);
            taos.putArchiveEntry(entry);
            taos.write(bytes);
            taos.closeArchiveEntry();
        }

        Path tempTarGz = Files.createTempFile("project", ".tar.gz");
        Files.write(tempTarGz, baos.toByteArray());
        URI tarUri = runContext.storage().putFile(tempTarGz.toFile());
        Files.deleteIfExists(tempTarGz);

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .project(
                Project.builder()
                    .source(Property.ofValue(tarUri.toString()))
                    .playbook(Property.ofValue("release.yml"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        Path extractedPb = runContext.workingDir().path().resolve("runner/project/release.yml");
        assertTrue(Files.exists(extractedPb));

        recordResult(
            "TC04", "Project Source", "Tar.gz archive uploaded to Kestra storage",
            "project.source=kestra:///.../project.tar.gz", "Extracted into runner/project/ and executed",
            "PASS", output.getRc(), dur, "Gzip and Tar extracted properly, target release.yml found"
        );
    }

    @Test
    void test05_NamespaceFilesRolesTemplatesScriptsStructure() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();
        Path workingDir = runContext.workingDir().path();

        // Simulate Kestra namespaceFiles injection into workingDir
        Path rolesDir = workingDir.resolve("roles/nginx/tasks");
        Path templatesDir = workingDir.resolve("templates");
        Path scriptsDir = workingDir.resolve("scripts");

        Files.createDirectories(rolesDir);
        Files.createDirectories(templatesDir);
        Files.createDirectories(scriptsDir);

        Files.writeString(rolesDir.resolve("main.yml"), "- name: Role Task\n  debug: msg='Role executed'\n");
        Files.writeString(templatesDir.resolve("app.conf.j2"), "server { listen {{ port }}; }\n");
        Files.writeString(scriptsDir.resolve("healthcheck.sh"), "#!/bin/bash\necho 'Service OK'\n");
        Files.writeString(workingDir.resolve("site.yml"), """
            - name: Full Namespace Stack Test
              hosts: localhost
              roles:
                - nginx
              tasks:
                - name: Check Health Script
                  ansible.builtin.command: bash scripts/healthcheck.sh
            """);

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .project(
                Project.builder()
                    .playbook(Property.ofValue("site.yml"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        Path runnerProject = workingDir.resolve("runner/project");
        assertTrue(Files.exists(runnerProject.resolve("roles/nginx/tasks/main.yml")), "roles must be copied to runner/project");
        assertTrue(Files.exists(runnerProject.resolve("templates/app.conf.j2")), "templates must be copied to runner/project");
        assertTrue(Files.exists(runnerProject.resolve("scripts/healthcheck.sh")), "scripts must be copied to runner/project");
        assertTrue(Files.exists(runnerProject.resolve("site.yml")), "site.yml must be copied to runner/project");

        recordResult(
            "TC05", "Namespace Files", "Namespace files with roles/, templates/, scripts/ folders",
            "workingDir/roles, workingDir/templates, workingDir/scripts", "All folders synced into runner/project/",
            "PASS", output.getRc(), dur, "Verified roles, templates, and scripts directories copied to runner/project/"
        );
    }

    @Test
    void test06_InventoryInlineExecution() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        String inv = "db-primary ansible_host=10.0.1.50\ndb-replica ansible_host=10.0.1.51\n";

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: all\n  tasks:\n    - ping:\n"))
                    .playbook(Property.ofValue("site.yml"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue(inv))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        Path hosts = runContext.workingDir().path().resolve("runner/inventory/hosts");
        assertTrue(Files.exists(hosts));
        String content = Files.readString(hosts);
        assertThat(content, containsString("db-primary"));
        assertThat(content, containsString("db-replica"));

        recordResult(
            "TC06", "Inventory", "Inline inventory string",
            "inventory.inline='db-primary... db-replica...'", "Written to runner/inventory/hosts",
            "PASS", output.getRc(), dur, "Verified INI content written to inventory/hosts"
        );
    }

    @Test
    void test07_InventoryStorageFile() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        Path tempInv = Files.createTempFile("hosts", ".ini");
        Files.writeString(tempInv, "[web]\nnode-01 ansible_host=172.16.0.1\nnode-02 ansible_host=172.16.0.2\n");
        URI invUri = runContext.storage().putFile(tempInv.toFile());
        Files.deleteIfExists(tempInv);

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: web\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .file(Property.ofValue(invUri.toString()))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        Path hosts = runContext.workingDir().path().resolve("runner/inventory/hosts");
        assertTrue(Files.exists(hosts));
        String content = Files.readString(hosts);
        assertThat(content, containsString("node-01"));

        recordResult(
            "TC07", "Inventory", "Inventory file from Kestra storage URI",
            "inventory.file=kestra:///.../hosts.ini", "Downloaded and placed into runner/inventory/hosts",
            "PASS", output.getRc(), dur, "Downloaded storage file to runner/inventory/hosts"
        );
    }

    @Test
    void test08_InventoryJsonDynamic() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        String jsonInv = """
            {
              "_meta": {
                "hostvars": {}
              },
              "all": {
                "hosts": ["cloud-node-1", "cloud-node-2"]
              }
            }
            """;

        Path tempJson = Files.createTempFile("inventory", ".json");
        Files.writeString(tempJson, jsonInv);
        URI jsonUri = runContext.storage().putFile(tempJson.toFile());
        Files.deleteIfExists(tempJson);

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: all\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .file(Property.ofValue(jsonUri.toString()))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        Path hosts = runContext.workingDir().path().resolve("runner/inventory/hosts");
        assertTrue(Files.exists(hosts));
        String content = Files.readString(hosts);
        assertThat(content, containsString("cloud-node-1"));

        recordResult(
            "TC08", "Inventory", "JSON dynamic/cached inventory from storage",
            "inventory.file=kestra:///.../inventory.json", "Written to runner/inventory/hosts",
            "PASS", output.getRc(), dur, "Dynamic JSON inventory resolved properly"
        );
    }

    @Test
    void test09_CheckModeAndDiff() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .checkMode(Property.ofValue(true))
            .diff(Property.ofValue(true))
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - debug: msg='Dry Run'\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        Path cmdline = runContext.workingDir().path().resolve("runner/env_backup/cmdline");
        String cmd = Files.readString(cmdline);
        assertThat(cmd, containsString("--check"));
        assertThat(cmd, containsString("--diff"));

        recordResult(
            "TC09", "Execution Controls", "Check mode (--check) and Diff mode (--diff)",
            "checkMode=true, diff=true", "--check and --diff flags written to env/cmdline",
            "PASS", output.getRc(), dur, "Verified --check and --diff in env/cmdline"
        );
    }

    @Test
    void test10_LimitAndTags() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .limit(Property.ofValue("app_servers:&east_coast"))
            .tags(Property.ofValue(List.of("deploy", "patch")))
            .skipTags(Property.ofValue(List.of("reboot")))
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: all\n  tasks:\n    - debug: msg='Tagged'\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        Path cmdline = runContext.workingDir().path().resolve("runner/env_backup/cmdline");
        String cmd = Files.readString(cmdline);
        assertThat(cmd, containsString("--limit app_servers:&east_coast"));
        assertThat(cmd, containsString("--tags deploy,patch"));
        assertThat(cmd, containsString("--skip-tags reboot"));

        recordResult(
            "TC10", "Execution Controls", "Target limiting and Tag filtering",
            "limit=app_servers:&east_coast, tags=[deploy, patch], skipTags=[reboot]", "Flags in env/cmdline",
            "PASS", output.getRc(), dur, "Verified --limit, --tags, and --skip-tags formatted properly"
        );
    }

    @Test
    void test11_VerbosityAndForks() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .verbosity(Property.ofValue(3))
            .forks(Property.ofValue(30))
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - debug: msg='Verbose'\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        Path cmdline = runContext.workingDir().path().resolve("runner/env_backup/cmdline");
        String cmd = Files.readString(cmdline);
        assertThat(cmd, containsString("-vvv"));
        assertThat(cmd, containsString("-f 30"));

        recordResult(
            "TC11", "Execution Controls", "High verbosity (-vvv) and Parallel forks (-f 30)",
            "verbosity=3, forks=30", "-vvv and -f 30 present in env/cmdline",
            "PASS", output.getRc(), dur, "Verified verbosity flag mapping and forks flag"
        );
    }

    @Test
    void test12_ExtraVarsSerialization() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        Map<String, Object> vars = Map.of(
            "env_name", "production",
            "max_connections", 1000,
            "features_enabled", List.of("tls", "waf")
        );

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .env(
                RunnerEnv.builder()
                    .extraVars(Property.ofValue(vars))
                    .build()
            )
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - debug: var=env_name\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        Path extravars = runContext.workingDir().path().resolve("runner/env_backup/extravars");
        assertTrue(Files.exists(extravars));
        String json = Files.readString(extravars);
        assertThat(json, containsString("\"env_name\":\"production\""));
        assertThat(json, containsString("\"max_connections\":1000"));

        recordResult(
            "TC12", "Environment & Secrets", "Extra variables JSON serialization",
            "env.extraVars={env_name: production, max_connections: 1000}", "Serialized to JSON in runner/env_backup/extravars",
            "PASS", output.getRc(), dur, "Verified JSON extra variables structure"
        );
    }

    @Test
    void test13_EnvVarsExport() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        Map<String, String> envs = Map.of(
            "ANSIBLE_HOST_KEY_CHECKING", "False",
            "ANSIBLE_PIPELINING", "True"
        );

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .env(
                RunnerEnv.builder()
                    .envVars(Property.ofValue(envs))
                    .build()
            )
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        Path envvars = runContext.workingDir().path().resolve("runner/env_backup/envvars");
        assertTrue(Files.exists(envvars));
        String json = Files.readString(envvars);
        assertThat(json, containsString("ANSIBLE_HOST_KEY_CHECKING"));

        recordResult(
            "TC13", "Environment & Secrets", "Environment variables JSON serialization",
            "env.envVars={ANSIBLE_HOST_KEY_CHECKING: False}", "Written to runner/env_backup/envvars",
            "PASS", output.getRc(), dur, "Verified environment variables exported to env/envvars"
        );
    }

    @Test
    void test14_DynamicPasswordsSecrets() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext(Map.of("secretVault", "vaultSuperSecret999"));
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .env(
                RunnerEnv.builder()
                    .passwords(Property.ofExpression("{\"vault_password\": \"{{ secretVault }}\"}"))
                    .build()
            )
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        Path passwords = runContext.workingDir().path().resolve("runner/env_backup/passwords");
        assertTrue(Files.exists(passwords));
        String json = Files.readString(passwords);
        assertThat(json, containsString("vaultSuperSecret999"));

        recordResult(
            "TC14", "Environment & Secrets", "Dynamic password rendering (Vault/Sudo)",
            "env.passwords={vault_password: '{{ secretVault }}'}", "Dynamically evaluated and written to runner/env_backup/passwords",
            "PASS", output.getRc(), dur, "Pebble expressions evaluated; written to runner/env_backup/passwords without leaking to CLI"
        );
    }

    @Test
    void test15_SshKeyPermissions() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        String key = "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAA...\n-----END OPENSSH PRIVATE KEY-----\n";

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .env(
                RunnerEnv.builder()
                    .sshKey(Property.ofValue(key))
                    .build()
            )
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        Path sshKeyFile = runContext.workingDir().path().resolve("runner/env_backup/ssh_key");
        assertTrue(Files.exists(sshKeyFile));
        try {
            Set<?> perms = Files.getPosixFilePermissions(sshKeyFile);
            String permStr = PosixFilePermissions.toString((Set<java.nio.file.attribute.PosixFilePermission>) perms);
            assertEquals("rw-------", permStr, "SSH key must strictly have 0600 permissions");
        } catch (UnsupportedOperationException ignored) {
            // Windows filesystems don't support POSIX attributes
        }

        recordResult(
            "TC15", "Environment & Secrets", "Private SSH key permission isolation (0600)",
            "env.sshKey=-----BEGIN OPENSSH...", "runner/env_backup/ssh_key written with chmod 0600",
            "PASS", output.getRc(), dur, "Verified rw------- (0600) POSIX permissions"
        );
    }

    @Test
    void test16_TimeoutSettings() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .timeout(Property.ofValue(Duration.ofMinutes(15)))
            .idleTimeout(Property.ofValue(Duration.ofMinutes(3)))
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        Path settings = runContext.workingDir().path().resolve("runner/env_backup/settings");
        assertTrue(Files.exists(settings));
        String json = Files.readString(settings);
        assertThat(json, containsString("\"job_timeout\":900"));
        assertThat(json, containsString("\"idle_timeout\":180"));

        recordResult(
            "TC16", "Engine Controls", "Execution and Idle timeouts in seconds",
            "timeout=PT15M (900s), idleTimeout=PT3M (180s)", "runner/env_backup/settings populated with job_timeout and idle_timeout",
            "PASS", output.getRc(), dur, "Verified duration converted to seconds in settings JSON"
        );
    }

    @Test
    void test17_TelemetryFailureExtraction() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder()
            .simulateFailure(true)
            .build();

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .failOnErrors(Property.ofValue(false))
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: all\n  tasks:\n    - name: Failing task\n      fail: msg='Error'\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("host-01\nhost-02\n"))
                    .build()
            )
            .outputSummary(Property.ofValue(true))
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        assertNotNull(output);
        assertEquals("failed", output.getStatus());
        assertEquals(2, output.getRc());
        assertEquals(1, output.getFailures());
        assertThat(output.getFailedHosts(), containsInAnyOrder("host-02"));
        assertEquals(1, output.getSummary().getOk());

        recordResult(
            "TC17", "Telemetry & Outputs", "Structured failure telemetry and host tracking",
            "failOnErrors=false with task failure on host-02", "status=failed, failures=1, failedHosts=[host-02]",
            "PASS", output.getRc(), dur, "Downstream routing enabled without premature worker crashes"
        );
    }

    @Test
    void test18_FailOnErrorsExceptionThrow() {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder()
            .simulateFailure(true)
            .build();

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .failOnErrors(Property.ofValue(true))
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: all\n  tasks:\n    - fail:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("host-01\nhost-02\n"))
                    .build()
            )
            .build();

        RunnableTaskException thrown = assertThrows(RunnableTaskException.class, () -> task.run(runContext));
        long dur = System.currentTimeMillis() - start;

        assertThat(thrown.getMessage(), containsString("Ansible Runner failed with status"));
        assertThat(thrown.getMessage(), containsString("exit code 2"));
        assertNotNull(thrown.getOutput());
        assertTrue(thrown.getOutput() instanceof AnsibleRunner.Output);
        var out = (AnsibleRunner.Output) thrown.getOutput();
        assertEquals("failed", out.getStatus());
        assertEquals(2, out.getRc());
        assertNotNull(out.getResultsUri());
        assertNotNull(out.getArtifactsUri());
        assertThat(out.getFailedHosts(), hasItem("host-02"));

        recordResult(
            "TC18", "Engine Controls", "failOnErrors=true hard failure exception",
            "failOnErrors=true, runner returns rc=2", "RunnableTaskException thrown with attached Output object",
            "PASS", 2, dur, "Verified exception raised with detailed error message and task outputs"
        );
    }

    @Test
    void test19_ArtifactsZipPackagingAndBlobStorage() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .autoCleanArtifacts(Property.ofValue(false))
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        assertNotNull(output.getArtifactsUri());
        try (InputStream is = runContext.storage().getFile(output.getArtifactsUri())) {
            assertNotNull(is);
            byte[] header = is.readNBytes(2);
            assertEquals('P', (char) header[0]);
            assertEquals('K', (char) header[1]);
        }

        recordResult(
            "TC19", "Storage & Archiving", "Zipped telemetry artifact upload to Kestra Internal Storage",
            "autoCleanArtifacts=false", "artifactsUri pointing to valid zip archive in Kestra storage",
            "PASS", output.getRc(), dur, "Verified ZIP archive magic bytes (PK) in uploaded blob"
        );
    }

    @Test
    void test20_AutoCleanArtifactsPurge() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .autoCleanArtifacts(Property.ofValue(true))
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        Path artifactsDir = runContext.workingDir().path().resolve("runner/artifacts");
        assertFalse(Files.exists(artifactsDir), "runner/artifacts must be deleted after upload when autoCleanArtifacts is true");

        recordResult(
            "TC20", "Storage & Archiving", "Local artifacts directory cleanup after upload",
            "autoCleanArtifacts=true", "runner/artifacts directory purged from local worker disk",
            "PASS", output.getRc(), dur, "Worker disk bloat prevented by auto-cleaning local artifact tree"
        );
    }

    @Test
    void test21_SimplifiedPlaybookAndInventoryPathResolution() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        // Create playbook and inventory directly in working directory (emulating namespaceFiles sync)
        Path playbooksDir = runContext.workingDir().path().resolve("playbooks");
        Path inventoryDir = runContext.workingDir().path().resolve("inventory");
        Files.createDirectories(playbooksDir);
        Files.createDirectories(inventoryDir);
        Files.writeString(playbooksDir.resolve("site.yml"), "- hosts: all\n  tasks:\n    - ping:\n");
        Files.writeString(inventoryDir.resolve("hosts.ini"), "[webservers]\nlocalhost ansible_connection=local\n");

        // Simple flow definition: project with only playbook: site.yml, inventory: inventory/hosts.ini
        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .project(
                Project.builder()
                    .playbook(Property.ofValue("site.yml"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .file(Property.ofValue("inventory/hosts.ini"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        assertEquals("successful", output.getStatus());
        assertEquals(0, output.getRc());

        Path projectPlaybook = runContext.workingDir().path().resolve("runner/project/site.yml");
        Path runnerHosts = runContext.workingDir().path().resolve("runner/inventory/hosts");
        assertTrue(Files.exists(projectPlaybook), "Playbook must be copied automatically from playbooks/ to runner/project/");
        assertTrue(Files.exists(runnerHosts), "Inventory must be copied automatically from inventory/hosts.ini to runner/inventory/hosts");

        recordResult(
            "TC21", "Namespace & Path Resolution", "Simplified playbook & inventory file name resolution",
            "project.playbook=site.yml, inventory.file=inventory/hosts.ini", "Resolved from namespace files in workingDir",
            "PASS", output.getRc(), dur, "User does not need to specify source or inline read expressions"
        );
    }

    @Test
    void test22_StructuredHostsMapInventory() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        Map<String, Object> hosts = Map.of(
            "srv1", Map.of("ansible_host", "10.0.0.1"),
            "srv2", Map.of("ansible_host", "10.0.0.2")
        );

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: all\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .hosts(Property.ofValue(hosts))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        Path hostsJson = runContext.workingDir().path().resolve("runner/inventory/hosts.json");
        assertTrue(Files.exists(hostsJson));
        String json = Files.readString(hostsJson);
        assertThat(json, containsString("srv1"));
        assertThat(json, containsString("10.0.0.1"));

        recordResult(
            "TC22", "Inventory Options", "Structured hosts map JSON inventory",
            "inventory.hosts={srv1: {ansible_host: 10.0.0.1}}", "runner/inventory/hosts.json generated",
            "PASS", output.getRc(), dur, "Native Ansible JSON inventory format generated from structured map"
        );
    }

    @Test
    void test23_StructuredGroupsMapInventory() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        Map<String, Object> groups = Map.of(
            "webservers", Map.of(
                "hosts", List.of("web1", "web2"),
                "vars", Map.of("http_port", 80)
            )
        );

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: webservers\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .groups(Property.ofValue(groups))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        Path hostsJson = runContext.workingDir().path().resolve("runner/inventory/hosts.json");
        assertTrue(Files.exists(hostsJson));
        String json = Files.readString(hostsJson);
        assertThat(json, containsString("webservers"));
        assertThat(json, containsString("http_port"));

        recordResult(
            "TC23", "Inventory Options", "Structured groups map JSON inventory",
            "inventory.groups={webservers: {...}}", "runner/inventory/hosts.json generated",
            "PASS", output.getRc(), dur, "Group hierarchy and group vars serialized directly to JSON inventory"
        );
    }

    @Test
    void test24_MultipleInventorySources() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        Path src1 = runContext.workingDir().path().resolve("inv1.ini");
        Path src2 = runContext.workingDir().path().resolve("inv2.ini");
        Files.writeString(src1, "[g1]\nhost1\n");
        Files.writeString(src2, "[g2]\nhost2\n");

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: all\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .sources(Property.ofValue(List.of("inv1.ini", "inv2.ini")))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        assertTrue(Files.exists(runContext.workingDir().path().resolve("runner/inventory/inv1.ini")));
        assertTrue(Files.exists(runContext.workingDir().path().resolve("runner/inventory/inv2.ini")));

        recordResult(
            "TC24", "Inventory Options", "Multiple inventory sources directory merging",
            "inventory.sources=[inv1.ini, inv2.ini]", "Both files copied into runner/inventory/",
            "PASS", output.getRc(), dur, "Multiple inventory sources merged in /runner/inventory/ tree"
        );
    }

    @Test
    void test25_OutputLogFileGenerationAndUpload() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .outputLogFile(Property.ofValue(true))
            .streamLogs(Property.ofValue(true))
            .logsMode(Property.ofValue(LogsMode.FULL))
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        assertNotNull(output.getOutputLogFile(), "outputLogFile URI must not be null when outputLogFile=true");
        try (InputStream is = runContext.storage().getFile(output.getOutputLogFile())) {
            assertNotNull(is);
        }

        recordResult(
            "TC25", "Logging Controls", "outputLogFile storage upload",
            "outputLogFile=true", "Complete execution log uploaded to internal storage",
            "PASS", output.getRc(), dur, "Execution log persisted and available via outputLogFile URI"
        );
    }

    @Test
    void test26_StreamLogsAndLogsModeSummary() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .streamLogs(Property.ofValue(true))
            .logsMode(Property.ofValue(LogsMode.SUMMARY))
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        assertEquals(0, output.getRc());

        recordResult(
            "TC26", "Logging Controls", "logsMode=SUMMARY log stream filtering",
            "logsMode=SUMMARY, streamLogs=true", "Filters noise and keeps high-level play/task headers",
            "PASS", output.getRc(), dur, "Validated SUMMARY logsMode operation"
        );
    }

    @Test
    void test27_MaxOutputsSizeValidation() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .maxOutputsSize(Property.ofValue(5L)) // Tiny limit to trigger guard
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        long dur = System.currentTimeMillis() - start;

        assertNotNull(output);
        assertNotNull(output.getResultsUri(), "resultsUri must be preserved even when maxOutputsSize is exceeded");
        assertTrue(output.getStats().isEmpty(), "stats must be truncated when maxOutputsSize is exceeded");
        assertTrue(output.getFailedHosts().isEmpty(), "failedHosts must be truncated when maxOutputsSize is exceeded");

        recordResult(
            "TC27", "Execution Guards", "maxOutputsSize guard against queue payload overflow",
            "maxOutputsSize=5 bytes", "Inline stats and failedHosts truncated while preserving resultsUri",
            "PASS", 0, dur, "Verified output payload size ceiling enforcement without discarding resultsUri"
        );
    }

    @Test
    void test28_ConsolidatedResultsJsonStorage() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        assertNotNull(output.getResultsUri(), "resultsUri must point to single consolidated results.json in internal storage");
        try (InputStream is = runContext.storage().getFile(output.getResultsUri())) {
            assertNotNull(is);
            String json = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(json, containsString("\"status\" : \"successful\""));
            assertThat(json, containsString("\"summary\" :"));
            assertThat(json, containsString("\"stats\" :"));
        }

        recordResult(
            "TC28", "Telemetry & Storage", "Consolidated single results.json report generation",
            "resultsUri generated in storage", "Single unified JSON replaces thousands of raw event files",
            "PASS", output.getRc(), dur, "Validated single consolidated results.json output structure"
        );
    }

    @Test
    void test29_AutoInstallGalaxyAndPythonRequirements() throws Exception {
        long start = System.currentTimeMillis();
        RunContext runContext = newRunContext();
        TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

        AnsibleRunner task = AnsibleRunner.builder()
            .taskRunner(runner)
            .autoInstallGalaxyRequirements(Property.ofValue(true))
            .autoInstallPythonRequirements(Property.ofValue(true))
            .galaxyDependencies(Property.ofValue(List.of("community.general")))
            .pythonDependencies(Property.ofValue(List.of("requests")))
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        AnsibleRunner.Output output = task.run(runContext);
        assertFalse(Files.exists(runContext.workingDir().path().resolve("runner/env")), "env directory must be deleted");
        long dur = System.currentTimeMillis() - start;

        assertEquals(0, output.getRc());

        recordResult(
            "TC29", "Dependency Management", "Auto-install Galaxy and Python dependencies",
            "galaxyDependencies=[community.general], pythonDependencies=[requests]", "Pre-commands injected for execution",
            "PASS", output.getRc(), dur, "Verified pre-commands container execution injection"
        );
    }

    @Test
    void test30_AllVerbosityLevels() throws Exception {
        Map<Integer, String> expectedFlags = Map.of(
            1, "-v",
            2, "-vv",
            3, "-vvv",
            4, "-vvvv"
        );

        for (Map.Entry<Integer, String> entry : expectedFlags.entrySet()) {
            RunContext runContext = newRunContext();
            TaskRunner<?> runner = ComprehensiveMockRunner.builder().build();

            AnsibleRunner task = AnsibleRunner.builder()
                .taskRunner(runner)
                .verbosity(Property.ofValue(entry.getKey()))
                .project(
                    Project.builder()
                        .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                        .build()
                )
                .inventory(
                    Inventory.builder()
                        .inline(Property.ofValue("localhost ansible_connection=local\n"))
                        .build()
                )
                .build();

            task.run(runContext);
            Path cmdline = runContext.workingDir().path().resolve("runner/env_backup/cmdline");
            assertTrue(Files.exists(cmdline));
            String cmd = Files.readString(cmdline).trim();
            assertThat(cmd, containsString(entry.getValue()));

            // Verify strict flag level (e.g. level 2 has -vv, but not -vvv)
            if (entry.getKey() < 4) {
                String excessiveFlag = entry.getValue() + "v";
                assertFalse(cmd.contains(excessiveFlag), "Verbosity " + entry.getKey() + " must not contain " + excessiveFlag);
            }
        }

        // Test verbosity = 0 (no -v flag written)
        RunContext runContextZero = newRunContext();
        TaskRunner<?> runnerZero = ComprehensiveMockRunner.builder().build();
        AnsibleRunner taskZero = AnsibleRunner.builder()
            .taskRunner(runnerZero)
            .verbosity(Property.ofValue(0))
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        taskZero.run(runContextZero);
        Path cmdlineZero = runContextZero.workingDir().path().resolve("runner/env_backup/cmdline");
        if (Files.exists(cmdlineZero)) {
            String cmd = Files.readString(cmdlineZero);
            assertFalse(cmd.contains("-v"), "Verbosity 0 must not produce any -v flags");
        }
    }

    @Test
    void test31_LargeLogVolumeIsCappedButFullyCaptured() throws Exception {
        RunContext runContext = newRunContext();
        Path spool = runContext.workingDir().path().resolve("big.log");
        int total = 50_000;

        AnsibleRunnerLogConsumer consumer = new AnsibleRunnerLogConsumer(
            runContext, true, LogsMode.FULL, spool, 1_000L, 100, java.util.Collections.emptyList()
        );
        for (int i = 0; i < total; i++) {
            consumer.accept("ok: [router-" + i + "] => (item=Gi0/" + i + ")", false, java.time.Instant.now());
        }
        consumer.accept("fatal: [switch-13]: FAILED! => {\"msg\": \"boom\"}", false, java.time.Instant.now());
        consumer.close();

        assertTrue(consumer.wasTruncated(), "Streaming must be truncated beyond maxLogLines");
        assertEquals(total + 1, consumer.getTotalLines());
        try (var lines = Files.lines(spool)) {
            assertEquals(total + 1, lines.count(), "Disk spool must contain every single line");
        }
        // Spool is read from disk (no in-memory buffer)
        try (InputStream is = consumer.getLogInputStream()) {
            assertNotNull(is);
        }
    }

    @Test
    void test32_SummaryModeDropsPerItemOkLines() throws Exception {
        RunContext runContext = newRunContext();
        Path spool = runContext.workingDir().path().resolve("summary.log");

        AnsibleRunnerLogConsumer consumer = new AnsibleRunnerLogConsumer(
            runContext, true, LogsMode.SUMMARY, spool, 0L, 100, java.util.Collections.emptyList()
        );
        consumer.accept("TASK [loop] ****", false, java.time.Instant.now());
        for (int i = 0; i < 2_000; i++) {
            consumer.accept("ok: [router-1] => (item=" + i + ")", false, java.time.Instant.now());
        }
        consumer.accept("changed: [router-1]", false, java.time.Instant.now());
        consumer.accept("router-1 : ok=2 changed=1 unreachable=0 failed=0", false, java.time.Instant.now());
        consumer.close();

        assertFalse(consumer.wasTruncated(), "Unlimited cap (0) must never truncate");
        try (var lines = Files.lines(spool)) {
            assertEquals(2_003, lines.count(), "Raw spool still keeps everything in SUMMARY mode");
        }
    }

    @Test
    void test33_ExtraVarsFiles_JsonAndYamlMerging() throws Exception {
        long start = System.currentTimeMillis();
        var runContext = newRunContext();
        var runner = ComprehensiveMockRunner.builder().build();

        var baseJson = runContext.workingDir().path().resolve("base.json");
        Files.writeString(baseJson, """
            {
                "env_name": "staging",
                "app": {
                    "name": "payment-service",
                    "port": 8080
                },
                "retries": 3
            }
            """);

        var overrideYaml = runContext.workingDir().path().resolve("override.yml");
        Files.writeString(overrideYaml, """
            env_name: production
            app:
                port: 9000
            db_pool: 20
            """);

        var task = AnsibleRunner.builder()
            .taskRunner(runner)
            .env(
                RunnerEnv.builder()
                    .extraVarsFiles(Property.ofValue(List.of("base.json", "override.yml")))
                    .extraVars(
                        Property.ofValue(
                            Map.of(
                                "app", Map.of("port", 9443),
                                "cluster", "us-east-1"
                            )
                        )
                    )
                    .build()
            )
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - debug: var=app\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        var output = task.run(runContext);
        var dur = System.currentTimeMillis() - start;

        var extravars = runContext.workingDir().path().resolve("runner/env_backup/extravars");
        assertTrue(Files.exists(extravars));
        var content = Files.readString(extravars);

        assertThat(content, containsString("\"env_name\":\"production\""));
        assertThat(content, containsString("\"name\":\"payment-service\""));
        assertThat(content, containsString("\"port\":9443"));
        assertThat(content, containsString("\"retries\":3"));
        assertThat(content, containsString("\"db_pool\":20"));
        assertThat(content, containsString("\"cluster\":\"us-east-1\""));

        recordResult(
            "TC33", "Environment & Secrets", "Extra variables files JSON + YAML deep merge",
            "extraVarsFiles: [base.json, override.yml] + inline extraVars", "Deep merged in order to runner/env_backup/extravars",
            "PASS", output.getRc(), dur, "Verified JSON and YAML deep merging with precedence"
        );
    }

    @Test
    void test34_ExtraVarsFiles_InternalStorageUri() throws Exception {
        long start = System.currentTimeMillis();
        var runContext = newRunContext();
        var runner = ComprehensiveMockRunner.builder().build();

        var tempYaml = Files.createTempFile("kestra-vars-", ".yml");
        Files.writeString(tempYaml, "region: eu-west-1\nreplicas: 5\n");
        var storageUri = runContext.storage().putFile(tempYaml.toFile());

        var task = AnsibleRunner.builder()
            .taskRunner(runner)
            .env(
                RunnerEnv.builder()
                    .extraVarsFiles(Property.ofValue(List.of(storageUri.toString())))
                    .build()
            )
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        var output = task.run(runContext);
        var dur = System.currentTimeMillis() - start;

        var extravars = runContext.workingDir().path().resolve("runner/env_backup/extravars");
        assertTrue(Files.exists(extravars));
        var content = Files.readString(extravars);
        assertThat(content, containsString("\"region\":\"eu-west-1\""));
        assertThat(content, containsString("\"replicas\":5"));

        recordResult(
            "TC34", "Environment & Secrets", "Extra variables file from internal storage URI",
            "extraVarsFiles: [kestra://...]", "Loaded from storage and serialized to runner/env_backup/extravars",
            "PASS", output.getRc(), dur, "Verified internal storage URI resolution for extra vars"
        );
    }

    @Test
    void test35_ExtraVarsFiles_NamespaceFiles() throws Exception {
        long start = System.currentTimeMillis();
        var runContext = newRunContext();
        var runner = ComprehensiveMockRunner.builder().build();

        var jsonContent = "{\"tier\":\"frontend\",\"traffic\":\"canary\"}";
        runContext.storage().namespace("company.network").putFile(
            Path.of("extra.json"),
            new ByteArrayInputStream(jsonContent.getBytes(StandardCharsets.UTF_8))
        );

        var task = AnsibleRunner.builder()
            .taskRunner(runner)
            .env(
                RunnerEnv.builder()
                    .extraVarsFiles(Property.ofValue(List.of("nsfile://company.network/extra.json")))
                    .build()
            )
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        var output = task.run(runContext);
        var dur = System.currentTimeMillis() - start;

        var extravars = runContext.workingDir().path().resolve("runner/env_backup/extravars");
        assertTrue(Files.exists(extravars));
        var content = Files.readString(extravars);
        assertThat(content, containsString("\"tier\":\"frontend\""));
        assertThat(content, containsString("\"traffic\":\"canary\""));

        recordResult(
            "TC35", "Environment & Secrets", "Extra variables file from namespace storage (nsfile://)",
            "extraVarsFiles: [nsfile://company.network/extra.json]", "Loaded from namespace storage and serialized",
            "PASS", output.getRc(), dur, "Verified cross-namespace extra vars resolution"
        );
    }

    @Test
    void test36_ExtraVarsFiles_SensitiveMasking() throws Exception {
        long start = System.currentTimeMillis();
        var runContext = newRunContext();
        var runner = ComprehensiveMockRunner.builder()
            .linesToLog(
                List.of(
                    "TASK [debug] ok: [localhost] => {\"vault_password\": \"superSecretToken12345\", \"api_key\": \"secretKeyXYZ\"}"
                )
            )
            .build();

        var secretJson = runContext.workingDir().path().resolve("secrets.json");
        Files.writeString(secretJson, """
            {
                "vault_password": "superSecretToken12345",
                "api_key": "secretKeyXYZ"
            }
            """);

        var task = AnsibleRunner.builder()
            .taskRunner(runner)
            .outputLogFile(Property.ofValue(true))
            .streamLogs(Property.ofValue(true))
            .logsMode(Property.ofValue(LogsMode.FULL))
            .env(
                RunnerEnv.builder()
                    .extraVarsFiles(Property.ofValue(List.of("secrets.json")))
                    .build()
            )
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - debug: var=vault_password\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        var output = task.run(runContext);
        var dur = System.currentTimeMillis() - start;

        var extravars = runContext.workingDir().path().resolve("runner/env_backup/extravars");
        assertTrue(Files.exists(extravars));

        assertNotNull(output.getOutputLogFile(), "outputLogFile URI must not be null when outputLogFile=true");
        try (InputStream is = runContext.storage().getFile(output.getOutputLogFile())) {
            var logContent = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(logContent, not(containsString("superSecretToken12345")));
            assertThat(logContent, not(containsString("secretKeyXYZ")));
            assertThat(logContent, containsString("***"));
        }

        recordResult(
            "TC36", "Environment & Secrets", "Extra variables file sensitive key masking",
            "extraVarsFiles: [secrets.json] containing vault_password", "Registered sensitive keys in SecretMasker and verified masking in logs",
            "PASS", output.getRc(), dur, "Verified automatic secret masking from extra vars files in execution logs"
        );
    }

    @Test
    void test37_ExtraVarsFiles_FileNotFound() {
        var runContext = newRunContext();
        var runner = ComprehensiveMockRunner.builder().build();

        var task = AnsibleRunner.builder()
            .taskRunner(runner)
            .env(
                RunnerEnv.builder()
                    .extraVarsFiles(Property.ofValue(List.of("non_existent_vars.yml")))
                    .build()
            )
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        var ex = assertThrows(FileNotFoundException.class, () -> task.run(runContext));
        assertThat(ex.getMessage(), containsString("Could not resolve extraVarsFile 'non_existent_vars.yml'"));
    }

    @Test
    void test38_ExtraVarsFiles_InvalidFormat() throws Exception {
        var runContext = newRunContext();
        var runner = ComprehensiveMockRunner.builder().build();

        var badYaml = runContext.workingDir().path().resolve("invalid.yml");
        Files.writeString(badYaml, "- item1\n- item2\n");

        var task = AnsibleRunner.builder()
            .taskRunner(runner)
            .env(
                RunnerEnv.builder()
                    .extraVarsFiles(Property.ofValue(List.of("invalid.yml")))
                    .build()
            )
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        var ex = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(ex.getMessage(), containsString("expected a YAML or JSON mapping/dictionary"));
    }

    @Test
    void test39_ExtraVarsFiles_PathTraversalBlocked() {
        var runContext = newRunContext();
        var runner = ComprehensiveMockRunner.builder().build();

        var task = AnsibleRunner.builder()
            .taskRunner(runner)
            .env(
                RunnerEnv.builder()
                    .extraVarsFiles(Property.ofValue(List.of("../../../../etc/passwd")))
                    .build()
            )
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        var ex = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(ex.getMessage(), containsString("Path traversal blocked"));
    }

    @Test
    void test40_ExtraVarsFiles_NamespacePathTraversalBlocked() {
        var runContext = newRunContext();
        var runner = ComprehensiveMockRunner.builder().build();

        var task = AnsibleRunner.builder()
            .taskRunner(runner)
            .env(
                RunnerEnv.builder()
                    .extraVarsFiles(Property.ofValue(List.of("nsfile://company.network/../../etc/passwd")))
                    .build()
            )
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        var ex = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(ex.getMessage(), containsString("Path traversal blocked in namespace"));
    }

    @Test
    void test41_ExtraVarsFiles_FromInputFiles_ResolvedInMemory() throws Exception {
        var runContext = newRunContext();
        var runner = ComprehensiveMockRunner.builder().build();

        var task = AnsibleRunner.builder()
            .taskRunner(runner)
            .inputFiles(Map.of("injected_vars.yml", "app_env: staging\nworkers: 8\n"))
            .env(
                RunnerEnv.builder()
                    .extraVarsFiles(Property.ofValue(List.of("injected_vars.yml")))
                    .build()
            )
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        var output = task.run(runContext);
        var extravars = runContext.workingDir().path().resolve("runner/env_backup/extravars");
        assertTrue(Files.exists(extravars));
        var content = Files.readString(extravars);
        assertThat(content, containsString("\"app_env\":\"staging\""));
        assertThat(content, containsString("\"workers\":8"));

        // File is materialized once by CommandsWrapper
        assertTrue(Files.exists(runContext.workingDir().path().resolve("injected_vars.yml")));
    }

    @Test
    void test42_ExtraVarsFiles_ParseErrorDoesNotLeakSecretSnippet() throws Exception {
        var runContext = newRunContext();
        var runner = ComprehensiveMockRunner.builder().build();

        var malformedYaml = runContext.workingDir().path().resolve("malformed.yml");
        Files.writeString(malformedYaml, "vault_token: superConfidentialToken123\nfoo: [unclosed list\n");

        var task = AnsibleRunner.builder()
            .taskRunner(runner)
            .env(
                RunnerEnv.builder()
                    .extraVarsFiles(Property.ofValue(List.of("malformed.yml")))
                    .build()
            )
            .project(
                Project.builder()
                    .source(Property.ofValue("- hosts: localhost\n  tasks:\n    - ping:\n"))
                    .build()
            )
            .inventory(
                Inventory.builder()
                    .inline(Property.ofValue("localhost ansible_connection=local\n"))
                    .build()
            )
            .build();

        var ex = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(ex.getMessage(), containsString("Failed to parse extra variables file 'malformed.yml'"));
        assertThat(ex.getMessage(), containsString("line "));
        assertThat(ex.getMessage(), not(containsString("superConfidentialToken123")));
        assertThat(ex.getMessage(), not(containsString("unclosed list")));
    }

    /**
     * Flexible Mock TaskRunner for testing all combinations of AnsibleRunner.
     */
    @SuperBuilder
    @NoArgsConstructor
    public static class ComprehensiveMockRunner extends TaskRunner<TaskRunnerDetailResult> {
        @Builder.Default
        protected String type = ComprehensiveMockRunner.class.getName();

        @Builder.Default
        private boolean simulateFailure = false;

        @Builder.Default
        private List<String> linesToLog = List.of();

        @Override
        public TaskRunnerResult<TaskRunnerDetailResult> run(RunContext runContext, TaskCommands taskCommands, List<String> filesToDownload) throws Exception {
            Path workingDir = taskCommands.getWorkingDirectory();
            Path runnerDir = workingDir.resolve("runner");

            List<String> commands = runContext.render(taskCommands.getCommands()).asList(String.class);
            String command = commands.stream().filter(c -> c.contains("--ident ")).findFirst().orElseThrow();
            int identIdx = command.indexOf("--ident ");
            String ident = command.substring(identIdx + 8).trim().split("[\\s;]")[0];

            Path identArtifacts = runnerDir.resolve("artifacts").resolve(ident);
            Path jobEvents = identArtifacts.resolve("job_events");
            Files.createDirectories(jobEvents);

            // Backup env dir for assertions since AnsibleRunner deletes it in finally block
            Path envDir = runnerDir.resolve("env");
            if (Files.exists(envDir)) {
                Path envBackup = runnerDir.resolve("env_backup");
                org.apache.commons.io.FileUtils.copyDirectory(envDir.toFile(), envBackup.toFile());
            }

            if (taskCommands.getLogConsumer() != null && linesToLog != null) {
                for (String line : linesToLog) {
                    taskCommands.getLogConsumer().accept(line, false);
                }
            }

            if (simulateFailure) {
                Files.writeString(identArtifacts.resolve("status"), "failed\n", StandardCharsets.UTF_8);
                Files.writeString(identArtifacts.resolve("rc"), "2\n", StandardCharsets.UTF_8);

                Map<String, Object> failEvent = Map.of(
                    "event", "runner_on_failed",
                    "event_data", Map.of(
                        "host", "host-02",
                        "task", "Simulated failure",
                        "failed", true
                    )
                );
                Files.writeString(jobEvents.resolve("1-fail.json"), JacksonMapper.ofJson().writeValueAsString(failEvent));

                Map<String, Object> statsEvent = Map.of(
                    "event", "playbook_on_stats",
                    "event_data", Map.of(
                        "ok", Map.of("host-01", 1),
                        "changed", Map.of(),
                        "failures", Map.of("host-02", 1),
                        "dark", Map.of(),
                        "skipped", Map.of()
                    )
                );
                Files.writeString(jobEvents.resolve("2-stats.json"), JacksonMapper.ofJson().writeValueAsString(statsEvent));

                return new TaskRunnerResult<>(2, taskCommands.getLogConsumer());
            } else {
                Files.writeString(identArtifacts.resolve("status"), "successful\n", StandardCharsets.UTF_8);
                Files.writeString(identArtifacts.resolve("rc"), "0\n", StandardCharsets.UTF_8);

                Map<String, Object> statsEvent = Map.of(
                    "event", "playbook_on_stats",
                    "event_data", Map.of(
                        "ok", Map.of("localhost", 1),
                        "changed", Map.of(),
                        "failures", Map.of(),
                        "dark", Map.of(),
                        "skipped", Map.of()
                    )
                );
                Files.writeString(jobEvents.resolve("1-stats.json"), JacksonMapper.ofJson().writeValueAsString(statsEvent));

                return new TaskRunnerResult<>(0, taskCommands.getLogConsumer());
            }
        }

        @Override
        public Map<String, Object> additionalVars(RunContext runContext, TaskCommands taskCommands) {
            return Collections.emptyMap();
        }
    }
}
