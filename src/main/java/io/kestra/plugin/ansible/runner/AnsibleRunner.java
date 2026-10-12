package io.kestra.plugin.ansible.runner;

import com.fasterxml.jackson.databind.JsonNode;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.InputFilesInterface;
import io.kestra.core.models.tasks.NamespaceFiles;
import io.kestra.core.models.tasks.NamespaceFilesInterface;
import io.kestra.core.models.tasks.OutputFilesInterface;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.NamespaceFilesUtils;
import io.kestra.plugin.ansible.runner.models.Inventory;
import io.kestra.plugin.ansible.runner.models.LogsMode;
import io.kestra.plugin.ansible.runner.models.Project;
import io.kestra.plugin.ansible.runner.models.RunnerEnv;
import io.kestra.plugin.ansible.runner.models.RunnerSummary;
import io.kestra.plugin.ansible.runner.utils.ArchiveUtils;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute Ansible playbooks using Ansible Runner",
    description = "Provides enterprise-grade Ansible orchestration with containerized Execution Environments, structured telemetry, and zero UI log bloat."
)
@Plugin(
    examples = {
        @Example(
            title = "Execute an inline Ansible playbook and inventory with zero external dependencies.",
            full = true,
            code = """
                id: run_ansible_inline
                namespace: dev

                tasks:
                  - id: run_ansible
                    type: io.kestra.plugin.ansible.runner.AnsibleRunner
                    project:
                      content: |
                        - hosts: localhost
                          tasks:
                            - name: Ping host
                              ansible.builtin.ping:
                    inventory:
                      content: |
                        localhost ansible_connection=local
                    verbosity: 1
                """
        ),
        @Example(
            title = "Execute an Ansible playbook using Namespace Files for project and inventory with live log streaming and telemetry output.",
            full = true,
            code = """
                id: run_ansible_playbook
                namespace: dev

                tasks:
                  - id: run_ansible
                    type: io.kestra.plugin.ansible.runner.AnsibleRunner
                    containerImage: quay.io/ansible/ansible-runner:latest
                    namespaceFiles:
                      enabled: true
                    project:
                      playbook: site.yml
                    inventory:
                      file: inventory/hosts.ini
                    streamLogs: true
                    outputLogFile: true
                    verbosity: 1
                """
        )
    }
)
public class AnsibleRunner extends Task implements
    RunnableTask<AnsibleRunner.Output>,
    NamespaceFilesInterface,
    InputFilesInterface,
    OutputFilesInterface {

    private static final String DEFAULT_CONTAINER_IMAGE = "quay.io/ansible/ansible-runner:stable-2.12-latest";
    private static final Pattern PKG_ALLOWLIST = Pattern.compile("^[a-zA-Z0-9_][a-zA-Z0-9_\\-\\.\\=\\>\\<\\~]*$");
    private static final Pattern WHITESPACE_PATTERN = Pattern.compile(".*\\s.*");
    private static final String DEFAULT_PLAYBOOK = "site.yml";
    private static final long DEFAULT_MAX_OUTPUTS_SIZE = 10 * 1024 * 1024L; // 10MB
    private static final long DEFAULT_MAX_LOG_LINES = 10_000L;
    private static final int LOG_BATCH_SIZE = 100;

    @Schema(title = "Execution Environment container image")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<String> containerImage = Property.ofValue(DEFAULT_CONTAINER_IMAGE);

    @Schema(
        title = "Task runner",
        description = "Runner implementation to execute the runner container; defaults to Docker."
    )
    @PluginProperty(group = "execution")
    @Builder.Default
    @Valid
    protected TaskRunner<?> taskRunner = Docker.instance();

    // --- File & Namespace Interfaces ---
    @Schema(
        title = "Namespace files configuration",
        description = "Enables loading files from Kestra namespace storage directly into the execution environment."
    )
    @PluginProperty(group = "execution")
    private NamespaceFiles namespaceFiles;

    @Schema(
        title = "Input files to inject into the working directory",
        description = "Map of additional files to create in the container environment before execution."
    )
    @PluginProperty(group = "execution")
    private Object inputFiles;

    @Schema(
        title = "Output files to capture and store in internal storage",
        description = "List of glob patterns for files to extract from the execution directory and make available as task outputs."
    )
    @PluginProperty(group = "execution")
    private Property<List<String>> outputFiles;

    // --- Dependency Management Controls ---
    @Schema(
        title = "Auto-install Ansible Galaxy collections and roles",
        description = "If true, automatically runs ansible-galaxy install -r requirements.yml if present before executing the playbook."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> autoInstallGalaxyRequirements = Property.ofValue(true);

    @Schema(
        title = "Auto-install Python requirements",
        description = "If true, automatically runs pip install --no-cache-dir -r requirements.txt if present before executing the playbook."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> autoInstallPythonRequirements = Property.ofValue(true);

    @Schema(
        title = "Explicit Ansible Galaxy collections or roles to install",
        description = "List of collection or role specifiers passed to ansible-galaxy install before execution."
    )
    @PluginProperty(group = "advanced")
    private Property<List<String>> galaxyDependencies;

    @Schema(
        title = "Explicit Python packages to install",
        description = "List of pip package specifiers passed to pip install before execution."
    )
    @PluginProperty(group = "advanced")
    private Property<List<String>> pythonDependencies;

    @Schema(
        title = "Commands to execute before the runner process",
        description = "Custom shell commands to run inside the container prior to ansible-runner execution."
    )
    @PluginProperty(group = "execution")
    private Property<List<String>> beforeCommands;

    // --- Logging & Output Controls ---
    @Schema(
        title = "Execution log verbosity mode",
        description = "Controls logging verbosity. SUMMARY streams task names, recap, and errors; FULL streams all stdout/stderr lines."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<LogsMode> logsMode = Property.ofValue(LogsMode.FULL);

    @Schema(
        title = "Stream container logs to Kestra logger in real time",
        description = "Whether to stream container stdout and stderr to the live execution logger (default true)."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> streamLogs = Property.ofValue(true);

    @Schema(
        title = "Maximum number of log lines streamed to the Kestra UI",
        description = "Protects the Kestra UI and log repository on very large runs. Lines are also batched into few log entries. " +
            "Once the limit is reached only failures, unreachable hosts and the play recap are still streamed, and the complete " +
            "log is automatically stored in `outputLogFile`. Set to 0 for unlimited (not recommended for huge playbooks)."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<@Min(0) Long> maxLogLines = Property.ofValue(DEFAULT_MAX_LOG_LINES);

    @Schema(
        title = "Whether to write the execution logs to a file in internal storage",
        description = "If true, stores the complete execution log as a file in Kestra storage and returns its URI."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> outputLogFile = Property.ofValue(false);

    @Schema(
        title = "Maximum allowed byte size for task outputs",
        description = "Guards against excessively large JSON telemetry exceeding the queue limit (default 10MB)."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<@Min(0) Long> maxOutputsSize = Property.ofValue(DEFAULT_MAX_OUTPUTS_SIZE);

    @Schema(
        title = "Generate and upload consolidated results.json report",
        description = "Whether to parse telemetry into a single structured results.json uploaded to internal storage (default true). Set to false to omit resultsUri from outputs."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> saveResults = Property.ofValue(true);

    @Schema(
        title = "Package and upload raw runner artifacts directory as a zip archive",
        description = "Whether to package the full /runner/artifacts/<ident>/ folder into a .zip archive (default true). Set to false to omit artifactsUri from outputs."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> saveArtifacts = Property.ofValue(true);

    @Schema(
        title = "Include summary metrics and changed count in task outputs",
        description = "Whether to include summary metrics (ok, changed, failures, unreachable, skipped) and changed host count in outputs (default false). When false, outputs are kept minimal."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> outputSummary = Property.ofValue(false);

    // --- Enterprise Execution Controls ---
    @Schema(title = "Dry-run check mode (--check)")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Boolean> checkMode = Property.ofValue(false);

    @Schema(title = "Show line-by-line configuration diffs (--diff)")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Boolean> diff = Property.ofValue(false);

    @Schema(title = "Limit execution to specific hosts or groups (--limit)")
    @PluginProperty(group = "execution")
    private Property<String> limit;

    @Schema(title = "Tags to execute (--tags)")
    @PluginProperty(group = "execution")
    private Property<List<String>> tags;

    @Schema(title = "Tags to bypass (--skip-tags)")
    @PluginProperty(group = "execution")
    private Property<List<String>> skipTags;

    @Schema(title = "Verbosity level (0 to 4)")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<@Min(0) @Max(4) Integer> verbosity = Property.ofValue(0);

    @Schema(title = "Number of parallel forks (default 5)")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<@Min(1) Integer> forks = Property.ofValue(5);

    // --- Project Contract (Defaults to site.yml with auto-locating) ---
    @Schema(
        title = "Project playbook configuration",
        anyOf = {Project.class, String.class}
    )
    @Builder.Default
    private Project project = Project.builder()
        .playbook(Property.ofValue(DEFAULT_PLAYBOOK))
        .build();

    // --- Inventory Contract (Nested & Flat support) ---
    @Schema(
        title = "Inventory configuration or file path",
        anyOf = {Inventory.class, String.class}
    )
    private Inventory inventory;

    // --- Environment & Credentials (Nested & Flat support) ---
    @Schema(title = "Environment and credentials configuration")
    private RunnerEnv env;

    // --- Engine Controls ---
    @Schema(title = "Execution timeout (maps to job_timeout)")
    private Property<Duration> timeout;

    @Schema(title = "Idle timeout (maps to idle_timeout)")
    private Property<Duration> idleTimeout;

    @Schema(title = "Auto-delete local artifact directory after archiving")
    @Builder.Default
    private Property<Boolean> autoCleanArtifacts = Property.ofValue(true);

    @Schema(title = "Fail task if return code is non-zero (default true)")
    @Builder.Default
    private Property<Boolean> failOnErrors = Property.ofValue(true);

    @Override
    public NamespaceFiles getNamespaceFiles() {
        return this.namespaceFiles;
    }

    @Override
    public Object getInputFiles() {
        return this.inputFiles;
    }

    @Override
    public Property<List<String>> getOutputFiles() {
        return this.outputFiles;
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        Path workingDir = runContext.workingDir().path();
        AnsibleRunnerLogConsumer logConsumer = null;
        Path logSpoolFile = Files.createTempFile("ansible-runner-log-tmp-", ".log");
        try {
        Path runnerDir = workingDir.resolve("runner");
        Path projectDir = runnerDir.resolve("project");
        Path inventoryDir = runnerDir.resolve("inventory");
        Path envDir = runnerDir.resolve("env");
        Path artifactsBaseDir = runnerDir.resolve("artifacts");

        Files.createDirectories(projectDir);
        Files.createDirectories(inventoryDir);
        Files.createDirectories(envDir);
        Files.createDirectories(artifactsBaseDir);

        // Pre-load namespace files directly into the working directory if configured
        if (this.namespaceFiles != null) {
            Boolean enabled = runContext.render(this.namespaceFiles.getEnabled()).as(Boolean.class).orElse(false);
            if (Boolean.TRUE.equals(enabled)) {
                NamespaceFilesUtils.loadNamespaceFiles(runContext, this.namespaceFiles);
            }
        }

        // 1. Resolve Playbook & Project Source
        String resolvedPlaybook = resolvePlaybook(runContext);
        resolveProjectSource(runContext, workingDir, projectDir, resolvedPlaybook);

        // 2. Resolve Inventory
        resolveInventory(runContext, workingDir, inventoryDir);

        // 3. Populate /runner/env/ (cmdline, extravars, envvars, passwords, ssh_key, settings)
        populateEnvDir(runContext, envDir);

        // 4. Configure Runner execution & Logging
        String ident = IdUtils.create();
        String renderedImage = runContext.render(this.containerImage).as(String.class).orElse(DEFAULT_CONTAINER_IMAGE);

        // Playbook-level failures (rc=2 failed hosts, rc=4 unreachable) must not make the task runner
        // throw before telemetry is parsed. If ansible-runner produced an rc artifact, exit 0 and let
        // failOnErrors decide; otherwise (runner never started) propagate the real exit code.
        List<String> runnerCommand = List.of(
            "ansible-runner run ./runner -p '" + resolvedPlaybook.replace("'", "'\\''") + "' --ident " + ident
                + "; __rc=$?; if [ -f ./runner/artifacts/" + ident + "/rc ]; then if [ $__rc -eq 2 ] || [ $__rc -eq 4 ]; then exit 0; fi; fi; exit $__rc"
        );

        boolean shouldStreamLogs = runContext.render(this.streamLogs).as(Boolean.class).orElse(true);
        LogsMode resolvedLogsMode = runContext.render(this.logsMode).as(LogsMode.class).orElse(LogsMode.FULL);
        boolean shouldOutputLogFile = runContext.render(this.outputLogFile).as(Boolean.class).orElse(false);
        long resolvedMaxLogLines = runContext.render(this.maxLogLines).as(Long.class).orElse(DEFAULT_MAX_LOG_LINES);

        // Full raw output is always spooled to disk (never kept in memory) so it can be uploaded without loss.
        // It lives in the system temp directory outside the container working directory to prevent Docker volume mount collisions.
        try {
            Files.setPosixFilePermissions(logSpoolFile, PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException ignored) {
            logSpoolFile.toFile().setReadable(true, true);
            logSpoolFile.toFile().setWritable(true, true);
        }

        List<String> secrets = new ArrayList<>();
        if (this.env != null) {
            if (this.env.getSshKey() != null) {
                String k = runContext.render(this.env.getSshKey()).as(String.class).orElse(null);
                if (k != null && !k.isBlank()) {
                    for (String line : k.split("\\r?\\n")) {
                        String trimmed = line.trim();
                        if (trimmed.length() > 5) {
                            secrets.add(trimmed);
                        }
                    }
                }
            }
            if (this.env.getPasswords() != null) {
                Map<String, String> map = runContext.render(this.env.getPasswords()).asMap(String.class, String.class);
                if (map != null) {
                    for (String val : map.values()) {
                        if (val != null && !val.isBlank()) {
                            String renderedVal = runContext.render(val);
                            if (renderedVal != null && !renderedVal.isBlank()) {
                                secrets.add(renderedVal);
                            }
                        }
                    }
                }
            }
        }

        logConsumer = new AnsibleRunnerLogConsumer(
            runContext,
            shouldStreamLogs,
            resolvedLogsMode,
            logSpoolFile,
            resolvedMaxLogLines,
            LOG_BATCH_SIZE,
            secrets
        );

        CommandsWrapper commandsWrapper = new CommandsWrapper(runContext)
            .withLogConsumer(logConsumer)
            .withTaskRunner(this.taskRunner)
            .withContainerImage(renderedImage)
            .withInterpreter(Property.ofValue(List.of("/bin/bash", "-c")))
            .withCommands(Property.ofValue(runnerCommand));

        // Configure pre-execution dependency installations
        List<String> preCommands = buildPreCommands(runContext, workingDir);
        if (!preCommands.isEmpty()) {
            commandsWrapper = commandsWrapper.withBeforeCommands(Property.ofValue(preCommands));
        }

        if (this.inputFiles != null) {
            commandsWrapper = commandsWrapper.withInputFiles(this.inputFiles);
        }

        if (this.outputFiles != null) {
            List<String> renderedOutputFiles = runContext.render(this.outputFiles).asList(String.class);
            List<String> outputPatterns = new ArrayList<>();
            for (String pattern : renderedOutputFiles) {
                outputPatterns.add(pattern);
                if (!pattern.startsWith("/") && !pattern.startsWith("**/") && !pattern.startsWith("runner/")) {
                    outputPatterns.add("runner/project/" + pattern);
                    outputPatterns.add("runner/" + pattern);
                }
            }
            outputPatterns.add("runner/artifacts/" + ident + "/**");
            commandsWrapper = commandsWrapper.withOutputFiles(outputPatterns);
        }

        ScriptOutput scriptOutput = commandsWrapper.run();
        int processExitCode = scriptOutput.getExitCode();
        Map<String, URI> extractedOutputFiles = scriptOutput.getOutputFiles();

        // 5. Parse Status, Return Code, and Job Events Telemetry
        Path identArtifactsDir = artifactsBaseDir.resolve(ident);
        ExecutionTelemetry telemetry = parseTelemetry(runContext, identArtifactsDir, processExitCode, secrets);

        // Check telemetry size against maxOutputsSize
        long maxSize = runContext.render(this.maxOutputsSize).as(Long.class).orElse(DEFAULT_MAX_OUTPUTS_SIZE);
        String telemetryJson = JacksonMapper.ofJson().writeValueAsString(telemetry);
        if (telemetryJson.getBytes(StandardCharsets.UTF_8).length > maxSize) {
            throw new IllegalStateException("Ansible outputs telemetry exceeds the configured maxOutputsSize of " + maxSize + " bytes.");
        }

        // 6. Generate single consolidated results.json file and upload to Internal Storage if saveResults is true
        URI resultsUri = null;
        boolean shouldSaveResults = runContext.render(this.saveResults).as(Boolean.class).orElse(true);
        if (shouldSaveResults && Files.exists(identArtifactsDir)) {
            Map<String, Object> consolidatedResults = new LinkedHashMap<>();
            consolidatedResults.put("status", telemetry.status);
            consolidatedResults.put("rc", telemetry.rc);
            consolidatedResults.put("summary", telemetry.summary);
            consolidatedResults.put("stats", telemetry.stats);
            consolidatedResults.put("failedHosts", telemetry.failedHosts);
            consolidatedResults.put("failedTasks", telemetry.failedTasks);
            // consolidatedResults.put("tasks", telemetry.tasks);

            Path resultsFile = workingDir.resolve("results-" + ident + ".json");
            Files.writeString(
                resultsFile,
                JacksonMapper.ofJson().writerWithDefaultPrettyPrinter().writeValueAsString(consolidatedResults),
                StandardCharsets.UTF_8
            );
            resultsUri = runContext.storage().putFile(resultsFile.toFile());
            Files.deleteIfExists(resultsFile);
        }

        Path zipFile = workingDir.resolve("artifacts-" + ident + ".zip");
        URI artifactsUri = null;
        boolean shouldSaveArtifacts = runContext.render(this.saveArtifacts).as(Boolean.class).orElse(true);
        if (shouldSaveArtifacts && Files.exists(identArtifactsDir)) {
            ArchiveUtils.zipDirectory(identArtifactsDir, zipFile);
            artifactsUri = runContext.storage().putFile(zipFile.toFile());
        }

        if (logConsumer != null) {
            logConsumer.close();
        }
        URI logFileUri = null;
        if ((shouldOutputLogFile || logConsumer.wasTruncated()) && Files.exists(logSpoolFile)) {
            logFileUri = runContext.storage().putFile(logSpoolFile.toFile());
        }
        Files.deleteIfExists(logSpoolFile);

        boolean shouldClean = runContext.render(this.autoCleanArtifacts).as(Boolean.class).orElse(true);
        if (shouldClean) {
            FileUtils.deleteQuietly(artifactsBaseDir.toFile());
            Files.deleteIfExists(zipFile);
        }

        Map<String, URI> finalOutputFiles = (this.outputFiles != null && extractedOutputFiles != null && !extractedOutputFiles.isEmpty())
            ? extractedOutputFiles
            : null;

        boolean shouldOutputSummary = runContext.render(this.outputSummary).as(Boolean.class).orElse(false);

        Output output = Output.builder()
            .resultsUri(resultsUri)
            .artifactsUri(artifactsUri)
            .outputLogFile(logFileUri)
            .outputFiles(finalOutputFiles)
            .status(telemetry.status)
            .rc(telemetry.rc)
            .failures(telemetry.summary.getFailures())
            .changed(shouldOutputSummary ? telemetry.summary.getChanged() : null)
            .failedHosts(telemetry.failedHosts)
            .stats(telemetry.stats)
            .summary(shouldOutputSummary ? telemetry.summary : null)
            .build();

        boolean failTask = runContext.render(this.failOnErrors).as(Boolean.class).orElse(true);
        if (failTask && !"successful".equalsIgnoreCase(telemetry.status)) {
            runContext.logger().error("Ansible Runner failed. Results: {}, Artifacts: {}, Logs: {}", resultsUri, artifactsUri, logFileUri);
            throw new IllegalStateException("Ansible Runner failed with status '" + telemetry.status + "' and exit code " + telemetry.rc +
                " on hosts: " + telemetry.failedHosts);
        }

        return output;
        } catch (Exception e) {
            try { Files.deleteIfExists(logSpoolFile); } catch (Exception ignored) {}
            throw e;
        } finally {
            if (logConsumer != null) logConsumer.close();
            FileUtils.deleteQuietly(workingDir.resolve("runner/env").toFile());
        }
    }

    private List<String> buildPreCommands(RunContext runContext, Path workingDir) throws IllegalVariableEvaluationException {
        List<String> cmds = new ArrayList<>();

        // Explicit before commands from user
        if (this.beforeCommands != null) {
            cmds.addAll(runContext.render(this.beforeCommands).asList(String.class));
        }

        // Auto-install Galaxy collections/roles
        boolean autoGalaxy = runContext.render(this.autoInstallGalaxyRequirements).as(Boolean.class).orElse(true);
        List<String> galaxyDeps = runContext.render(this.galaxyDependencies).asList(String.class);
        if (autoGalaxy) {
            cmds.add("if [ -f ./runner/project/requirements.yml ]; then ansible-galaxy install -r ./runner/project/requirements.yml; elif [ -f requirements.yml ]; then ansible-galaxy install -r requirements.yml; fi");
        }
        if (!galaxyDeps.isEmpty()) {
            for (String dep : galaxyDeps) {
                if (!PKG_ALLOWLIST.matcher(dep).matches()) {
                    throw new IllegalArgumentException("Invalid galaxy dependency: " + dep + ". Must match " + PKG_ALLOWLIST.pattern());
                }
            }
            List<String> quotedGalaxyDeps = galaxyDeps.stream().map(d -> "'" + d.replace("'", "'\\''") + "'").toList();
            cmds.add("ansible-galaxy collection install " + String.join(" ", quotedGalaxyDeps));
        }

        // Auto-install Python requirements
        boolean autoPython = runContext.render(this.autoInstallPythonRequirements).as(Boolean.class).orElse(true);
        List<String> pythonDeps = runContext.render(this.pythonDependencies).asList(String.class);
        if (autoPython) {
            cmds.add("if [ -f ./runner/project/requirements.txt ]; then pip install --no-cache-dir -r ./runner/project/requirements.txt; elif [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi");
        }
        if (!pythonDeps.isEmpty()) {
            for (String dep : pythonDeps) {
                if (!PKG_ALLOWLIST.matcher(dep).matches()) {
                    throw new IllegalArgumentException("Invalid python dependency: " + dep + ". Must match " + PKG_ALLOWLIST.pattern());
                }
            }
            List<String> quotedPythonDeps = pythonDeps.stream().map(d -> "'" + d.replace("'", "'\\''") + "'").toList();
            cmds.add("pip install --no-cache-dir " + String.join(" ", quotedPythonDeps));
        }

        return cmds;
    }

    private String resolvePlaybook(RunContext runContext) throws IllegalVariableEvaluationException {
        if (this.project != null && this.project.getPlaybook() != null) {
            return runContext.render(this.project.getPlaybook()).as(String.class).orElse(DEFAULT_PLAYBOOK);
        }
        return DEFAULT_PLAYBOOK;
    }

    private void resolveProjectSource(RunContext runContext, Path workingDir, Path projectDir, String playbookName)
        throws Exception {
        String inlineContent = null;
        if (this.project != null) {
            if (this.project.getContent() != null) {
                inlineContent = runContext.render(this.project.getContent()).as(String.class).orElse(null);
            } else if (this.project.getInline() != null) {
                inlineContent = runContext.render(this.project.getInline()).as(String.class).orElse(null);
            }
        }

        if (inlineContent != null && !inlineContent.isBlank()) {
            writePlaybookContent(projectDir, playbookName, inlineContent);
            return;
        }

        String sourceStr = null;
        if (this.project != null && this.project.getSource() != null) {
            sourceStr = runContext.render(this.project.getSource()).as(String.class).orElse(null);
        }
        

        if (sourceStr != null && !sourceStr.isBlank()) {
            if (sourceStr.contains("\n") || sourceStr.trim().startsWith("---") || sourceStr.trim().startsWith("- ") || sourceStr.trim().startsWith("----")) {
                writePlaybookContent(projectDir, playbookName, sourceStr);
                return;
            }

            if (sourceStr.startsWith("kestra://") || sourceStr.contains("://")) {
                URI sourceUri = URI.create(sourceStr);
                try (InputStream is = runContext.storage().getFile(sourceUri)) {
                    if (sourceStr.endsWith(".zip")) {
                        ArchiveUtils.unzip(is, projectDir);
                    } else if (sourceStr.endsWith(".tar.gz") || sourceStr.endsWith(".tgz")) {
                        ArchiveUtils.untarGz(is, projectDir);
                    } else {
                        Files.copy(is, secureResolve(projectDir, playbookName), StandardCopyOption.REPLACE_EXISTING);
                    }
                }
            } else {
                try {
                    String relSource = sourceStr.startsWith("/") ? sourceStr.substring(1) : sourceStr;
                    Path localSource = secureResolve(workingDir, relSource);
                    if (Files.isDirectory(localSource)) {
                        FileUtils.copyDirectory(localSource.toFile(), projectDir.toFile());
                    } else if (Files.isRegularFile(localSource)) {
                        if (relSource.endsWith(".zip")) {
                            try (InputStream is = Files.newInputStream(localSource)) {
                                ArchiveUtils.unzip(is, projectDir);
                            }
                        } else if (relSource.endsWith(".tar.gz") || relSource.endsWith(".tgz")) {
                            try (InputStream is = Files.newInputStream(localSource)) {
                                ArchiveUtils.untarGz(is, projectDir);
                            }
                        } else {
                            Files.copy(localSource, secureResolve(projectDir, playbookName), StandardCopyOption.REPLACE_EXISTING);
                        }
                    } else {
                        writePlaybookContent(projectDir, playbookName, sourceStr);
                    }
                } catch (Exception e) {
                    if (sourceStr.startsWith("kestra://")) {
                        throw new IllegalStateException("Failed to read Kestra internal storage playbook: " + sourceStr, e);
                    }
                    runContext.logger().warn("Failed to parse playbook as local file, treating as inline content");
                    writePlaybookContent(projectDir, playbookName, sourceStr);
                }
            }
        }

        // Auto-locate playbook in workingDir (e.g. from namespace files or current workspace)
        Path targetPlaybook = secureResolve(projectDir, playbookName);
        if (!Files.exists(targetPlaybook)) {
            Path directWorkingDirFile = workingDir.resolve(playbookName);
            Path playbooksSubDirFile = workingDir.resolve("playbooks").resolve(playbookName);
            if (Files.exists(directWorkingDirFile)) {
                Files.copy(directWorkingDirFile, targetPlaybook, StandardCopyOption.REPLACE_EXISTING);
            } else if (Files.exists(playbooksSubDirFile)) {
                Files.copy(playbooksSubDirFile, targetPlaybook, StandardCopyOption.REPLACE_EXISTING);
            } else {
                try (Stream<Path> stream = Files.walk(workingDir, 4)) {
                    Optional<Path> found = stream
                        .filter(p -> Files.isRegularFile(p) && p.getFileName().toString().equals(playbookName))
                        .filter(p -> !p.startsWith(projectDir))
                        .findFirst();
                    if (found.isPresent()) {
                        Files.copy(found.get(), targetPlaybook, StandardCopyOption.REPLACE_EXISTING);
                    }
                }
            }
        }

        if (!Files.exists(targetPlaybook)) {
            throw new IllegalArgumentException("Playbook not found: " + playbookName);
        }

        // If workingDir has a playbooks directory, copy its entire contents to projectDir
        Path playbooksDir = workingDir.resolve("playbooks");
        if (Files.isDirectory(playbooksDir)) {
            try {
                FileUtils.copyDirectory(playbooksDir.toFile(), projectDir.toFile());
            } catch (Exception e) { runContext.logger().warn("Failed to copy playbooks dir: {}", e.getMessage()); }
        }

        // Support standard Ansible project directories and files from workingDir into runner/project/
        List<String> standardDirs = List.of(
            "roles", "templates", "scripts", "group_vars", "host_vars",
            "library", "lookup_plugins", "filter_plugins", "callback_plugins",
            "module_utils", "collections"
        );
        for (String dirName : standardDirs) {
            Path srcDir = workingDir.resolve(dirName);
            Path destDir = projectDir.resolve(dirName);
            if (Files.isDirectory(srcDir) && !Files.exists(destDir)) {
                FileUtils.copyDirectory(srcDir.toFile(), destDir.toFile());
            }
        }

        Path ansibleCfg = workingDir.resolve("ansible.cfg");
        if (Files.exists(ansibleCfg) && !Files.exists(projectDir.resolve("ansible.cfg"))) {
            Files.copy(ansibleCfg, projectDir.resolve("ansible.cfg"), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private void writePlaybookContent(Path projectDir, String playbookName, String content) throws IOException {
        String sanitized = content.trim();
        if (sanitized.startsWith("{") && sanitized.endsWith("}")) {
            try {
                Map<String, Object> map = JacksonMapper.ofJson().readValue(
                    sanitized,
                    new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {}
                );
                if (!map.isEmpty() && map.keySet().stream().anyMatch(k -> k.endsWith(".yml") || k.endsWith(".yaml") || k.contains("/"))) {
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        Path target = secureResolve(projectDir, entry.getKey());
                        if (target.getParent() != null) {
                            Files.createDirectories(target.getParent());
                        }
                        String fileVal = entry.getValue() != null ? entry.getValue().toString() : "";
                        writePlaybookContent(target.getParent() != null ? target.getParent() : projectDir, target.getFileName().toString(), fileVal);
                    }
                    return;
                }
            } catch (Exception ignored) {
            }
        }
        if (sanitized.startsWith("----")) {
            sanitized = sanitized.replaceFirst("^----+(\\r?\\n)", "---$1");
            if (sanitized.startsWith("----")) {
                sanitized = sanitized.replaceFirst("^----+", "---");
            }
        }
        Files.writeString(secureResolve(projectDir, playbookName), sanitized + "\n", StandardCharsets.UTF_8);
    }

    private void resolveInventory(RunContext runContext, Path workingDir, Path inventoryDir) throws Exception {
        // 1. Inline INI/YAML content
        String invContent = null;
        if (this.inventory != null) {
            if (this.inventory.getContent() != null) {
                invContent = runContext.render(this.inventory.getContent()).as(String.class).orElse(null);
            } else if (this.inventory.getInline() != null) {
                invContent = runContext.render(this.inventory.getInline()).as(String.class).orElse(null);
            }
        }
        

        if (invContent != null && !invContent.isBlank()) {
            Files.writeString(inventoryDir.resolve("hosts"), invContent.trim() + "\n", StandardCharsets.UTF_8);
            return;
        }

        // 2. Structured hosts map (serialized to JSON inventory)
        if (this.inventory != null && this.inventory.getHosts() != null) {
            Map<String, Object> renderedHosts = runContext.render(this.inventory.getHosts()).asMap(String.class, Object.class);
            if (!renderedHosts.isEmpty()) {
                Map<String, Object> jsonInventory = new HashMap<>();
                Map<String, Object> allGroup = new HashMap<>();
                allGroup.put("hosts", new ArrayList<>(renderedHosts.keySet()));
                jsonInventory.put("all", allGroup);
                Map<String, Object> meta = new HashMap<>();
                meta.put("hostvars", renderedHosts);
                jsonInventory.put("_meta", meta);
                Files.writeString(inventoryDir.resolve("hosts.json"), JacksonMapper.ofJson().writeValueAsString(jsonInventory), StandardCharsets.UTF_8);
                return;
            }
        }

        // 3. Structured groups map (serialized to JSON inventory)
        if (this.inventory != null && this.inventory.getGroups() != null) {
            Map<String, Object> renderedGroups = runContext.render(this.inventory.getGroups()).asMap(String.class, Object.class);
            if (!renderedGroups.isEmpty()) {
                Files.writeString(inventoryDir.resolve("hosts.json"), JacksonMapper.ofJson().writeValueAsString(renderedGroups), StandardCharsets.UTF_8);
                return;
            }
        }

        // 4. Dynamic inventory script
        if (this.inventory != null && this.inventory.getScript() != null) {
            String scriptStr = runContext.render(this.inventory.getScript()).as(String.class).orElse(null);
            if (scriptStr != null && !scriptStr.isBlank()) {
                Path scriptTarget = inventoryDir.resolve("hosts");
                Path localScript = secureResolve(workingDir, scriptStr.startsWith("/") ? scriptStr.substring(1) : scriptStr);
                if (Files.isRegularFile(localScript)) {
                    Files.copy(localScript, scriptTarget, StandardCopyOption.REPLACE_EXISTING);
                } else {
                    Files.writeString(scriptTarget, scriptStr.trim() + "\n", StandardCharsets.UTF_8);
                }
                makeExecutable(scriptTarget);
                return;
            }
        }

        // 5. Multiple inventory sources
        if (this.inventory != null && this.inventory.getSources() != null) {
            List<String> renderedSources = runContext.render(this.inventory.getSources()).asList(String.class);
            if (!renderedSources.isEmpty()) {
                for (String src : renderedSources) {
                    if (src.startsWith("kestra://") || src.contains("://")) {
                        try (InputStream is = runContext.storage().getFile(URI.create(src))) {
                            Path target = inventoryDir.resolve(Paths.get(URI.create(src).getPath()).getFileName().toString());
                            Files.copy(is, target, StandardCopyOption.REPLACE_EXISTING);
                        }
                    } else {
                        Path localSrc = secureResolve(workingDir, src.startsWith("/") ? src.substring(1) : src);
                        if (Files.isDirectory(localSrc)) {
                            FileUtils.copyDirectory(localSrc.toFile(), inventoryDir.toFile());
                        } else if (Files.isRegularFile(localSrc)) {
                            Files.copy(localSrc, inventoryDir.resolve(localSrc.getFileName().toString()), StandardCopyOption.REPLACE_EXISTING);
                        }
                    }
                }
                return;
            }
        }

        // 6. Single inventory file or string path
        String invFile = null;
        if (this.inventory != null && this.inventory.getFile() != null) {
            invFile = runContext.render(this.inventory.getFile()).as(String.class).orElse(null);
        }
        

        if (invFile != null && !invFile.isBlank()) {
            if (invFile.startsWith("kestra://") || invFile.contains("://")) {
                try (InputStream is = runContext.storage().getFile(URI.create(invFile))) {
                    Files.copy(is, inventoryDir.resolve("hosts"), StandardCopyOption.REPLACE_EXISTING);
                }
            } else {
                String relInv = invFile.startsWith("/") ? invFile.substring(1) : invFile;
                Path localInv = secureResolve(workingDir, relInv);
                Path localSubInv = secureResolve(workingDir.resolve("inventory"), relInv);
                Path localPluralInv = secureResolve(workingDir.resolve("inventories"), relInv);

                if (Files.isDirectory(localInv)) {
                    FileUtils.copyDirectory(localInv.toFile(), inventoryDir.toFile());
                } else if (Files.isRegularFile(localInv)) {
                    Files.copy(localInv, inventoryDir.resolve("hosts"), StandardCopyOption.REPLACE_EXISTING);
                } else if (Files.isRegularFile(localSubInv)) {
                    Files.copy(localSubInv, inventoryDir.resolve("hosts"), StandardCopyOption.REPLACE_EXISTING);
                } else if (Files.isRegularFile(localPluralInv)) {
                    Files.copy(localPluralInv, inventoryDir.resolve("hosts"), StandardCopyOption.REPLACE_EXISTING);
                } else if (invFile.contains("\n") || invFile.contains("[") || invFile.contains(",")) {
                    Files.writeString(inventoryDir.resolve("hosts"), invFile.trim() + "\n", StandardCharsets.UTF_8);
                } else {
                    Files.writeString(inventoryDir.resolve("hosts"), invFile.trim() + "\n", StandardCharsets.UTF_8);
                }
            }
            return;
        }

        // 7. Auto-detect inventory from workingDir if present (e.g. from namespace files)
        Path inventoryFolder = workingDir.resolve("inventory");
        if (Files.isDirectory(inventoryFolder)) {
            try {
                FileUtils.copyDirectory(inventoryFolder.toFile(), inventoryDir.toFile());
                try (java.util.stream.Stream<Path> invStream = Files.list(inventoryDir)) {
                    if (invStream.findAny().isPresent()) {
                    runContext.logger().info("Using auto-detected inventory directory: {}", inventoryFolder);
                        return;
                    }
                }
            } catch (Exception ignored) {}
        }
        Path inventoriesFolder = workingDir.resolve("inventories");
        if (Files.isDirectory(inventoriesFolder)) {
            try {
                FileUtils.copyDirectory(inventoriesFolder.toFile(), inventoryDir.toFile());
                try (java.util.stream.Stream<Path> invStream = Files.list(inventoryDir)) {
                    if (invStream.findAny().isPresent()) {
                    runContext.logger().info("Using auto-detected inventory directory: {}", inventoriesFolder);
                        return;
                    }
                }
            } catch (Exception ignored) {}
        }

        List<String> candidateInvs = List.of(
            "inventory/hosts.ini", "inventory/hosts", "inventory/hosts.yml", "inventory/hosts.yaml",
            "hosts.ini", "hosts", "hosts.yml", "hosts.yaml", "inventory.ini"
        );
        for (String cand : candidateInvs) {
            Path candPath = secureResolve(workingDir, cand);
            if (Files.isRegularFile(candPath)) {
                Files.copy(candPath, inventoryDir.resolve("hosts"), StandardCopyOption.REPLACE_EXISTING);
                return;
            }
        }

        // No default inventory fallback. Ansible will fail if none provided and not implicit.
    }

    private void makeExecutable(Path path) {
        try {
            Set<PosixFilePermission> perms = Files.getPosixFilePermissions(path);
            perms.add(PosixFilePermission.OWNER_EXECUTE);
            perms.add(PosixFilePermission.GROUP_EXECUTE);
            perms.add(PosixFilePermission.OTHERS_EXECUTE);
            Files.setPosixFilePermissions(path, perms);
        } catch (UnsupportedOperationException e) {
            path.toFile().setExecutable(true, false);
        } catch (IOException ignored) {}
    }

    private void populateEnvDir(RunContext runContext, Path envDir) throws Exception {
        // A. cmdline
        List<String> cmdArgs = new ArrayList<>();
        if (Boolean.TRUE.equals(runContext.render(this.checkMode).as(Boolean.class).orElse(false))) {
            cmdArgs.add("--check");
        }
        if (Boolean.TRUE.equals(runContext.render(this.diff).as(Boolean.class).orElse(false))) {
            cmdArgs.add("--diff");
        }
        String renderedLimit = runContext.render(this.limit).as(String.class).orElse(null);
        if (renderedLimit != null && !renderedLimit.isBlank()) {
            validateCmdlineArg(renderedLimit);
            cmdArgs.add("--limit");
            cmdArgs.add(renderedLimit);
        }
        List<String> renderedTags = runContext.render(this.tags).asList(String.class);
        if (!renderedTags.isEmpty()) {
            for (String t : renderedTags) validateCmdlineArg(t);
            cmdArgs.add("--tags");
            cmdArgs.add(String.join(",", renderedTags));
        }
        List<String> renderedSkipTags = runContext.render(this.skipTags).asList(String.class);
        if (!renderedSkipTags.isEmpty()) {
            for (String t : renderedSkipTags) validateCmdlineArg(t);
            cmdArgs.add("--skip-tags");
            cmdArgs.add(String.join(",", renderedSkipTags));
        }
        Integer rForks = runContext.render(this.forks).as(Integer.class).orElse(5);
        if (rForks != null) {
            if (rForks < 1) {
                throw new IllegalArgumentException("forks must be greater than or equal to 1");
            }
            cmdArgs.add("-f");
            cmdArgs.add(String.valueOf(rForks));
        }
        Integer rVerbosity = runContext.render(this.verbosity).as(Integer.class).orElse(0);
        if (rVerbosity != null) {
            if (rVerbosity < 0 || rVerbosity > 4) {
                throw new IllegalArgumentException("verbosity must be between 0 and 4");
            }
            if (rVerbosity > 0) {
                cmdArgs.add("-" + "v".repeat(rVerbosity));
            }
        }
        if (!cmdArgs.isEmpty()) {
            Files.writeString(envDir.resolve("cmdline"), String.join(" ", cmdArgs) + "\n", StandardCharsets.UTF_8);
        }

        // B. extravars
        if (this.env != null && this.env.getExtraVars() != null) {
            Map<String, Object> rawExtraVars = runContext.render(this.env.getExtraVars()).asMap(String.class, Object.class);
            if (rawExtraVars != null && !rawExtraVars.isEmpty()) {
                Files.writeString(envDir.resolve("extravars"), JacksonMapper.ofJson().writeValueAsString(rawExtraVars), StandardCharsets.UTF_8);
            }
        }

        // C. envvars
        if (this.env != null && this.env.getEnvVars() != null) {
            Map<String, String> rawEnvVars = runContext.render(this.env.getEnvVars()).asMap(String.class, String.class);
            if (rawEnvVars != null && !rawEnvVars.isEmpty()) {
                Files.writeString(envDir.resolve("envvars"), JacksonMapper.ofJson().writeValueAsString(rawEnvVars), StandardCharsets.UTF_8);
            }
        }

        // D. passwords (secure secret mapping rendered at point of write)
        if (this.env != null && this.env.getPasswords() != null) {
            Map<String, String> rawPasswords = runContext.render(this.env.getPasswords()).asMap(String.class, String.class);
            if (rawPasswords != null && !rawPasswords.isEmpty()) {
                Map<String, String> renderedPasswords = new HashMap<>();
                for (Map.Entry<String, String> entry : rawPasswords.entrySet()) {
                    String val = entry.getValue() != null ? runContext.render(entry.getValue()) : null;
                    renderedPasswords.put(entry.getKey(), val);
                }
                Files.writeString(envDir.resolve("passwords"), JacksonMapper.ofJson().writeValueAsString(renderedPasswords), StandardCharsets.UTF_8);
            }
        }

        // E. ssh_key (written with secure 0600 POSIX permissions)
        String rawKey = null;
        if (this.env != null && this.env.getSshKey() != null) {
            rawKey = runContext.render(this.env.getSshKey()).as(String.class).orElse(null);
        }
        
        if (rawKey != null && !rawKey.isBlank()) {
            Path sshKeyPath = envDir.resolve("ssh_key");
            Files.writeString(sshKeyPath, rawKey.trim() + "\n", StandardCharsets.UTF_8);
            try {
                Files.setPosixFilePermissions(sshKeyPath, PosixFilePermissions.fromString("rw-------"));
            } catch (UnsupportedOperationException ignored) {
                sshKeyPath.toFile().setReadable(true, true);
                sshKeyPath.toFile().setWritable(true, true);
                sshKeyPath.toFile().setExecutable(false, false);
            }
        }

        // F. settings (execution timeout & idle timeout)
        Map<String, Object> settings = new HashMap<>();
        if (this.timeout != null) {
            Duration dur = runContext.render(this.timeout).as(Duration.class).orElse(null);
            if (dur != null) {
                settings.put("job_timeout", dur.toSeconds());
            }
        }
        if (this.idleTimeout != null) {
            Duration dur = runContext.render(this.idleTimeout).as(Duration.class).orElse(null);
            if (dur != null) {
                settings.put("idle_timeout", dur.toSeconds());
            }
        }
        if (!settings.isEmpty()) {
            Files.writeString(envDir.resolve("settings"), JacksonMapper.ofJson().writeValueAsString(settings), StandardCharsets.UTF_8);
        }
    }

    private ExecutionTelemetry parseTelemetry(RunContext runContext, Path identArtifactsDir, int processExitCode, List<String> secrets) {
        String status = "failed";
        int rc = processExitCode;
        Set<String> failedHostsSet = new java.util.LinkedHashSet<>();
        List<Map<String, Object>> failedTasks = new ArrayList<>();
        // tasks list removed to prevent memory exhaustion
        Map<String, Object> stats = new HashMap<>();
        RunnerSummary.RunnerSummaryBuilder summaryBuilder = RunnerSummary.builder()
            .ok(0)
            .changed(0)
            .failures(0)
            .unreachable(0)
            .skipped(0);

        if (!Files.exists(identArtifactsDir)) {
            if (processExitCode == 0) {
                throw new IllegalStateException("Ansible runner execution failed: process completed with exit code 0, but artifacts directory was not found at: " + identArtifactsDir);
            }
        }

        if (Files.exists(identArtifactsDir)) {
            // Read status
            Path statusFile = identArtifactsDir.resolve("status");
            if (Files.exists(statusFile)) {
                try {
                    status = Files.readString(statusFile, StandardCharsets.UTF_8).trim();
                } catch (IOException ignored) {}
            }

            // Read rc
            Path rcFile = identArtifactsDir.resolve("rc");
            if (Files.exists(rcFile)) {
                try {
                    rc = Integer.parseInt(Files.readString(rcFile, StandardCharsets.UTF_8).trim());
                } catch (Exception e) {
                    runContext.logger().warn("Failed to parse rc file: {}", e.getMessage());
                }
            }

            // Read job_events
            Path jobEventsDir = identArtifactsDir.resolve("job_events");
            if (Files.isDirectory(jobEventsDir)) {
                try (Stream<Path> stream = Files.list(jobEventsDir)) {
                    List<Path> eventFiles = stream
                        .filter(p -> p.toString().endsWith(".json"))
                        .sorted(Comparator.comparing(Path::getFileName))
                        .toList();

                    for (Path eventFile : eventFiles) {
                        try {
                            JsonNode root = JacksonMapper.ofJson().readTree(eventFile.toFile());
                            String event = root.path("event").asText("");
                            JsonNode eventData = root.path("event_data");

                            String host = eventData.path("remote_addr").asText(eventData.path("host").asText(""));
                            String taskName = eventData.path("task").asText("");
                            String playName = eventData.path("play").asText("");

                            if ("runner_on_failed".equals(event) || "runner_on_unreachable".equals(event)) {
                                if (!host.isBlank()) {
                                    if (failedHostsSet.size() < 1000) failedHostsSet.add(host);
                                }
                                Map<String, Object> failedTask = new LinkedHashMap<>();
                                failedTask.put("host", host);
                                failedTask.put("task", taskName);
                                failedTask.put("play", playName);
                                failedTask.put("event", event);
                                String msg = eventData.path("res").path("msg").asText("");
                                if (!msg.isBlank()) {
                                    if (secrets != null) {
                                        for (String secret : secrets) {
                                            if (secret != null && !secret.isBlank()) {
                                                msg = msg.replace(secret, "***");
                                            }
                                        }
                                    }
                                    failedTask.put("error", msg);
                                }
                                if (failedTasks.size() < 1000) failedTasks.add(failedTask);
                            }

                            if ("playbook_on_stats".equals(event)) {
                                JsonNode changedNode = eventData.path("changed");
                                JsonNode failuresNode = eventData.path("failures");
                                JsonNode okNode = eventData.path("ok");
                                JsonNode unreachableNode = eventData.path("dark");
                                JsonNode skippedNode = eventData.path("skipped");

                                int totChanged = sumJsonNode(changedNode);
                                int totFailures = sumJsonNode(failuresNode);
                                int totOk = sumJsonNode(okNode);
                                int totUnreachable = sumJsonNode(unreachableNode);
                                int totSkipped = sumJsonNode(skippedNode);

                                summaryBuilder
                                    .changed(totChanged)
                                    .failures(totFailures)
                                    .ok(totOk)
                                    .unreachable(totUnreachable)
                                    .skipped(totSkipped);

                                Set<String> allHosts = new HashSet<>();
                                addHostKeys(allHosts, changedNode);
                                addHostKeys(allHosts, failuresNode);
                                addHostKeys(allHosts, okNode);
                                addHostKeys(allHosts, unreachableNode);
                                addHostKeys(allHosts, skippedNode);

                                for (String h : allHosts) {
                                    Map<String, Object> hStat = new HashMap<>();
                                    hStat.put("ok", okNode.path(h).asInt(0));
                                    hStat.put("changed", changedNode.path(h).asInt(0));
                                    hStat.put("failures", failuresNode.path(h).asInt(0));
                                    hStat.put("unreachable", unreachableNode.path(h).asInt(0));
                                    hStat.put("skipped", skippedNode.path(h).asInt(0));
                                    if (stats.size() < 1000) stats.put(h, hStat);

                                    if ((failuresNode.path(h).asInt(0) > 0 || unreachableNode.path(h).asInt(0) > 0)) {
                                        if (failedHostsSet.size() < 1000) failedHostsSet.add(h);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            runContext.logger().warn("Failed to parse job event json: {}", e.getMessage());
                        }
                    }
                } catch (IOException ignored) {}
            }
        }

        return new ExecutionTelemetry(status, rc, new ArrayList<>(failedHostsSet), stats, summaryBuilder.build(), failedTasks);
    }


    private void validateCmdlineArg(String arg) {
        if (arg == null || arg.indexOf('\0') != -1 || WHITESPACE_PATTERN.matcher(arg).matches() || arg.startsWith("-")) {
            throw new IllegalArgumentException("Invalid cmdline argument: '" + arg + "'. Cannot contain whitespace, NUL bytes, or start with '-'.");
        }
    }

    private Path secureResolve(Path base, String userPath) {
        Path resolved = base.resolve(userPath).normalize();
        if (!resolved.startsWith(base.normalize())) {
            throw new IllegalArgumentException("Path traversal blocked: " + userPath + " escapes the base directory.");
        }
        return resolved;
    }

    private int sumJsonNode(JsonNode node) {
        int sum = 0;
        if (node != null && node.isObject()) {
            Iterator<JsonNode> it = node.elements();
            while (it.hasNext()) {
                sum += it.next().asInt(0);
            }
        }
        return sum;
    }

    private void addHostKeys(Set<String> set, JsonNode node) {
        if (node != null && node.isObject()) {
            Iterator<String> it = node.fieldNames();
            while (it.hasNext()) {
                set.add(it.next());
            }
        }
    }

    private record ExecutionTelemetry(
        String status,
        int rc,
        List<String> failedHosts,
        Map<String, Object> stats,
        RunnerSummary summary,
        List<Map<String, Object>> failedTasks
    ) {}

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Structured JSON telemetry report",
            description = "A compact JSON file containing task and host execution summaries. Note: this excludes individual loop items to prevent massive file sizes."
        )
        private URI resultsUri;

        @Schema(
            title = "Internal Ansible job events zip archive",
            description = "A ZIP file of the raw /runner/artifacts/ folder containing all uncompressed Ansible runner internal JSON events (including every loop iteration)."
        )
        private URI artifactsUri;

        @Schema(
            title = "Complete raw terminal output log",
            description = "The complete, raw text log of the playbook execution. This contains 100% of the log lines, even if UI streaming was capped."
        )
        private URI outputLogFile;

        @Schema(
            title = "Extracted custom output files",
            description = "Specific files generated by your playbook (e.g. report.csv) that were captured by the task's outputFiles property."
        )
        private Map<String, URI> outputFiles;

        @Schema(title = "Final execution status (successful, failed, timeout)")
        private String status;

        @Schema(title = "Return code of the runner process")
        private Integer rc;

        @Schema(title = "Total failed task count")
        private Integer failures;

        @Schema(title = "Total changed task count")
        private Integer changed;

        @Schema(title = "List of hostnames that experienced task failures")
        private List<String> failedHosts;

        @Schema(title = "Host-by-host statistics map")
        private Map<String, Object> stats;

        @Schema(title = "Structured summary metrics")
        private RunnerSummary summary;
    }
}
