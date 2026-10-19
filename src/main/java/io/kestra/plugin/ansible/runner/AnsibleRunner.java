package io.kestra.plugin.ansible.runner;

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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
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
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

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

    @Schema(
        title = "Project playbook configuration",
        anyOf = { Project.class, String.class }
    )
    @Builder.Default
    private Project project = Project.builder()
        .playbook(Property.ofValue(DEFAULT_PLAYBOOK))
        .build();

    @Schema(
        title = "Inventory configuration or file path",
        anyOf = { Inventory.class, String.class }
    )
    private Inventory inventory;

    @Schema(title = "Environment and credentials configuration")
    private RunnerEnv env;

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
    public void kill() {
        if (this.taskRunner != null) {
            this.taskRunner.kill();
        }
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        var workingDir = runContext.workingDir().path();
        AnsibleRunnerLogConsumer logConsumer = null;
        var logSpoolFile = Files.createTempFile("ansible-runner-log-tmp-", ".log");
        try {
            var runnerDir = workingDir.resolve("runner");
            var projectDir = runnerDir.resolve("project");
            var inventoryDir = runnerDir.resolve("inventory");
            var envDir = runnerDir.resolve("env");
            var artifactsBaseDir = runnerDir.resolve("artifacts");

            Files.createDirectories(projectDir);
            Files.createDirectories(inventoryDir);
            Files.createDirectories(envDir);
            Files.createDirectories(artifactsBaseDir);

            // Pre-load namespace files directly into the working directory if configured.
            // Loaded once here so playbooks, inventories, and roles are available in workingDir before resolution;
            // not passed to CommandsWrapper to prevent duplicate loading.
            if (this.namespaceFiles != null && !Boolean.FALSE.equals(runContext.render(this.namespaceFiles.getEnabled()).as(Boolean.class).orElse(true))) {
                NamespaceFilesUtils.loadNamespaceFiles(runContext, this.namespaceFiles);
            }

            var resolvedPlaybook = resolvePlaybook(runContext);
            resolveProjectSource(runContext, workingDir, projectDir, resolvedPlaybook);
            resolveInventory(runContext, workingDir, inventoryDir);

            var rPasswords = renderPasswords(runContext);
            populateEnvDir(runContext, envDir, rPasswords);

            var ident = IdUtils.create();
            var rImage = runContext.render(this.containerImage).as(String.class).orElse(DEFAULT_CONTAINER_IMAGE);

            // Playbook-level failures (rc=2 failed hosts, rc=4 unreachable) must not make the task runner
            // throw before telemetry is parsed. If ansible-runner produced an rc artifact, exit 0 and let
            // failOnErrors decide; otherwise (runner never started) propagate the real exit code.
            var runnerCommand = List.of(
                "ansible-runner run ./runner -p '" + resolvedPlaybook.replace("'", "'\\''") + "' --ident " + ident
                    + "; __rc=$?; if [ -f ./runner/artifacts/" + ident + "/rc ]; then if [ $__rc -eq 2 ] || [ $__rc -eq 4 ]; then exit 0; fi; fi; exit $__rc"
            );

            var rStreamLogs = runContext.render(this.streamLogs).as(Boolean.class).orElse(true);
            var rLogsMode = runContext.render(this.logsMode).as(LogsMode.class).orElse(LogsMode.FULL);
            var rOutputLogFile = runContext.render(this.outputLogFile).as(Boolean.class).orElse(false);
            var rMaxLogLines = runContext.render(this.maxLogLines).as(Long.class).orElse(DEFAULT_MAX_LOG_LINES);

            // Full raw output is always spooled to disk (never kept in memory) so it can be uploaded without loss.
            // It lives in the system temp directory outside the container working directory to prevent Docker volume mount collisions.
            try {
                Files.setPosixFilePermissions(logSpoolFile, PosixFilePermissions.fromString("rw-------"));
            } catch (UnsupportedOperationException ignored) {
                logSpoolFile.toFile().setReadable(true, true);
                logSpoolFile.toFile().setWritable(true, true);
            }

            var secretMasker = new SecretMasker();
            if (this.env != null) {
                if (this.env.getSshKey() != null) {
                    var rSshKey = runContext.render(this.env.getSshKey()).as(String.class).orElse(null);
                    if (rSshKey != null && !rSshKey.isBlank()) {
                        secretMasker.add(rSshKey);
                    }
                }
                if (rPasswords != null && !rPasswords.isEmpty()) {
                    for (var val : rPasswords.values()) {
                        if (val != null && !val.isBlank()) {
                            secretMasker.add(val);
                        }
                    }
                }
                if (this.env.getExtraVars() != null) {
                    var rExtraVars = runContext.render(this.env.getExtraVars()).asMap(String.class, Object.class);
                    secretMasker.addSensitiveValues(rExtraVars);
                }
                if (this.env.getEnvVars() != null) {
                    var rEnvVars = runContext.render(this.env.getEnvVars()).asMap(String.class, String.class);
                    secretMasker.addSensitiveValues(rEnvVars);
                }
            }

            logConsumer = new AnsibleRunnerLogConsumer(
                runContext,
                rStreamLogs,
                rLogsMode,
                logSpoolFile,
                rMaxLogLines,
                LOG_BATCH_SIZE,
                secretMasker
            );

            var commandsWrapper = new CommandsWrapper(runContext)
                .withLogConsumer(logConsumer)
                .withTaskRunner(this.taskRunner)
                .withContainerImage(rImage)
                .withInterpreter(Property.ofValue(List.of("/bin/bash", "-c")))
                .withCommands(Property.ofValue(runnerCommand));

            var preCommands = buildPreCommands(runContext, workingDir);
            if (!preCommands.isEmpty()) {
                commandsWrapper = commandsWrapper.withBeforeCommands(Property.ofValue(preCommands));
            }

            if (this.inputFiles != null) {
                commandsWrapper = commandsWrapper.withInputFiles(this.inputFiles);
            }

            var outputPatterns = new ArrayList<String>();
            if (this.outputFiles != null) {
                var rOutputFiles = runContext.render(this.outputFiles).asList(String.class);
                for (var pattern : rOutputFiles) {
                    outputPatterns.add(pattern);
                    if (!pattern.startsWith("/") && !pattern.startsWith("**/") && !pattern.startsWith("runner/")) {
                        outputPatterns.add("runner/project/" + pattern);
                        outputPatterns.add("runner/" + pattern);
                    }
                }
            }
            outputPatterns.add("runner/artifacts/" + ident + "/**");
            commandsWrapper = commandsWrapper.withOutputFiles(outputPatterns);

            var scriptOutput = commandsWrapper.run();
            var processExitCode = scriptOutput.getExitCode();
            var extractedOutputFiles = scriptOutput.getOutputFiles();

            var identArtifactsDir = artifactsBaseDir.resolve(ident);
            if (extractedOutputFiles != null) {
                var prefix = "runner/artifacts/" + ident + "/";
                for (var entry : extractedOutputFiles.entrySet()) {
                    if (entry.getKey().startsWith(prefix)) {
                        var target = workingDir.resolve(entry.getKey());
                        if (!Files.exists(target)) {
                            Files.createDirectories(target.getParent());
                            try (var is = runContext.storage().getFile(entry.getValue())) {
                                Files.copy(is, target, StandardCopyOption.REPLACE_EXISTING);
                            }
                        }
                    }
                }
            }

            var telemetry = parseTelemetry(runContext, identArtifactsDir, processExitCode, secretMasker);

            if (Files.exists(identArtifactsDir)) {
                secretMasker.maskTree(identArtifactsDir, runContext.logger());
            }

            URI resultsUri = null;
            var rSaveResults = runContext.render(this.saveResults).as(Boolean.class).orElse(true);
            if (rSaveResults && Files.exists(identArtifactsDir)) {
                var consolidatedResults = new LinkedHashMap<String, Object>();
                consolidatedResults.put("status", telemetry.status);
                consolidatedResults.put("rc", telemetry.rc);
                consolidatedResults.put("summary", telemetry.summary);
                consolidatedResults.put("stats", telemetry.stats);
                consolidatedResults.put("failedHosts", telemetry.failedHosts);
                consolidatedResults.put("failedTasks", telemetry.failedTasks);

                var resultsFile = workingDir.resolve("results-" + ident + ".json");
                Files.writeString(
                    resultsFile,
                    JacksonMapper.ofJson().writerWithDefaultPrettyPrinter().writeValueAsString(consolidatedResults),
                    StandardCharsets.UTF_8
                );
                resultsUri = runContext.storage().putFile(resultsFile.toFile());
                Files.deleteIfExists(resultsFile);
            }

            var zipFile = workingDir.resolve("artifacts-" + ident + ".zip");
            URI artifactsUri = null;
            var rSaveArtifacts = runContext.render(this.saveArtifacts).as(Boolean.class).orElse(true);
            if (rSaveArtifacts && Files.exists(identArtifactsDir)) {
                ArchiveUtils.zipDirectory(identArtifactsDir, zipFile);
                artifactsUri = runContext.storage().putFile(zipFile.toFile());
            }

            if (logConsumer != null) {
                logConsumer.close();
            }
            URI logFileUri = null;
            if ((rOutputLogFile || logConsumer.wasTruncated()) && Files.exists(logSpoolFile)) {
                logFileUri = runContext.storage().putFile(logSpoolFile.toFile());
            }
            Files.deleteIfExists(logSpoolFile);

            var rAutoCleanArtifacts = runContext.render(this.autoCleanArtifacts).as(Boolean.class).orElse(true);
            if (rAutoCleanArtifacts) {
                FileUtils.deleteQuietly(artifactsBaseDir.toFile());
                Files.deleteIfExists(zipFile);
            }

            Map<String, URI> finalOutputFiles = null;
            if (this.outputFiles != null && extractedOutputFiles != null && !extractedOutputFiles.isEmpty()) {
                finalOutputFiles = new HashMap<>();
                for (var entry : extractedOutputFiles.entrySet()) {
                    if (!entry.getKey().startsWith("runner/artifacts/")) {
                        finalOutputFiles.put(entry.getKey(), entry.getValue());
                    }
                }
                if (finalOutputFiles.isEmpty()) {
                    finalOutputFiles = null;
                }
            }

            var rOutputSummary = runContext.render(this.outputSummary).as(Boolean.class).orElse(false);

            var output = Output.builder()
                .resultsUri(resultsUri)
                .artifactsUri(artifactsUri)
                .outputLogFile(logFileUri)
                .outputFiles(finalOutputFiles)
                .status(telemetry.status)
                .rc(telemetry.rc)
                .failures(telemetry.summary.getFailures())
                .changed(rOutputSummary ? telemetry.summary.getChanged() : null)
                .failedHosts(telemetry.failedHosts)
                .stats(telemetry.stats)
                .summary(rOutputSummary ? telemetry.summary : null)
                .build();

            // Check output size against maxOutputsSize after storing results; truncate stats and failedHosts to preserve URIs
            var rMaxOutputsSize = runContext.render(this.maxOutputsSize).as(Long.class).orElse(DEFAULT_MAX_OUTPUTS_SIZE);
            var outputBytes = JacksonMapper.ofJson().writeValueAsBytes(output);
            if (outputBytes.length > rMaxOutputsSize) {
                runContext.logger().warn(
                    "Ansible task output ({} bytes) exceeds configured maxOutputsSize of {} bytes. " +
                        "Truncating inline failedHosts and stats to fit limit and preserve resultsUri, artifactsUri, and outputLogFile. " +
                        "Complete details remain stored in {}",
                    outputBytes.length, rMaxOutputsSize, resultsUri
                );
                output = output.toBuilder()
                    .failedHosts(List.of())
                    .stats(Map.of())
                    .build();
            }

            var rFailOnErrors = runContext.render(this.failOnErrors).as(Boolean.class).orElse(true);
            if (rFailOnErrors && !"successful".equalsIgnoreCase(telemetry.status)) {
                runContext.logger().error("Ansible Runner failed. Results: {}, Artifacts: {}, Logs: {}", resultsUri, artifactsUri, logFileUri);
                throw new IllegalStateException(
                    "Ansible Runner failed with status '" + telemetry.status + "' and exit code " + telemetry.rc +
                        " on hosts: " + telemetry.failedHosts
                );
            }

            return output;
        } catch (Exception e) {
            try {
                Files.deleteIfExists(logSpoolFile);
            } catch (Exception ignored) {
            }
            throw e;
        } finally {
            if (logConsumer != null)
                logConsumer.close();
            FileUtils.deleteQuietly(workingDir.resolve("runner/env").toFile());
        }
    }

    private List<String> buildPreCommands(RunContext runContext, Path workingDir) throws IllegalVariableEvaluationException {
        List<String> cmds = new ArrayList<>();

        if (this.beforeCommands != null) {
            cmds.addAll(runContext.render(this.beforeCommands).asList(String.class));
        }

        var autoGalaxy = runContext.render(this.autoInstallGalaxyRequirements).as(Boolean.class).orElse(true);
        var galaxyDeps = runContext.render(this.galaxyDependencies).asList(String.class);
        if (autoGalaxy) {
            cmds.add(
                "if [ -f ./runner/project/requirements.yml ]; then ansible-galaxy install -r ./runner/project/requirements.yml; elif [ -f requirements.yml ]; then ansible-galaxy install -r requirements.yml; fi"
            );
        }
        if (!galaxyDeps.isEmpty()) {
            for (String dep : galaxyDeps) {
                if (!PKG_ALLOWLIST.matcher(dep).matches()) {
                    throw new IllegalArgumentException("Invalid galaxy dependency: " + dep + ". Must match " + PKG_ALLOWLIST.pattern());
                }
            }
            var quotedGalaxyDeps = galaxyDeps.stream().map(d -> "'" + d.replace("'", "'\\''") + "'").toList();
            cmds.add("ansible-galaxy collection install " + String.join(" ", quotedGalaxyDeps));
        }

        var autoPython = runContext.render(this.autoInstallPythonRequirements).as(Boolean.class).orElse(true);
        var pythonDeps = runContext.render(this.pythonDependencies).asList(String.class);
        if (autoPython) {
            cmds.add(
                "if [ -f ./runner/project/requirements.txt ]; then pip install --no-cache-dir -r ./runner/project/requirements.txt; elif [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi"
            );
        }
        if (!pythonDeps.isEmpty()) {
            for (String dep : pythonDeps) {
                if (!PKG_ALLOWLIST.matcher(dep).matches()) {
                    throw new IllegalArgumentException("Invalid python dependency: " + dep + ". Must match " + PKG_ALLOWLIST.pattern());
                }
            }
            var quotedPythonDeps = pythonDeps.stream().map(d -> "'" + d.replace("'", "'\\''") + "'").toList();
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

            if (sourceStr.startsWith("kestra://")) {
                var sourceUri = URI.create(sourceStr);
                try (InputStream is = runContext.storage().getFile(sourceUri)) {
                    if (sourceStr.endsWith(".zip")) {
                        ArchiveUtils.unzip(is, projectDir);
                    } else if (sourceStr.endsWith(".tar.gz") || sourceStr.endsWith(".tgz")) {
                        ArchiveUtils.untarGz(is, projectDir);
                    } else {
                        Files.copy(is, secureResolve(projectDir, playbookName), StandardCopyOption.REPLACE_EXISTING);
                    }
                }
            } else if (sourceStr.contains("://")) {
                throw new IllegalArgumentException(
                    "Unsupported URI scheme for project source: " + sourceStr + ". Only kestra:// internal storage URIs or local workspace paths are supported."
                );
            } else {
                var relSource = sourceStr.startsWith("/") ? sourceStr.substring(1) : sourceStr;
                var localSource = secureResolve(workingDir, relSource);
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
                    throw new IllegalArgumentException("Project source not found: " + sourceStr);
                }
            }
        }

        var targetPlaybook = secureResolve(projectDir, playbookName);
        if (!Files.exists(targetPlaybook)) {
            var directWorkingDirFile = workingDir.resolve(playbookName);
            var playbooksSubDirFile = workingDir.resolve("playbooks").resolve(playbookName);
            if (Files.exists(directWorkingDirFile)) {
                Files.copy(directWorkingDirFile, targetPlaybook, StandardCopyOption.REPLACE_EXISTING);
            } else if (Files.exists(playbooksSubDirFile)) {
                Files.copy(playbooksSubDirFile, targetPlaybook, StandardCopyOption.REPLACE_EXISTING);
            } else {
                try (Stream<Path> stream = Files.walk(workingDir, 4)) {
                    var found = stream
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

        var playbooksDir = workingDir.resolve("playbooks");
        if (Files.isDirectory(playbooksDir)) {
            try {
                FileUtils.copyDirectory(playbooksDir.toFile(), projectDir.toFile());
            } catch (Exception e) {
                runContext.logger().warn("Failed to copy playbooks dir: {}", e.getMessage());
            }
        }

        var standardDirs = List.of(
            "roles", "templates", "scripts", "group_vars", "host_vars",
            "library", "lookup_plugins", "filter_plugins", "callback_plugins",
            "module_utils", "collections"
        );
        for (String dirName : standardDirs) {
            var srcDir = workingDir.resolve(dirName);
            var destDir = projectDir.resolve(dirName);
            if (Files.isDirectory(srcDir) && !Files.exists(destDir)) {
                FileUtils.copyDirectory(srcDir.toFile(), destDir.toFile());
            }
        }

        var ansibleCfg = workingDir.resolve("ansible.cfg");
        if (Files.exists(ansibleCfg) && !Files.exists(projectDir.resolve("ansible.cfg"))) {
            Files.copy(ansibleCfg, projectDir.resolve("ansible.cfg"), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private void writePlaybookContent(Path projectDir, String playbookName, String content) throws IOException {
        var sanitized = content.trim();
        if (sanitized.startsWith("{") && sanitized.endsWith("}")) {
            try {
                Map<String, Object> map = JacksonMapper.ofJson().readValue(
                    sanitized,
                    new TypeReference<Map<String, Object>>() {
                    }
                );
                if (!map.isEmpty() && map.keySet().stream().anyMatch(k -> k.endsWith(".yml") || k.endsWith(".yaml") || k.contains("/"))) {
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        var target = secureResolve(projectDir, entry.getKey());
                        if (target.getParent() != null) {
                            Files.createDirectories(target.getParent());
                        }
                        var fileVal = entry.getValue() != null ? entry.getValue().toString() : "";
                        writePlaybookContent(target.getParent() != null ? target.getParent() : projectDir, target.getFileName().toString(), fileVal);
                    }
                    return;
                }
            } catch (JsonProcessingException ignored) {
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

        if (this.inventory != null && this.inventory.getHosts() != null) {
            var rHosts = runContext.render(this.inventory.getHosts()).asMap(String.class, Object.class);
            if (!rHosts.isEmpty()) {
                var jsonInventory = new HashMap<String, Object>();
                var allGroup = new HashMap<String, Object>();
                allGroup.put("hosts", new ArrayList<>(rHosts.keySet()));
                jsonInventory.put("all", allGroup);
                var meta = new HashMap<String, Object>();
                meta.put("hostvars", rHosts);
                jsonInventory.put("_meta", meta);
                Files.writeString(inventoryDir.resolve("hosts.json"), JacksonMapper.ofJson().writeValueAsString(jsonInventory), StandardCharsets.UTF_8);
                return;
            }
        }

        if (this.inventory != null && this.inventory.getGroups() != null) {
            var rGroups = runContext.render(this.inventory.getGroups()).asMap(String.class, Object.class);
            if (!rGroups.isEmpty()) {
                Files.writeString(inventoryDir.resolve("hosts.json"), JacksonMapper.ofJson().writeValueAsString(rGroups), StandardCharsets.UTF_8);
                return;
            }
        }

        if (this.inventory != null && this.inventory.getScript() != null) {
            var scriptStr = runContext.render(this.inventory.getScript()).as(String.class).orElse(null);
            if (scriptStr != null && !scriptStr.isBlank()) {
                var scriptTarget = inventoryDir.resolve("hosts");
                var localScript = secureResolve(workingDir, scriptStr.startsWith("/") ? scriptStr.substring(1) : scriptStr);
                if (Files.isRegularFile(localScript)) {
                    Files.copy(localScript, scriptTarget, StandardCopyOption.REPLACE_EXISTING);
                } else {
                    Files.writeString(scriptTarget, scriptStr.trim() + "\n", StandardCharsets.UTF_8);
                }
                makeExecutable(scriptTarget);
                return;
            }
        }

        if (this.inventory != null && this.inventory.getSources() != null) {
            var rSources = runContext.render(this.inventory.getSources()).asList(String.class);
            if (!rSources.isEmpty()) {
                for (var src : rSources) {
                    if (src.startsWith("kestra://")) {
                        var uri = URI.create(src);
                        var path = uri.getPath();
                        var fileName = path != null ? Paths.get(path).getFileName().toString() : "inventory";
                        var target = secureResolve(inventoryDir, fileName);
                        try (var is = runContext.storage().getFile(uri)) {
                            Files.copy(is, target, StandardCopyOption.REPLACE_EXISTING);
                        }
                    } else if (src.contains("://")) {
                        throw new IllegalArgumentException(
                            "Unsupported URI scheme for inventory source: " + src + ". Only kestra:// internal storage URIs or local workspace paths are supported."
                        );
                    } else {
                        var localSrc = secureResolve(workingDir, src.startsWith("/") ? src.substring(1) : src);
                        if (Files.isDirectory(localSrc)) {
                            FileUtils.copyDirectory(localSrc.toFile(), inventoryDir.toFile());
                        } else if (Files.isRegularFile(localSrc)) {
                            Files.copy(localSrc, secureResolve(inventoryDir, localSrc.getFileName().toString()), StandardCopyOption.REPLACE_EXISTING);
                        } else {
                            throw new IllegalArgumentException("Inventory source not found: " + src);
                        }
                    }
                }
                return;
            }
        }

        String invFile = null;
        if (this.inventory != null && this.inventory.getFile() != null) {
            invFile = runContext.render(this.inventory.getFile()).as(String.class).orElse(null);
        }

        if (invFile != null && !invFile.isBlank()) {
            if (invFile.startsWith("kestra://")) {
                try (var is = runContext.storage().getFile(URI.create(invFile))) {
                    Files.copy(is, inventoryDir.resolve("hosts"), StandardCopyOption.REPLACE_EXISTING);
                }
            } else if (invFile.contains("://")) {
                throw new IllegalArgumentException("Unsupported URI scheme for inventory file: " + invFile + ". Only kestra:// internal storage URIs or local workspace paths are supported.");
            } else {
                var relInv = invFile.startsWith("/") ? invFile.substring(1) : invFile;
                var localInv = secureResolve(workingDir, relInv);
                var localSubInv = secureResolve(workingDir.resolve("inventory"), relInv);
                var localPluralInv = secureResolve(workingDir.resolve("inventories"), relInv);

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
                    throw new IllegalArgumentException("Inventory file not found: " + invFile);
                }
            }
            return;
        }

        var inventoryFolder = workingDir.resolve("inventory");
        if (Files.isDirectory(inventoryFolder)) {
            try {
                FileUtils.copyDirectory(inventoryFolder.toFile(), inventoryDir.toFile());
                try (Stream<Path> invStream = Files.list(inventoryDir)) {
                    if (invStream.findAny().isPresent()) {
                        runContext.logger().info("Using auto-detected inventory directory: {}", inventoryFolder);
                        return;
                    }
                }
            } catch (Exception e) {
                runContext.logger().warn("Failed to auto-detect inventory directory: {}", e.getMessage());
            }
        }
        var inventoriesFolder = workingDir.resolve("inventories");
        if (Files.isDirectory(inventoriesFolder)) {
            try {
                FileUtils.copyDirectory(inventoriesFolder.toFile(), inventoryDir.toFile());
                try (Stream<Path> invStream = Files.list(inventoryDir)) {
                    if (invStream.findAny().isPresent()) {
                        runContext.logger().info("Using auto-detected inventory directory: {}", inventoriesFolder);
                        return;
                    }
                }
            } catch (Exception e) {
                runContext.logger().warn("Failed to auto-detect inventory directory: {}", e.getMessage());
            }
        }

        var candidateInvs = List.of(
            "inventory/hosts.ini", "inventory/hosts", "inventory/hosts.yml", "inventory/hosts.yaml",
            "hosts.ini", "hosts", "hosts.yml", "hosts.yaml", "inventory.ini"
        );
        for (String cand : candidateInvs) {
            var candPath = secureResolve(workingDir, cand);
            if (Files.isRegularFile(candPath)) {
                Files.copy(candPath, inventoryDir.resolve("hosts"), StandardCopyOption.REPLACE_EXISTING);
                return;
            }
        }

    }

    private void makeExecutable(Path path) {
        try {
            var perms = Files.getPosixFilePermissions(path);
            perms.add(PosixFilePermission.OWNER_EXECUTE);
            perms.add(PosixFilePermission.GROUP_EXECUTE);
            perms.add(PosixFilePermission.OTHERS_EXECUTE);
            Files.setPosixFilePermissions(path, perms);
        } catch (UnsupportedOperationException e) {
            path.toFile().setExecutable(true, false);
        } catch (IOException ignored) {
        }
    }

    private Map<String, String> renderPasswords(RunContext runContext) throws IllegalVariableEvaluationException {
        if (this.env == null || this.env.getPasswords() == null) {
            return Map.of();
        }
        Map<String, String> rawPasswords = runContext.render(this.env.getPasswords()).asMap(String.class, String.class);
        if (rawPasswords == null || rawPasswords.isEmpty()) {
            return Map.of();
        }
        return Map.copyOf(rawPasswords);
    }

    private void populateEnvDir(RunContext runContext, Path envDir, Map<String, String> rPasswords) throws Exception {
        var cmdArgs = new ArrayList<String>();
        if (Boolean.TRUE.equals(runContext.render(this.checkMode).as(Boolean.class).orElse(false))) {
            cmdArgs.add("--check");
        }
        if (Boolean.TRUE.equals(runContext.render(this.diff).as(Boolean.class).orElse(false))) {
            cmdArgs.add("--diff");
        }
        var rLimit = runContext.render(this.limit).as(String.class).orElse(null);
        if (rLimit != null && !rLimit.isBlank()) {
            validateCmdlineArg(rLimit);
            cmdArgs.add("--limit");
            cmdArgs.add(rLimit);
        }
        var rTags = runContext.render(this.tags).asList(String.class);
        if (!rTags.isEmpty()) {
            for (var t : rTags)
                validateCmdlineArg(t);
            cmdArgs.add("--tags");
            cmdArgs.add(String.join(",", rTags));
        }
        var rSkipTags = runContext.render(this.skipTags).asList(String.class);
        if (!rSkipTags.isEmpty()) {
            for (var t : rSkipTags)
                validateCmdlineArg(t);
            cmdArgs.add("--skip-tags");
            cmdArgs.add(String.join(",", rSkipTags));
        }
        var rForks = runContext.render(this.forks).as(Integer.class).orElse(5);
        if (rForks != null) {
            if (rForks < 1) {
                throw new IllegalArgumentException("forks must be greater than or equal to 1");
            }
            cmdArgs.add("-f");
            cmdArgs.add(String.valueOf(rForks));
        }
        var rVerbosity = runContext.render(this.verbosity).as(Integer.class).orElse(0);
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

        if (this.env != null && this.env.getExtraVars() != null) {
            var rawExtraVars = runContext.render(this.env.getExtraVars()).asMap(String.class, Object.class);
            if (rawExtraVars != null && !rawExtraVars.isEmpty()) {
                Files.writeString(envDir.resolve("extravars"), JacksonMapper.ofJson().writeValueAsString(rawExtraVars), StandardCharsets.UTF_8);
            }
        }

        if (this.env != null && this.env.getEnvVars() != null) {
            var rawEnvVars = runContext.render(this.env.getEnvVars()).asMap(String.class, String.class);
            if (rawEnvVars != null && !rawEnvVars.isEmpty()) {
                Files.writeString(envDir.resolve("envvars"), JacksonMapper.ofJson().writeValueAsString(rawEnvVars), StandardCharsets.UTF_8);
            }
        }

        if (rPasswords != null && !rPasswords.isEmpty()) {
            Files.writeString(envDir.resolve("passwords"), JacksonMapper.ofJson().writeValueAsString(rPasswords), StandardCharsets.UTF_8);
        }

        // Written with secure 0600 POSIX permissions to protect private key
        String rawKey = null;
        if (this.env != null && this.env.getSshKey() != null) {
            rawKey = runContext.render(this.env.getSshKey()).as(String.class).orElse(null);
        }

        if (rawKey != null && !rawKey.isBlank()) {
            var sshKeyPath = envDir.resolve("ssh_key");
            Files.writeString(sshKeyPath, rawKey.trim() + "\n", StandardCharsets.UTF_8);
            try {
                Files.setPosixFilePermissions(sshKeyPath, PosixFilePermissions.fromString("rw-------"));
            } catch (UnsupportedOperationException ignored) {
                sshKeyPath.toFile().setReadable(true, true);
                sshKeyPath.toFile().setWritable(true, true);
                sshKeyPath.toFile().setExecutable(false, false);
            }
        }

        var settings = new HashMap<String, Object>();
        if (this.timeout != null) {
            var dur = runContext.render(this.timeout).as(Duration.class).orElse(null);
            if (dur != null) {
                settings.put("job_timeout", dur.toSeconds());
            }
        }
        if (this.idleTimeout != null) {
            var dur = runContext.render(this.idleTimeout).as(Duration.class).orElse(null);
            if (dur != null) {
                settings.put("idle_timeout", dur.toSeconds());
            }
        }
        if (!settings.isEmpty()) {
            Files.writeString(envDir.resolve("settings"), JacksonMapper.ofJson().writeValueAsString(settings), StandardCharsets.UTF_8);
        }
    }

    private ExecutionTelemetry parseTelemetry(RunContext runContext, Path identArtifactsDir, int processExitCode, SecretMasker secretMasker) {
        String status = "failed";
        int rc = processExitCode;
        Set<String> failedHostsSet = new LinkedHashSet<>();
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
            var statusFile = identArtifactsDir.resolve("status");
            if (Files.exists(statusFile)) {
                try {
                    status = Files.readString(statusFile, StandardCharsets.UTF_8).trim();
                } catch (IOException e) {
                    runContext.logger().warn("Failed to read status file: {}", e.getMessage());
                }
            }

            var rcFile = identArtifactsDir.resolve("rc");
            if (Files.exists(rcFile)) {
                try {
                    rc = Integer.parseInt(Files.readString(rcFile, StandardCharsets.UTF_8).trim());
                } catch (Exception e) {
                    runContext.logger().warn("Failed to parse rc file: {}", e.getMessage());
                }
            }

            var jobEventsDir = identArtifactsDir.resolve("job_events");
            if (Files.isDirectory(jobEventsDir)) {
                try (Stream<Path> stream = Files.list(jobEventsDir)) {
                    var eventFiles = stream
                        .filter(p -> p.toString().endsWith(".json"))
                        .sorted(Comparator.comparing(Path::getFileName))
                        .toList();

                    for (Path eventFile : eventFiles) {
                        try {
                            JsonNode root = JacksonMapper.ofJson().readTree(eventFile.toFile());
                            var event = root.path("event").asText("");
                            JsonNode eventData = root.path("event_data");

                            var host = eventData.path("remote_addr").asText(eventData.path("host").asText(""));
                            var taskName = eventData.path("task").asText("");
                            var playName = eventData.path("play").asText("");

                            if ("runner_on_failed".equals(event) || "runner_on_unreachable".equals(event)) {
                                if (!host.isBlank()) {
                                    if (failedHostsSet.size() < 1000)
                                        failedHostsSet.add(host);
                                }
                                Map<String, Object> failedTask = new LinkedHashMap<>();
                                failedTask.put("host", host);
                                failedTask.put("task", taskName);
                                failedTask.put("play", playName);
                                failedTask.put("event", event);
                                var msg = eventData.path("res").path("msg").asText("");
                                if (!msg.isBlank()) {
                                    if (secretMasker != null) {
                                        msg = secretMasker.mask(msg);
                                    }
                                    failedTask.put("error", msg);
                                }
                                if (failedTasks.size() < 1000)
                                    failedTasks.add(failedTask);
                            }

                            if ("playbook_on_stats".equals(event)) {
                                JsonNode changedNode = eventData.path("changed");
                                JsonNode failuresNode = eventData.path("failures");
                                JsonNode okNode = eventData.path("ok");
                                JsonNode unreachableNode = eventData.path("dark");
                                JsonNode skippedNode = eventData.path("skipped");

                                var totChanged = sumJsonNode(changedNode);
                                var totFailures = sumJsonNode(failuresNode);
                                var totOk = sumJsonNode(okNode);
                                var totUnreachable = sumJsonNode(unreachableNode);
                                var totSkipped = sumJsonNode(skippedNode);

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
                                    if (stats.size() < 1000)
                                        stats.put(h, hStat);

                                    if ((failuresNode.path(h).asInt(0) > 0 || unreachableNode.path(h).asInt(0) > 0)) {
                                        if (failedHostsSet.size() < 1000)
                                            failedHostsSet.add(h);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            runContext.logger().warn("Failed to parse job event json: {}", e.getMessage());
                        }
                    }
                } catch (IOException e) {
                    runContext.logger().warn("Failed to list job events directory: {}", e.getMessage());
                }
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
        var resolved = base.resolve(userPath).normalize();
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
        List<Map<String, Object>> failedTasks) {
    }

    @Builder(toBuilder = true)
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
