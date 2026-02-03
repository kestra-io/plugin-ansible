package io.kestra.plugin.ansible.cli;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.assets.AssetsDeclaration;
import io.kestra.core.models.assets.Custom;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.executions.TaskRunAttempt;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.*;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.models.tasks.runners.TaskRunnerDetailResult;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.WorkerTaskResult;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run Ansible CLI commands",
    description = "Executes ansible or ansible-playbook commands with the configured task runner. Generates ansible.cfg with the Kestra callback by default unless you supply one. Uses the cytopia/ansible:latest-tools image by default and merges outputs across multiple commands."
)
@Plugin(
    examples = {
        @Example(
            title = "Execute a list of Ansible CLI commands to orchestrate an Ansible playbook stored in the Editor using [Namespace Files](https://kestra.io/docs/developer-guide/namespace-files).",
            full = true,
            code = """
                id: ansible
                namespace: company.team

                tasks:
                  - id: ansible_task
                    type: io.kestra.plugin.ansible.cli.AnsibleCLI
                    inputFiles:
                      inventory.ini: "{{ read('inventory.ini') }}"
                      myplaybook.yml: "{{ read('myplaybook.yml') }}"
                    containerImage: cytopia/ansible:latest-tools
                    commands:
                      - ansible-playbook -i inventory.ini myplaybook.yml
                """
        ),
        @Example(
            title = "Execute a list of Ansible CLI commands to orchestrate an Ansible playbook defined inline in the flow definition.",
            full = true,
            code = """
                id: ansible
                namespace: company.team

                tasks:
                  - id: ansible_task
                    type: io.kestra.plugin.ansible.cli.AnsibleCLI
                    inputFiles:
                      inventory.ini: |
                        localhost ansible_connection=local
                      myplaybook.yml: |
                        ---
                        - hosts: localhost
                          tasks:
                            - name: Print Hello World
                              debug:
                                msg: "Hello, World!"
                    containerImage: cytopia/ansible:latest-tools
                    commands:
                      - ansible-playbook -i inventory.ini myplaybook.yml
                """
        ),
        @Example(
            title = "Execute an Ansible playbook and use ansible.builtin.debug command to extract outputs.",
            full = true,
            code = """
                id: ansible_playbook_outputs
                namespace: company.team

                tasks:
                  - id: ansible_playbook_outputs
                    type: io.kestra.plugin.ansible.cli.AnsibleCLI
                    outputLogFile: true
                    inputFiles:
                      playbook.yml: |
                        ---
                        - hosts: localhost
                          tasks:
                            - name: Create file
                              shell: echo "Test output" >> greeting.txt

                            - name: Register output file to var
                              shell: cat greeting.txt
                              register: myOutput

                            - name: Print return information from the previous task
                              ansible.builtin.debug:
                                var: myOutput

                            - name: Prints two lines of messages
                              ansible.builtin.debug:
                                msg:
                                  - "Multiline message : line 1"
                                  - "Multiline message : line 2"
                    containerImage: cytopia/ansible:latest-tools
                    commands:
                      - ansible-playbook -i localhost -c local playbook.yml
                """
        )
    }
)
public class AnsibleCLI extends Task implements RunnableTask<AnsibleCLI.AnsibleOutput>, NamespaceFilesInterface, InputFilesInterface, OutputFilesInterface {
    private static final String DEFAULT_IMAGE = "cytopia/ansible:latest-tools";
    public static final String ANSIBLE_CFG = "ansible.cfg";
    public static final String PLUGINS_KESTRA_LOGGER_PY = "callback_plugins/kestra_logger.py";
    private static final String INVENTORY_FILE = "inventory.ini";
    private static final String DEFAULT_INVENTORY_GROUP = "ungrouped";
    private static final String ASSET_PROVIDER = "ansible_inventory";
    private static final String ASSET_REGION = "unknown";
    private static final String ASSET_STATE = "targeted";
    private static final String TABLE_ASSET_TYPE = "io.kestra.plugin.ee.assets.VM";

    @Schema(
        title = "Run once before commands",
        description = "Optional shell commands executed only before the first main command, rendered with the same variables."
    )
    protected Property<List<String>> beforeCommands;

    @Schema(
        title = "Commands to run sequentially",
        description = "Commands are executed one by one in the same working directory and their outputs are merged. Group related steps in a single task to optimize performance."
    )
    @NotNull
    protected Property<List<String>> commands;

    @Schema(
        title = "Additional environment variables",
        description = "Variables injected into the task runner environment for every command."
    )
    protected Property<Map<String, String>> env;

    @Schema(
        title = "Deprecated Docker options",
        description = "Use taskRunner instead; kept for backward compatibility."
    )
    @PluginProperty
    @Deprecated
    private DockerOptions docker;

    @Schema(
        title = "Task runner",
        description = "Runner implementation to execute the commands; defaults to Docker. Provide runner-specific properties as needed."
    )
    @PluginProperty
    @Builder.Default
    @Valid
    protected TaskRunner<?> taskRunner = Docker.instance();

    @Schema(
        title = "Task runner container image",
        description = "Used only by container-based runners; defaults to cytopia/ansible:latest-tools. Supply a lean image with required modules to speed execution. Non-container runners won't include Ansible dependencies, so rely on this image (or provide your own) when you need them."
    )
    @Builder.Default
    protected Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    @Schema(
        title = "Ansible configuration",
        description = """
            If omitted, a generated ansible.cfg in the working directory enables the Kestra callback plugin and logs to `log`.
            Provide custom content to override defaults; include the callback settings above if you still want structured outputs.
            """
    )
    @Builder.Default
    protected Property<String> ansibleConfig = Property.ofExpression("""
        [defaults]
        log_path          = {{ workingDir }}/log
        callback_plugins  = ./callback_plugins
        callbacks_enabled = kestra_logger
        stdout_callback   = ansible.builtin.null
        result_format     = json
        pretty_results    = true
        """);

    @Schema(
        title = "Publish Ansible log file",
        description = "If true, uploads the ansible log as output file `log`; multi-command runs concatenate per-command logs. Default is false."
    )
    @Builder.Default
    private Property<Boolean> outputLogFile = Property.ofValue(false);

    private NamespaceFiles namespaceFiles;

    private Object inputFiles;

    private Property<List<String>> outputFiles;

    @Override
    public AnsibleOutput run(RunContext runContext) throws Exception {
        List<String> outputFilesList = new ArrayList<>(runContext.render(this.outputFiles).asList(String.class));

        boolean wantLogFile = runContext.render(this.outputLogFile).as(Boolean.class).orElse(false);
        if (wantLogFile) {
            outputFilesList.add("log");
        }

        var rEnv = runContext.render(this.env).asMap(String.class, String.class);

        // We want to create input files once and reuse the same working dir for all commands
        CommandsWrapper baseWrapper = new CommandsWrapper(runContext)
            .withWarningOnStdErr(false)
            .withDockerOptions(injectDefaults(docker))
            .withTaskRunner(this.taskRunner)
            .withContainerImage(runContext.render(this.containerImage).as(String.class).orElseThrow())
            .withInterpreter(Property.ofValue(List.of("/bin/bash", "-c")))
            .withEnv(rEnv.isEmpty() ? new HashMap<>() : rEnv)
            .withNamespaceFiles(namespaceFiles)
            .withEnableOutputDirectory(true)
            .withOutputFiles(outputFilesList);

        Path workingDir = baseWrapper.getWorkingDirectory();

        Map<String, Object> extraVars = new HashMap<>();
        extraVars.put("workingDir", workingDir);
        Map<String, Object> additionalVars = this.taskRunner.additionalVars(runContext, baseWrapper);
        extraVars.putAll(additionalVars);

        PluginUtilsService.createInputFiles(
            runContext,
            workingDir,
            this.finalInputFiles(runContext, workingDir),
            additionalVars
        );
        emitInventoryAssets(runContext, workingDir);

        List<String> rCommands = runContext.render(this.commands).asList(String.class, extraVars);

        // run each ansible-playbook separately and merge outputs
        Map<String, Object> mergedVars = new HashMap<>();
        List<AnsibleOutput.PlaybookOutput> mergedPlaybooks = new ArrayList<>();
        List<Map<String, Object>> mergedRawOutputs = new ArrayList<>();

        int mergedExitCode = 0;
        int mergedStdOutCount = 0;
        int mergedStdErrCount = 0;

        Map<String, URI> lastOutputFiles = Map.of();
        TaskRunnerDetailResult lastTaskRunner = null;

        boolean beforeDone = false;

        // Collect per-command log paths (because Ansible truncates log_path each run)
        boolean multiCmd = rCommands.size() > 1;
        List<Path> perCommandLogs = new ArrayList<>();

        int idx = 0;
        for (String cmd : rCommands) {
            Map<String, String> envForRun = new HashMap<>(rEnv.isEmpty() ? Map.of() : rEnv);

            // If multiple commands and outputLogFile enabled,
            // override ANSIBLE_LOG_PATH so each run writes a different file.
            if (wantLogFile && multiCmd) {
                Path logPath = workingDir.resolve("log-" + idx);
                envForRun.put("ANSIBLE_LOG_PATH", logPath.toString());
                perCommandLogs.add(logPath);
            }

            CommandsWrapper commandWrapper = baseWrapper
                .withEnv(envForRun)
                // run beforeCommands only once, before the first command (rendered with extra vars)
                .withBeforeCommands(beforeDone ? null : Property.ofValue(
                    runContext.render(this.beforeCommands).asList(String.class, extraVars)
                ))
                // single command per run so Kestra doesn't overwrite outputs
                .withCommands(Property.ofValue(List.of(cmd)));

            ScriptOutput out = commandWrapper.run();

            mergedExitCode = Math.max(mergedExitCode, out.getExitCode());
            mergedStdOutCount += out.getStdOutLineCount();
            mergedStdErrCount += out.getStdErrLineCount();

            lastOutputFiles = out.getOutputFiles();
            lastTaskRunner = out.getTaskRunner();

            Map<String, Object> vars = out.getVars();
            if (vars != null) {
                // merge raw outputs (backward compatible)
                Object maybeOutputs = vars.get("outputs");
                if (maybeOutputs instanceof List<?> list) {
                    for (Object o : list) {
                        if (o instanceof Map<?, ?> m) {
                            // noinspection unchecked
                            mergedRawOutputs.add((Map<String, Object>) m);
                        }
                    }
                }

                // merge structured playbooks
                List<AnsibleOutput.PlaybookOutput> pbs = extractPlaybooks(vars);
                if (pbs != null && !pbs.isEmpty()) {
                    mergedPlaybooks.addAll(pbs);
                }

                // merge remaining vars (last-wins except lists above)
                for (Map.Entry<String, Object> e : vars.entrySet()) {
                    String key = e.getKey();
                    if ("outputs".equals(key) || "playbooks".equals(key)) {
                        continue;
                    }
                    mergedVars.put(key, e.getValue());
                }
            }

            beforeDone = true;
            idx++;
        }

        // If we produced per-command logs, concatenate into final "log"
        if (wantLogFile && multiCmd && !perCommandLogs.isEmpty()) {
            Path finalLog = workingDir.resolve("log");
            // truncate/create
            Files.writeString(finalLog, "", StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

            for (Path p : perCommandLogs) {
                if (Files.exists(p)) {
                    String content = Files.readString(p, StandardCharsets.UTF_8);
                    if (!content.isEmpty()) {
                        Files.writeString(finalLog, content, StandardCharsets.UTF_8,
                            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                        if (!content.endsWith("\n")) {
                            Files.writeString(finalLog, "\n", StandardCharsets.UTF_8,
                                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                        }
                    }
                }
            }

            // upload final log so outputs contains "log"
            URI logUri = runContext.storage().putFile(finalLog.toFile());
            Map<String, URI> patched = new HashMap<>(lastOutputFiles);
            patched.put("log", logUri);
            lastOutputFiles = patched;
        }

        // ensure merged vars expose the expected root keys
        mergedVars.put("outputs", mergedRawOutputs);
        mergedVars.put("playbooks", mergedPlaybooks);

        // minimal UI timeline support: emit dynamic worker results
        emitDynamicTaskRuns(runContext, mergedPlaybooks);

        return AnsibleOutput.builder()
            .vars(mergedVars)
            .exitCode(mergedExitCode)
            .outputFiles(lastOutputFiles)
            .stdOutLineCount(mergedStdOutCount)
            .stdErrLineCount(mergedStdErrCount)
            .taskRunner(lastTaskRunner)
            .playbooks(mergedPlaybooks)
            .build();
    }

    protected Map<String, String> finalInputFiles(RunContext runContext, Path workingDir) throws IOException, IllegalVariableEvaluationException {
        Map<String, String> map = this.inputFiles != null ? new HashMap<>(PluginUtilsService.transformInputFiles(runContext, this.inputFiles)) : new HashMap<>();

        // Add config file if not exists
        if (map.containsKey(ANSIBLE_CFG)) {
            runContext.logger().warn("Found an existing ansible.cfg file. Ignoring creation of a new ansible.cfg file.");
        } else {
            String config = runContext.render(this.ansibleConfig).as(String.class, Map.of("workingDir", workingDir)).orElseThrow();
            URI uri = runContext.storage().putFile(new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8)), ANSIBLE_CFG);
            map.put(ANSIBLE_CFG, uri.toString());
        }

        // Add python plugin
        InputStream ansibleCustomLogger = getClass().getClassLoader().getResourceAsStream(PLUGINS_KESTRA_LOGGER_PY);
        URI pluginUri = runContext.storage().putFile(ansibleCustomLogger, PLUGINS_KESTRA_LOGGER_PY);
        map.put(PLUGINS_KESTRA_LOGGER_PY, pluginUri.toString());
        return map;
    }

    private DockerOptions injectDefaults(DockerOptions original) {
        if (original == null) {
            return null;
        }

        var builder = original.toBuilder();
        if (original.getImage() == null) {
            builder.image(DEFAULT_IMAGE);
        }
        if (original.getEntryPoint() == null || original.getEntryPoint().isEmpty()) {
            builder.entryPoint(List.of(""));
        }

        return builder.build();
    }

    private void emitInventoryAssets(RunContext runContext, Path workingDir) throws Exception {
        if (!isAutoAssetsEnabled(runContext)) {
            return;
        }

        var inventoryPath = workingDir.resolve(INVENTORY_FILE);
        if (!Files.exists(inventoryPath)) {
            return;
        }

        var assets = parseInventoryAssets(inventoryPath);
        if (assets.isEmpty()) {
            return;
        }

        var flowInfo = runContext.flowInfo();
        var assetEmitter = runContext.assets();
        for (var asset : assets) {
            assetEmitter.upsert(Custom.builder()
                .tenantId(flowInfo.tenantId())
                .namespace(flowInfo.namespace())
                .id(asset.assetId())
                .type(TABLE_ASSET_TYPE)
                .metadata(asset.metadata())
                .build()
            );
            // .inputs(...)
            // .outputs(...)
        }
    }

    private boolean isAutoAssetsEnabled(RunContext runContext) throws IllegalVariableEvaluationException {
        var assetsProperty = this.getAssets();
        if (assetsProperty == null) {
            return false;
        }

        var rAssets = runContext.render(assetsProperty).as(AssetsDeclaration.class).orElse(null);
        return rAssets != null && rAssets.isEnableAuto();
    }

    private List<InventoryAsset> parseInventoryAssets(Path inventoryPath) throws IOException {
        var lines = Files.readAllLines(inventoryPath, StandardCharsets.UTF_8);
        var assetsByHost = new LinkedHashMap<String, String>();
        var currentGroup = DEFAULT_INVENTORY_GROUP;
        var inHostGroup = true;

        for (var rawLine : lines) {
            var line = rawLine.trim();
            if (line.isEmpty() || line.startsWith("#") || line.startsWith(";")) {
                continue;
            }

            if (line.startsWith("[") && line.endsWith("]")) {
                var groupName = line.substring(1, line.length() - 1).trim();
                if (groupName.isEmpty()) {
                    currentGroup = DEFAULT_INVENTORY_GROUP;
                    inHostGroup = true;
                    continue;
                }

                if (groupName.contains(":")) {
                    currentGroup = null;
                    inHostGroup = false;
                    continue;
                }

                currentGroup = groupName;
                inHostGroup = true;
                continue;
            }

            if (!inHostGroup || currentGroup == null) {
                continue;
            }

            var host = line.split("\\s+")[0].trim();
            if (host.isEmpty()) {
                continue;
            }

            assetsByHost.putIfAbsent(host, currentGroup);
        }

        return assetsByHost.entrySet().stream()
            .map(entry -> new InventoryAsset(entry.getKey(), Map.<String, Object>of(
                "provider", ASSET_PROVIDER,
                "region", ASSET_REGION,
                "state", ASSET_STATE,
                "group", entry.getValue()
            )))
            .toList();
    }

    private record InventoryAsset(String assetId, Map<String, Object> metadata) {
    }

    @SuppressWarnings("unchecked")
    private List<AnsibleOutput.PlaybookOutput> extractPlaybooks(Map<String, Object> vars) {
        if (vars == null) {
            return List.of();
        }

        Object maybePlaybooks = vars.get("playbooks");
        if (!(maybePlaybooks instanceof List<?> list)) {
            return List.of();
        }

        return JacksonMapper.ofJson().convertValue(
            list,
            JacksonMapper.ofJson().getTypeFactory()
                .constructCollectionType(List.class, AnsibleOutput.PlaybookOutput.class)
        );
    }

    /**
     * Create dynamic TaskRuns per Ansible task so UI can render bars.
     */
    private void emitDynamicTaskRuns(RunContext runContext, List<AnsibleOutput.PlaybookOutput> playbooks) throws IllegalVariableEvaluationException {
        if (playbooks == null || playbooks.isEmpty()) {
            return;
        }

        List<WorkerTaskResult> results = new ArrayList<>();

        for (AnsibleOutput.PlaybookOutput pb : playbooks) {
            if (pb == null || pb.getPlays() == null) continue;

            for (AnsibleOutput.PlayOutput play : pb.getPlays()) {
                if (play == null || play.getTasks() == null) continue;

                for (AnsibleOutput.TaskOutput task : play.getTasks()) {
                    if (task == null) continue;

                    String uid = task.getUid();
                    String startedAtStr = task.getStartedAt();
                    String endedAtStr = task.getEndedAt();
                    if (uid == null || startedAtStr == null || endedAtStr == null) {
                        continue; // no timeline info => skip
                    }

                    Instant started;
                    Instant ended;
                    try {
                        started = Instant.parse(startedAtStr);
                        ended = Instant.parse(endedAtStr);
                    } catch (Exception e) {
                        continue; // bad format => skip
                    }

                    ArrayList<State.History> histories = new ArrayList<>();
                    histories.add(new State.History(State.Type.CREATED, started));
                    histories.add(new State.History(State.Type.RUNNING, started));

                    // Compute final state: failed if any host failed/unreachable, else success.
                    State.Type finalType = State.Type.SUCCESS;
                    if (task.getHosts() != null) {
                        for (AnsibleOutput.HostResult hr : task.getHosts()) {
                            if (hr == null) continue;
                            String status = hr.getStatus();
                            if ("failed".equalsIgnoreCase(status) || "unreachable".equalsIgnoreCase(status)) {
                                finalType = State.Type.FAILED;
                                break;
                            }
                        }
                    }

                    histories.add(new State.History(finalType, ended));
                    State state = State.of(finalType, histories);

                    WorkerTaskResult wtr = WorkerTaskResult.builder()
                        .taskRun(TaskRun.builder()
                            .id(IdUtils.create())
                            .namespace(runContext.render("{{ flow.namespace }}"))
                            .flowId(runContext.render("{{ flow.id }}"))
                            .taskId(uid) // stable identity for UI grouping
                            .value(runContext.render("{{ taskrun.id }}"))
                            .executionId(runContext.render("{{ execution.id }}"))
                            .parentTaskRunId(runContext.render("{{ taskrun.id }}"))
                            .state(state)
                            .attempts(List.of(TaskRunAttempt.builder()
                                .state(state)
                                .build()
                            ))
                            .build()
                        )
                        .build();

                    results.add(wtr);
                }
            }
        }

        if (!results.isEmpty()) {
            runContext.dynamicWorkerResult(results);
        }
    }

    @SuperBuilder
    @Getter
    public static class AnsibleOutput extends ScriptOutput {

        @Schema(
            title = "Structured playbook outputs",
            description = "Each item corresponds to one ansible-playbook command execution with its plays, tasks, and host results."
        )
        private List<PlaybookOutput> playbooks;

        @Builder
        @Getter
        @NoArgsConstructor
        @AllArgsConstructor
        public static class PlaybookOutput {
            @Schema(
                title = "Plays executed in this playbook"
            )
            private List<PlayOutput> plays;
        }

        @Builder
        @Getter
        @NoArgsConstructor
        @AllArgsConstructor
        public static class PlayOutput {
            @Schema(
                title = "Play name",
                description = "If missing in Ansible (not mandatory, so possible), a fallback name can be used.",
                example = "Hello World Playbook"
            )
            private String name;

            @Schema(
                title = "Tasks executed in this play",
                description = "Indexed in execution order even when names are absent; each task yields one result set by host."
            )
            private List<TaskOutput> tasks;
        }

        @Builder
        @Getter
        @NoArgsConstructor
        @AllArgsConstructor
        public static class TaskOutput {
            @Schema(
                title = "Stable task uid",
                example = "play:Hello World Playbook|task:Task 1"
            )
            private String uid;

            @Schema(
                title = "Task name",
                description = "If missing in Ansible (not mandatory, so possible), fallback to action or 'unnamed_task_<n>'.",
                example = "Task 1"
            )
            private String name;

            @Schema(
                title = "Task start time (UTC ISO-8601)",
                example = "2025-11-28T14:58:23.569Z"
            )
            private String startedAt;

            @Schema(
                title = "Task end time (UTC ISO-8601)",
                example = "2025-11-28T14:58:23.589Z"
            )
            private String endedAt;

            @Schema(
                title = "Per-host results for this task",
                description = "A task can target multiple hosts; each host yields one result event."
            )
            private List<HostResult> hosts;
        }

        @Builder
        @Getter
        @NoArgsConstructor
        @AllArgsConstructor
        public static class HostResult {
            @Schema(
                title = "Host name from inventory",
                example = "localhost1"
            )
            private String host;

            @Schema(
                title = "Execution status",
                description = "Typical values: ok, failed, skipped, unreachable.",
                example = "ok"
            )
            private String status;

            @Schema(
                title = "Raw Ansible result payload for this host",
                description = """
                    Arbitrary structure directly from Ansible.
                    If the task uses loops, Ansible already returns a list in this object.
                    """
            )
            private Object result;
        }
    }
}
