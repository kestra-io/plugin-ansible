package io.kestra.plugin.ansible.cli;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.slf4j.event.Level;

import com.fasterxml.jackson.core.type.TypeReference;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.assets.AssetIdentifier;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.executions.TaskRunAttempt;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.*;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.models.tasks.runners.TaskRunnerDetailResult;
import io.kestra.core.queues.QueueException;
import io.kestra.core.runners.AssetEmit;
import io.kestra.core.runners.DynamicTaskRunLog;
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
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run Ansible CLI commands",
    description = """
        Executes ansible or ansible-playbook commands with the configured task runner. Generates ansible.cfg with the Kestra callback by default unless you supply one, and merges outputs across multiple commands.
        A `requirements.txt` or `requirements.yml` present in the working directory is installed before commands run; see `autoInstallPythonRequirements` and `autoInstallGalaxyRequirements`.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Execute a list of Ansible CLI commands to orchestrate an Ansible playbook stored in the Editor using [Namespace Files](https://kestra.io/docs/concepts/namespace-files).",
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
        ),
        @Example(
            title = "Expose only explicitly declared playbook values as outputs. With `outputsMode: EXPLICIT`, the bundled `kestra` module declares which values become task outputs; raw per-host results are redacted from outputs and logs, so sensitive data fetched by the playbook is never leaked. The playbook is kept in flow variables without using the `render()` function, so that its Ansible Jinja expressions are not evaluated by Kestra.",
            full = true,
            code = """
                id: ansible_explicit_outputs
                namespace: company.team

                variables:
                  playbook: |
                    ---
                    - hosts: localhost
                      tasks:
                        - name: Fetch credentials needed by the automation
                          ansible.builtin.set_fact:
                            credential:
                              username: svc-automation
                              password: "not-for-kestra-outputs"

                        - name: Do the work
                          ansible.builtin.set_fact:
                            records_updated: 3
                          register: work_result

                        - name: Declare what downstream tasks may see
                          kestra:
                            outputs:
                              records_updated: "{{ records_updated }}"
                              work_status: "{{ 'skipped' if work_result.skipped | default(false) else 'ok' }}"

                tasks:
                  - id: ansible_task
                    type: io.kestra.plugin.ansible.cli.AnsibleCLI
                    outputsMode: EXPLICIT
                    inputFiles:
                      playbook.yml: "{{ vars.playbook }}"
                    containerImage: cytopia/ansible:latest-tools
                    commands:
                      - ansible-playbook -i localhost -c local playbook.yml
                """
        ),
        @Example(
            title = "Supply a custom `ansible.cfg` while keeping the Kestra integration working. When you provide your own `ansibleConfig`, the generated one is skipped entirely, so you must keep the `callback_plugins`/`callbacks_enabled` lines for output capture and the `library = ./library` line for the bundled `kestra` module to resolve; add your own settings below them.",
            full = true,
            code = """
                id: ansible_custom_config
                namespace: company.team

                tasks:
                  - id: ansible_task
                    type: io.kestra.plugin.ansible.cli.AnsibleCLI
                    outputsMode: EXPLICIT
                    ansibleConfig: |
                      [defaults]
                      log_path          = {{ workingDir }}/log
                      callback_plugins  = ./callback_plugins
                      callbacks_enabled = kestra_logger
                      stdout_callback   = ansible.builtin.null
                      result_format     = json
                      pretty_results    = true
                      library           = ./library
                      forks             = 10
                      timeout           = 30
                    inputFiles:
                      playbook.yml: |
                        ---
                        - hosts: localhost
                          tasks:
                            - name: Declare outputs
                              kestra:
                                outputs:
                                  deployed: true
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
    public static final String LIBRARY_KESTRA_PY = "library/kestra.py";
    public static final String OUTPUTS_MODE_ENV = "KESTRA_OUTPUTS_MODE";
    public static final String OUTPUTS_FILE_ENV = "KESTRA_OUTPUTS_FILE";
    private static final String INVENTORY_FILE = "inventory.ini";
    private static final String VM_ASSET_TYPE = "io.kestra.plugin.ee.assets.VM";
    private static final Pattern ASSET_ID_PATTERN = Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9._-]*$");
    // 10 MB: comfortably under common queue message-size limits (e.g. a customer's
    // kestra.queue.message-protection.limit of 50 MB) while leaving headroom for the rest of the
    // WorkerTaskResult. This is a judgement call, not a guarantee: a plugin cannot read the
    // configured queue limit at runtime, see `maxOutputsSize` schema.
    private static final long DEFAULT_MAX_OUTPUTS_SIZE = 10_000_000L;
    // 100 MB: an upper ceiling on what the property may be raised to. Past this, the payload is
    // beyond any realistic queue message-size limit and the guard would only be nominal, while
    // the parse itself would have to materialize that volume in the worker heap.
    static final long MAX_OUTPUTS_SIZE_CEILING = 100_000_000L;
    // Plenty for a summary or a failure reason while keeping a single dynamic-taskrun log line
    // from growing unbounded when a host result is large.
    private static final int MAX_LOG_LINE_LENGTH = 4_000;

    // Ensure Ansible can find our bundled callback/library dirs regardless of where the playbook
    // lives or what the image configures (issue #120). Prepend them onto the search path using the
    // runtime working dir ($PWD, the current directory on every runner), keeping any existing value
    // so an image's own callback (e.g. ARA) still loads.
    private static final List<String> CALLBACK_PATH_EXPORTS = List.of(
        "export ANSIBLE_CALLBACK_PLUGINS=\"$PWD/callback_plugins${ANSIBLE_CALLBACK_PLUGINS:+:$ANSIBLE_CALLBACK_PLUGINS}\"",
        "export ANSIBLE_LIBRARY=\"$PWD/library${ANSIBLE_LIBRARY:+:$ANSIBLE_LIBRARY}\""
    );

    @Schema(
        title = "Run once before commands",
        description = "Optional shell commands executed only before the first main command, rendered with the same variables."
    )
    @PluginProperty(group = "execution")
    protected Property<List<String>> beforeCommands;

    @Schema(
        title = "Commands to run sequentially",
        description = """
            Commands are executed one by one in the same working directory and their outputs are merged.
            List each `ansible-playbook` invocation as its own entry: the outputs payload is written to one file per entry, so invocations chained with `&&`/`;` overwrite each other's payload.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<List<String>> commands;

    @Schema(
        title = "Additional environment variables",
        description = "Variables injected into the task runner environment for every command."
    )
    @PluginProperty(group = "execution")
    protected Property<Map<String, String>> env;

    @Schema(
        title = "Deprecated Docker options",
        description = "Use taskRunner instead; kept for backward compatibility."
    )
    @PluginProperty(group = "deprecated")
    @Deprecated
    private DockerOptions docker;

    @Schema(
        title = "Task runner",
        description = "Runner implementation to execute the commands; defaults to Docker. Provide runner-specific properties as needed."
    )
    @PluginProperty(group = "execution")
    @Builder.Default
    @Valid
    protected TaskRunner<?> taskRunner = Docker.instance();

    @Schema(
        title = "Task runner container image",
        description = "Used only by container-based runners; defaults to cytopia/ansible:latest-tools. Supply a lean image with required modules to speed execution. Non-container runners won't include Ansible dependencies, so rely on this image (or provide your own) when you need them."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    @Schema(
        title = "Ansible configuration",
        description = """
            If omitted, a generated ansible.cfg in the working directory enables the Kestra callback plugin and logs to `log`.
            Provide custom content to override defaults; include the callback settings above if you still want structured outputs.
            The bundled callback and module directories are also pinned via `ANSIBLE_CALLBACK_PLUGINS` and `ANSIBLE_LIBRARY`, which supersede a custom `ansible.cfg`: declare your own callback or module paths through the task's `env` instead.
            """
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<String> ansibleConfig = Property.ofExpression("""
        [defaults]
        log_path          = {{ workingDir }}/log
        callback_plugins  = ./callback_plugins
        callbacks_enabled = kestra_logger
        stdout_callback   = ansible.builtin.null
        result_format     = json
        pretty_results    = true
        library           = ./library
        """);

    @Schema(
        title = "Outputs capture mode",
        description = """
            ALL (default) captures every per-host result of every playbook task; `outputs` is a list of per-host result maps.
            EXPLICIT captures only values declared in the playbook via the bundled `kestra` module, redacting per-host payloads to `{"changed": <bool>}` while keeping task names, timings, and statuses; `outputs` is then a map, not a list, so switching modes changes its shape for downstream references.
            A custom `ansibleConfig` must keep `stdout_callback = ansible.builtin.null` to preserve redaction, and `library = ./library` for the bundled module to resolve.
            """
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<OutputsMode> outputsMode = Property.ofValue(OutputsMode.ALL);

    @Schema(
        title = "Maximum size of the outputs payload",
        description = """
            Upper bound, in bytes, on the serialized size of the merged `outputs`/`playbooks` payload; default 10 000 000 (10 MB), maximum 100 000 000 (100 MB). Exceeding it fails the task instead of emitting an oversized output.
            This guards against the `WorkerTaskResult` being rejected by the message queue (`kestra.queue.message-protection.limit`), which a plugin cannot read: align this value with your instance's configuration. It is checked once all commands have completed, so it never shortens a slow run.
            It also bounds what is read back into memory: a callback outputs file larger than this value is never parsed, so an oversized run fails instead of materializing that volume in the worker heap.
            """
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    protected Property<@Min(1) @Max(MAX_OUTPUTS_SIZE_CEILING) Long> maxOutputsSize = Property.ofValue(DEFAULT_MAX_OUTPUTS_SIZE);

    @Schema(
        title = "Per-host task log verbosity",
        description = """
            SUMMARY (default) logs one line per host: its status, plus the error reason for `failed`/`unreachable` hosts. FULL logs the whole per-host result payload (truncated past a few KB).
            This changes only what is logged, never what is captured: EXPLICIT-mode redaction happens upstream in the callback, so FULL is not an escape hatch from it.
            """
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<LogsMode> logsMode = Property.ofValue(LogsMode.SUMMARY);

    @Schema(
        title = "Publish Ansible log file",
        description = "If true, uploads the ansible log as output file `log`; multi-command runs concatenate per-command logs. Default is false."
    )
    @Builder.Default
    @PluginProperty(group = "source")
    private Property<Boolean> outputLogFile = Property.ofValue(false);

    @Schema(
        title = "Auto-install Python dependencies",
        description = """
            If true (default), runs `pip install --no-cache-dir -r requirements.txt` before commands when a `requirements.txt` file is present in the working directory.
            The check is performed at runtime in the runner shell, so the file may come from `inputFiles`, `namespaceFiles`, or any other source materialized into the working directory.
            """
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<Boolean> autoInstallPythonRequirements = Property.ofValue(true);

    @Schema(
        title = "Auto-install Ansible Galaxy collections and roles",
        description = """
            If true (default), runs `ansible-galaxy install -r requirements.yml` before commands when a `requirements.yml` file is present in the working directory.
            Since Ansible 2.10, `ansible-galaxy install` handles both the `collections:` and `roles:` keys defined in `requirements.yml`.
            """
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<Boolean> autoInstallGalaxyRequirements = Property.ofValue(true);

    @PluginProperty(group = "source")
    private NamespaceFiles namespaceFiles;

    @PluginProperty(group = "source")
    private Object inputFiles;

    @PluginProperty(group = "destination")
    private Property<List<String>> outputFiles;

    @Override
    public AnsibleOutput run(RunContext runContext) throws Exception {
        List<String> outputFilesList = new ArrayList<>(runContext.render(this.outputFiles).asList(String.class));

        boolean wantLogFile = runContext.render(this.outputLogFile).as(Boolean.class).orElse(false);
        if (wantLogFile) {
            outputFilesList.add("log");
        }

        var rEnv = runContext.render(this.env).asMap(String.class, String.class);

        OutputsMode rOutputsModeEnum = runContext.render(this.outputsMode).as(OutputsMode.class)
            .orElse(OutputsMode.ALL);
        String rOutputsMode = rOutputsModeEnum.name().toLowerCase(Locale.ROOT);

        long rMaxOutputsSize = runContext.render(this.maxOutputsSize).as(Long.class).orElse(DEFAULT_MAX_OUTPUTS_SIZE);
        LogsMode rLogsMode = runContext.render(this.logsMode).as(LogsMode.class).orElse(LogsMode.SUMMARY);

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

        PluginUtilsService.createInputFilesRaw(
            runContext,
            workingDir,
            this.finalInputFiles(runContext, workingDir)
        );
        emitInventoryAssets(runContext, workingDir);

        // Auto-install requirement files when present in the working directory.
        // Shell-level conditionals so detection happens after working dir is materialized by the task runner.
        // `[ ! -f X ] || cmd` — file absent: exits 0; file present: runs cmd and propagates its exit code.
        List<String> autoInstallCommands = new ArrayList<>();
        if (runContext.render(this.autoInstallPythonRequirements).as(Boolean.class).orElseThrow()) {
            autoInstallCommands.add("[ ! -f requirements.txt ] || pip install --no-cache-dir -r requirements.txt");
        }
        if (runContext.render(this.autoInstallGalaxyRequirements).as(Boolean.class).orElseThrow()) {
            autoInstallCommands.add("[ ! -f requirements.yml ] || ansible-galaxy install -r requirements.yml");
        }

        List<String> rCommands = runContext.render(this.commands).asList(String.class, extraVars);

        // run each ansible-playbook separately and merge outputs
        Map<String, Object> mergedVars = new HashMap<>();
        List<AnsibleOutput.PlaybookOutput> mergedPlaybooks = new ArrayList<>();
        Map<String, Object> mergedExplicitOutputs = new HashMap<>();

        int mergedExitCode = 0;
        int mergedStdOutCount = 0;
        int mergedStdErrCount = 0;
        // largest callback outputs file that was refused before parsing, see readOutputsFile
        long oversizedOutputsFileBytes = 0;

        Map<String, URI> lastOutputFiles = Map.of();
        TaskRunnerDetailResult lastTaskRunner = null;

        boolean beforeDone = false;

        // Collect per-command log paths (because Ansible truncates log_path each run)
        boolean multiCmd = rCommands.size() > 1;
        List<Path> perCommandLogs = new ArrayList<>();

        int idx = 0;
        for (String cmd : rCommands) {
            Map<String, String> envForRun = new HashMap<>(rEnv.isEmpty() ? Map.of() : rEnv);
            envForRun.put(OUTPUTS_MODE_ENV, rOutputsMode);

            // Each command gets its own outputs file: the callback writes it once per
            // ansible-playbook run, and a shared name would let a later command in a
            // multi-command task overwrite an earlier one's payload before we read it back.
            Path outputsFile = workingDir.resolve("kestra-outputs-" + idx + ".json");
            envForRun.put(OUTPUTS_FILE_ENV, outputsFile.toString());

            // If multiple commands and outputLogFile enabled,
            // override ANSIBLE_LOG_PATH so each run writes a different file.
            if (wantLogFile && multiCmd) {
                Path logPath = workingDir.resolve("log-" + idx);
                envForRun.put("ANSIBLE_LOG_PATH", logPath.toString());
                perCommandLogs.add(logPath);
            }

            // First (before any user `cd`, so $PWD is the working-dir root) and on every command
            // (each runs in its own container).
            List<String> beforeForRun = new ArrayList<>(CALLBACK_PATH_EXPORTS);
            if (!beforeDone) {
                // User beforeCommands can configure the environment
                // (e.g. private pip index, proxy, auth) before auto-install fires.
                beforeForRun.addAll(runContext.render(this.beforeCommands).asList(String.class, extraVars));
                beforeForRun.addAll(autoInstallCommands);
            }
            Property<List<String>> mergedBeforeCommands = Property.ofValue(beforeForRun);

            CommandsWrapper commandWrapper = baseWrapper
                .withEnv(envForRun)
                // user beforeCommands + auto-install run once before the first command; the callback
                // path exports are re-applied before every command (each runs in its own container)
                .withBeforeCommands(mergedBeforeCommands)
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
                // merge structured playbooks, in case a command's own stdout emits them
                // directly (e.g. a user-authored "::{...}::" line); ansible-playbook runs no
                // longer go through this path (see the outputs file read below, issue #126)
                List<AnsibleOutput.PlaybookOutput> pbs = extractPlaybooks(vars);
                if (pbs != null && !pbs.isEmpty()) {
                    mergedPlaybooks.addAll(pbs);
                }

                // merge remaining vars (last-wins); "outputs" is rebuilt from playbooks below
                for (Map.Entry<String, Object> e : vars.entrySet()) {
                    String key = e.getKey();
                    if ("outputs".equals(key) || "playbooks".equals(key)) {
                        continue;
                    }
                    mergedVars.put(key, e.getValue());
                }
            }

            // Read back the outputs/playbooks payload the kestra_logger callback wrote to a
            // file instead of printing to stdout: a single, potentially multi-MB stdout line
            // stalls the task runner's line-oriented log pipeline for minutes (issue #126).
            boolean looksLikePlaybookCommand = cmd.contains("ansible-playbook");
            OutputsFileRead outputsRead = readOutputsFile(runContext, outputsFile, looksLikePlaybookCommand, rMaxOutputsSize);
            oversizedOutputsFileBytes = Math.max(oversizedOutputsFileBytes, outputsRead.oversizedBytes());
            outputsRead.payload().ifPresent(payload -> {
                if (payload.get("outputs") instanceof Map<?, ?> explicit) {
                    explicit.forEach((k, v) -> mergedExplicitOutputs.put(String.valueOf(k), v));
                }

                List<AnsibleOutput.PlaybookOutput> filePlaybooks = extractPlaybooks(payload);
                if (!filePlaybooks.isEmpty()) {
                    mergedPlaybooks.addAll(filePlaybooks);
                }
            });

            beforeDone = true;
            idx++;
        }

        // If we produced per-command logs, concatenate into final "log"
        if (wantLogFile && multiCmd && !perCommandLogs.isEmpty()) {
            Path finalLog = workingDir.resolve("log");
            // truncate/create
            Files.writeString(
                finalLog, "", StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING
            );

            for (Path p : perCommandLogs) {
                if (Files.exists(p)) {
                    String content = Files.readString(p, StandardCharsets.UTF_8);
                    if (!content.isEmpty()) {
                        Files.writeString(
                            finalLog, content, StandardCharsets.UTF_8,
                            StandardOpenOption.CREATE, StandardOpenOption.APPEND
                        );
                        if (!content.endsWith("\n")) {
                            Files.writeString(
                                finalLog, "\n", StandardCharsets.UTF_8,
                                StandardOpenOption.CREATE, StandardOpenOption.APPEND
                            );
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

        // ensure merged vars expose the expected root keys; in ALL mode the flat list is
        // rebuilt from the structured playbooks instead of being duplicated by the callback
        Object mergedRawOrExplicitOutputs = rOutputsModeEnum == OutputsMode.EXPLICIT
            ? mergedExplicitOutputs
            : flattenHostResults(mergedPlaybooks);
        mergedVars.put("outputs", mergedRawOrExplicitOutputs);
        mergedVars.put("playbooks", mergedPlaybooks);

        // Emit before the size guard: these travel on the log queue, not the WorkerTaskResult the
        // guard protects, so a run that trips the guard still leaves per-task diagnostics behind.
        emitDynamicTaskRuns(runContext, mergedPlaybooks, rLogsMode);

        failOnOversizedOutputsFile(oversizedOutputsFileBytes, rMaxOutputsSize);
        checkOutputsSize(mergedVars, rMaxOutputsSize);

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
        URI pluginUri = runContext.storage().putFile(bundledResource(PLUGINS_KESTRA_LOGGER_PY), PLUGINS_KESTRA_LOGGER_PY);
        map.put(PLUGINS_KESTRA_LOGGER_PY, pluginUri.toString());

        // Add the kestra module so playbooks can declare explicit outputs
        URI moduleUri = runContext.storage().putFile(bundledResource(LIBRARY_KESTRA_PY), LIBRARY_KESTRA_PY);
        map.put(LIBRARY_KESTRA_PY, moduleUri.toString());
        return map;
    }

    private InputStream bundledResource(String path) throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(path);
        if (stream == null) {
            throw new IOException("Bundled resource not found on the classpath: " + path);
        }
        return stream;
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
        var inventoryPath = workingDir.resolve(INVENTORY_FILE);
        if (!Files.isRegularFile(inventoryPath)) {
            return;
        }

        var inventoryContent = Files.readString(inventoryPath, StandardCharsets.UTF_8);
        var inputs = extractInventoryAssetInputs(inventoryContent);
        if (inputs.isEmpty()) {
            return;
        }

        try {
            var assetEmitter = runContext.assets();
            var alreadyEmittedInputs = new LinkedHashSet<>(
                assetEmitter.emitted().stream()
                    .flatMap(assetEmit -> assetEmit.inputs().stream())
                    .toList()
            );
            var newInputs = inputs.stream()
                .filter(input -> !alreadyEmittedInputs.contains(input))
                .toList();

            if (newInputs.isEmpty()) {
                runContext.logger().debug("No new host asset input(s) to emit from '{}'.", INVENTORY_FILE);
                return;
            }

            assetEmitter.emit(new AssetEmit(newInputs, List.of()));
            runContext.logger().info("Emitted {} host asset input(s) from '{}'.", newInputs.size(), INVENTORY_FILE);
        } catch (UnsupportedOperationException e) {
            // OSS edition or tests where EE assets are not available — silently skip.
            runContext.logger().debug("Asset emission is not supported in this edition, skipping.");
        } catch (QueueException e) {
            runContext.logger().warn("Unable to emit host asset input(s) from '{}'.", INVENTORY_FILE, e);
        }
    }

    static List<AssetIdentifier> extractInventoryAssetInputs(String inventoryContent) {
        if (inventoryContent == null || inventoryContent.isBlank()) {
            return List.of();
        }

        var uniqueHosts = new LinkedHashSet<String>();
        var hostSection = true;

        for (var rawLine : inventoryContent.split("\\R")) {
            var line = stripInlineComment(rawLine).trim();
            if (line.isEmpty()) {
                continue;
            }

            if (line.startsWith("[") && line.endsWith("]") && line.length() > 2) {
                var section = line.substring(1, line.length() - 1).trim().toLowerCase(Locale.ROOT);
                hostSection = !section.endsWith(":vars") && !section.endsWith(":children");
                continue;
            }

            if (!hostSection) {
                continue;
            }

            var host = line.split("\\s+")[0].trim();
            if (host.isEmpty() || host.contains("=") || !ASSET_ID_PATTERN.matcher(host).matches()) {
                continue;
            }

            uniqueHosts.add(host);
        }

        return uniqueHosts.stream()
            .map(host -> new AssetIdentifier(null, null, host, VM_ASSET_TYPE))
            .toList();
    }

    private static String stripInlineComment(String rawLine) {
        if (rawLine == null) {
            return "";
        }

        var trimmed = rawLine.trim();
        if (trimmed.startsWith("#") || trimmed.startsWith(";")) {
            return "";
        }

        return trimmed.replaceFirst("\\s[;#].*$", "");
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

    /** Streams the non-null elements of a possibly null list. */
    private static <T> Stream<T> nonNulls(List<T> list) {
        return list == null ? Stream.empty() : list.stream().filter(Objects::nonNull);
    }

    /** Walks the structured payload down to every Ansible task, in playbook -&gt; play -&gt; task order. */
    private static Stream<AnsibleOutput.TaskOutput> tasks(List<AnsibleOutput.PlaybookOutput> playbooks) {
        return nonNulls(playbooks)
            .flatMap(playbook -> nonNulls(playbook.getPlays()))
            .flatMap(play -> nonNulls(play.getTasks()));
    }

    /**
     * Rebuilds the flat, backward-compatible ALL-mode "outputs" list from the structured
     * playbooks, in playbook -&gt; play -&gt; task -&gt; host order. The callback used to emit every
     * per-host result twice (once flat, once structured); it now only emits the structured form,
     * see kestra_logger.py's `_log_kestra_outputs` (issue #126).
     */
    static List<Object> flattenHostResults(List<AnsibleOutput.PlaybookOutput> playbooks) {
        return tasks(playbooks)
            .flatMap(task -> nonNulls(task.getHosts()))
            .map(AnsibleOutput.HostResult::getResult)
            .toList();
    }

    /**
     * Reads back the outputs/playbooks payload the kestra_logger callback writes to a file
     * (env var {@link #OUTPUTS_FILE_ENV}) instead of printing it to stdout. Degrades gracefully:
     * a missing or unreadable file is not a hard failure, since it can legitimately happen when
     * the command did not run ansible-playbook at all, the playbook crashed before completing, or
     * a user-supplied `ansibleConfig` does not load the callback.
     *
     * <p>The file is never parsed before its size has been checked against {@code maxOutputsSize}:
     * the payload this fix moves off stdout can be hundreds of megabytes, and deserializing it
     * would materialize that volume in the worker heap before the output guard could reject it.
     * An oversized file is reported back through {@link OutputsFileRead#oversizedBytes()} so the
     * caller can still emit the diagnostics it has before failing the task.
     *
     * <p>The file is deleted once consumed: in ALL mode it holds raw per-host results (registered
     * vars, stdout/msg, gathered facts) that may carry secrets a playbook fetched, and there is no
     * reason to leave them in plaintext in the working directory for the rest of the task.
     *
     * @param warnIfMissing whether a missing file is worth a warning; suppressed for commands that
     *                       do not look like an ansible-playbook invocation, to avoid spurious
     *                       warnings on every auto-install/before-command in a multi-command task.
     */
    OutputsFileRead readOutputsFile(RunContext runContext, Path outputsFile, boolean warnIfMissing, long maxOutputsSize) {
        if (!Files.isRegularFile(outputsFile)) {
            if (warnIfMissing) {
                runContext.logger().warn(
                    "Ansible outputs file '{}' was not found after running an ansible-playbook command; its "
                        + "outputs/playbooks will be empty. This is expected if the playbook crashed before "
                        + "completing, or if a custom `ansibleConfig` does not enable the bundled callback "
                        + "(it needs both `callback_plugins`/`callbacks_enabled = kestra_logger` and `library = ./library`).",
                    outputsFile
                );
            }
            return OutputsFileRead.EMPTY;
        }

        try {
            // The file only ever holds "playbooks" (plus "outputs" in EXPLICIT mode), while the
            // final payload adds the flat list rebuilt from those same playbooks: a file already
            // over the bound can never fit, so rejecting on file size raises no false failure.
            long fileSize = Files.size(outputsFile);
            if (fileSize > maxOutputsSize) {
                return new OutputsFileRead(Optional.empty(), fileSize);
            }

            try (InputStream is = Files.newInputStream(outputsFile)) {
                return new OutputsFileRead(
                    Optional.ofNullable(JacksonMapper.ofJson().readValue(is, new TypeReference<Map<String, Object>>() {})),
                    0
                );
            }
        } catch (IOException e) {
            runContext.logger().warn(
                "Unable to parse the Ansible outputs file '{}': {}. Its outputs/playbooks will be empty.",
                outputsFile, e.getMessage()
            );
            return OutputsFileRead.EMPTY;
        } finally {
            try {
                Files.deleteIfExists(outputsFile);
            } catch (IOException e) {
                runContext.logger().debug("Unable to delete the Ansible outputs file '{}': {}", outputsFile, e.getMessage());
            }
        }
    }

    /**
     * Outcome of {@link #readOutputsFile}: either the parsed payload, or the size of a file that
     * was too large to be parsed at all (0 when nothing was oversized).
     */
    record OutputsFileRead(Optional<Map<String, Object>> payload, long oversizedBytes) {
        static final OutputsFileRead EMPTY = new OutputsFileRead(Optional.empty(), 0);
    }

    /**
     * Guards against a `WorkerTaskResult` the platform message queue may reject (see
     * `kestra.queue.message-protection.limit`). This runs after every command has already
     * completed, so it is a task-output/queue guard, not a fix for slow execution — it cannot
     * make a long-running ansible-playbook command finish sooner.
     */
    void checkOutputsSize(Map<String, Object> mergedVars, long maxOutputsSize) throws IOException {
        Map<String, Object> outputsPayload = new HashMap<>();
        outputsPayload.put("outputs", mergedVars.get("outputs"));
        outputsPayload.put("playbooks", mergedVars.get("playbooks"));

        // Measure by counting the serialized bytes as they are produced, and stop at the bound:
        // materializing the whole payload as a byte[] just to read its length would allocate a
        // second full copy of data that was itself just parsed out of the outputs file.
        try (BoundedCountingOutputStream counter = new BoundedCountingOutputStream(maxOutputsSize)) {
            JacksonMapper.ofJson().writeValue(counter, outputsPayload);
        } catch (LimitExceededException e) {
            throw new IllegalStateException(outputsTooLargeMessage(
                "Ansible outputs payload exceeds the configured `maxOutputsSize` of " + maxOutputsSize + " bytes."
            ));
        }
    }

    /**
     * Fails the task when a callback outputs file was too large to be parsed at all, with the same
     * actionable message as {@link #checkOutputsSize}. Called after the per-task logs have been
     * emitted, so the run still leaves behind whatever diagnostics the other commands produced.
     */
    static void failOnOversizedOutputsFile(long oversizedBytes, long maxOutputsSize) {
        if (oversizedBytes <= 0) {
            return;
        }

        throw new IllegalStateException(outputsTooLargeMessage(
            "The Ansible outputs file written by the callback is " + oversizedBytes + " bytes, exceeding the "
                + "configured `maxOutputsSize` of " + maxOutputsSize + " bytes; it was not parsed, so it could "
                + "not exhaust the worker's heap."
        ));
    }

    private static String outputsTooLargeMessage(String what) {
        return what
            + " This guards against a worker task result the platform queue may reject; it is unrelated to how"
            + " long the ansible-playbook command itself took to run. Reduce the captured volume with"
            + " `outputsMode: EXPLICIT` (only declared values are exposed), or raise `maxOutputsSize` if your"
            + " instance's queue is configured to accept larger messages.";
    }

    /** Counts written bytes without keeping them, and aborts as soon as the bound is exceeded. */
    private static final class BoundedCountingOutputStream extends OutputStream {
        private final long limit;
        private long count;

        private BoundedCountingOutputStream(long limit) {
            this.limit = limit;
        }

        @Override
        public void write(int b) throws IOException {
            add(1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            add(len);
        }

        private void add(long written) throws IOException {
            count += written;
            if (count > limit) {
                throw new LimitExceededException();
            }
        }
    }

    /** Internal signal that {@link BoundedCountingOutputStream}'s bound was crossed. */
    private static final class LimitExceededException extends IOException {
        @Override
        public synchronized Throwable fillInStackTrace() {
            // control flow only, never surfaced: skip the stack trace
            return this;
        }
    }

    /**
     * Create one dynamic TaskRun per Ansible task and emit that task's host-result lines as logs
     * tagged with the taskrun's id, so logs are attributed per Ansible task instead of all landing
     * on the parent task's root taskrun (issue kestra-ee#8520).
     */
    private void emitDynamicTaskRuns(RunContext runContext, List<AnsibleOutput.PlaybookOutput> playbooks, LogsMode logsMode) throws IllegalVariableEvaluationException {
        if (playbooks == null || playbooks.isEmpty()) {
            return;
        }

        // toList() so the body can throw the checked render() exception
        for (AnsibleOutput.TaskOutput task : tasks(playbooks).toList()) {
            String uid = task.getUid();
            Instant started = parseInstant(task.getStartedAt());
            Instant ended = parseInstant(task.getEndedAt());
            if (uid == null || started == null || ended == null) {
                continue; // missing or unparseable timeline info => skip
            }

            State.Type finalType = hasFailedHost(task) ? State.Type.FAILED : State.Type.SUCCESS;
            State state = State.of(finalType, new ArrayList<>(List.of(
                new State.History(State.Type.CREATED, started),
                new State.History(State.Type.RUNNING, started),
                new State.History(finalType, ended)
            )));

            TaskRun subTaskRun = TaskRun.builder()
                .id(IdUtils.create())
                .tenantId(runContext.flowInfo().tenantId())
                .namespace(runContext.render("{{ flow.namespace }}"))
                .flowId(runContext.render("{{ flow.id }}"))
                .taskId(uid) // stable identity for per-task grouping
                .executionId(runContext.render("{{ execution.id }}"))
                .parentTaskRunId(runContext.render("{{ taskrun.id }}"))
                .state(state)
                .attempts(
                    List.of(
                        TaskRunAttempt.builder()
                            .state(state)
                            .build()
                    )
                )
                .build();

            // Logs ride with the taskrun, so the run context fills in the execution context
            // and masks secrets; the plugin never builds a LogEntry itself.
            runContext.dynamicWorkerResult(
                WorkerTaskResult.builder().taskRun(subTaskRun).build(),
                taskLogs(task, logsMode)
            );
        }
    }

    private static Instant parseInstant(String value) {
        if (value == null) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean hasFailedHost(AnsibleOutput.TaskOutput task) {
        return nonNulls(task.getHosts()).anyMatch(host -> isFailure(host.getStatus()));
    }

    private static boolean isFailure(String status) {
        return "failed".equalsIgnoreCase(status) || "unreachable".equalsIgnoreCase(status);
    }

    /**
     * Build the host-result log lines for a task.
     * SUMMARY (default) logs only the status for ok/skipped hosts, and the status plus the
     * failure reason for failed/unreachable hosts. FULL always logs the entire result payload.
     * Neither mode overrides EXPLICIT-mode redaction: the host result payload is already reduced
     * by the callback before it ever reaches this method.
     */
    static List<DynamicTaskRunLog> taskLogs(AnsibleOutput.TaskOutput task, LogsMode logsMode) {
        if (task.getHosts() == null) {
            return List.of();
        }

        List<DynamicTaskRunLog> logs = new ArrayList<>();
        for (AnsibleOutput.HostResult hr : task.getHosts()) {
            if (hr == null) {
                continue;
            }

            String status = hr.getStatus();
            boolean failed = isFailure(status);

            String message;
            if (logsMode == LogsMode.FULL) {
                message = "[" + hr.getHost() + "] " + status + " => " + truncateForLog(stringifyResult(hr.getResult()));
            } else if (failed) {
                message = "[" + hr.getHost() + "] " + status + " => " + truncateForLog(failureReason(hr.getResult()));
            } else {
                message = "[" + hr.getHost() + "] " + status;
            }

            logs.add(new DynamicTaskRunLog(failed ? Level.ERROR : Level.INFO, message));
        }

        return logs;
    }

    static String failureReason(Object result) {
        if (result instanceof Map<?, ?> m && m.get("msg") != null) {
            return String.valueOf(m.get("msg"));
        }
        return stringifyResult(result);
    }

    static String truncateForLog(String s) {
        if (s == null || s.length() <= MAX_LOG_LINE_LENGTH) {
            return s;
        }
        return s.substring(0, MAX_LOG_LINE_LENGTH) + "... (truncated, see `outputs`)";
    }

    static String stringifyResult(Object result) {
        if (result == null) {
            return "{}";
        }

        try {
            return JacksonMapper.ofJson().writeValueAsString(result);
        } catch (Exception e) {
            return String.valueOf(result);
        }
    }

    public enum OutputsMode {
        ALL,
        EXPLICIT
    }

    public enum LogsMode {
        SUMMARY,
        FULL
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
