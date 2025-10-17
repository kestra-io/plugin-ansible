package io.kestra.plugin.ansible.cli;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.exceptions.ResourceExpiredException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.*;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.models.tasks.runners.TaskRunnerResult;
import io.kestra.core.runners.FilesService;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.kv.KVValue;
import io.kestra.core.storages.kv.KVValueAndMetadata;
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
import java.util.*;
import java.util.stream.Stream;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute an Ansible command."
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
                      - ansible-playbook -i inventory.ini myplaybook.yml"""
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
                      - ansible-playbook -i inventory.ini myplaybook.yml"""
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
public class AnsibleCLI extends Task implements RunnableTask<ScriptOutput>, NamespaceFilesInterface, InputFilesInterface, OutputFilesInterface {
    private static final String DEFAULT_IMAGE = "cytopia/ansible:latest-tools";
    private static final String ANSIBLE_CFG = "ansible.cfg";
    private static final String PLUGINS_KESTRA_LOGGER_PY = "callback_plugins/kestra_logger.py";

    @Schema(
        title = "The commands to run before the main list of commands"
    )
    protected Property<List<String>> beforeCommands;

    @Schema(
        title = "The commands to run"
    )
    @NotNull
    protected Property<List<String>> commands;

    @Schema(
        title = "Additional environment variables for the current process"
    )
    protected Property<Map<String, String>> env;

    @Schema(
        title = "Deprecated, use 'taskRunner' instead"
    )
    @PluginProperty
    @Deprecated
    private DockerOptions docker;

    @Schema(
        title = "The task runner to use",
        description = "Task runners are provided by plugins, each have their own properties."
    )
    @PluginProperty
    @Builder.Default
    @Valid
    protected TaskRunner<?> taskRunner = Docker.instance();

    @Schema(title = "The task runner container image, only used if the task runner is container-based.")
    @Builder.Default
    protected Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    @Schema(
        title = "Ansible configuration.",
        description = """
            If not provided an ansible.cfg file will be created at the working directory base and will enable a custom callback plugin to output your results to Kestra.
            If you want to provide your own ansible configuration but still want to use the output callback for Kestra add the following lines to your configuration :
            ```
            [defaults]
            log_path={{ workingDir }}/log
            callback_plugins = ./callback_plugins
            stdout_callback = kestra_logger
            ```"""
    )
    @Builder.Default
    protected Property<String> ansibleConfig = Property.ofExpression("""
        [defaults]
        log_path={{ workingDir }}/log
        callback_plugins = ./callback_plugins
        stdout_callback = kestra_logger""");

    @Schema(
        title = "Output log file",
        description = "If true, the ansible log file will be available in the outputs."
    )
    @Builder.Default
    private Property<Boolean> outputLogFile = Property.ofValue(false);

    @Schema(
        title = "Reuse the same Ansible container across tasks",
        description = "If true, the container will be created once and reused for all AnsibleCLI tasks in the same flow execution."
    )
    @Builder.Default
    protected Property<Boolean> reuseContainer = Property.ofValue(false);

    private static final String KV_CONTAINER_KEY = "ansible_container_id";

    private NamespaceFiles namespaceFiles;

    private Object inputFiles;

    private Property<List<String>> outputFiles;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        List<String> outputFilesList = new ArrayList<>();
        outputFilesList.addAll(runContext.render(this.outputFiles).asList(String.class));

        if (Boolean.TRUE.equals(runContext.render(this.outputLogFile).as(Boolean.class).orElseThrow())) {
            outputFilesList.add("log");
        }

        var renderedEnvMap = runContext.render(this.env).asMap(String.class, String.class);

        CommandsWrapper commandsWrapper = new CommandsWrapper(runContext)
            .withWarningOnStdErr(false)
            .withDockerOptions(injectDefaults(docker))
            .withTaskRunner(this.taskRunner)
            .withContainerImage(runContext.render(this.containerImage).as(String.class).orElseThrow())
            .withInterpreter(Property.ofValue(List.of("/bin/bash", "-c")))
            .withBeforeCommands(this.beforeCommands)
            .withCommands(this.commands)
            .withEnv(renderedEnvMap.isEmpty() ? new HashMap<>() : renderedEnvMap)
            .withNamespaceFiles(namespaceFiles)
            .withEnableOutputDirectory(true)
            .withOutputFiles(outputFilesList);

        Path workingDir = commandsWrapper.getWorkingDirectory();

        PluginUtilsService.createInputFiles(
            runContext,
            workingDir,
            this.finalInputFiles(runContext, workingDir),
            this.taskRunner.additionalVars(runContext, commandsWrapper)
        );

        Optional<String> reusedContainerId = Optional.empty();
        Boolean rReuseContainer = runContext.render(this.reuseContainer).as(Boolean.class).orElse(false);
        if (rReuseContainer) {
            reusedContainerId = runContext.namespaceKv(runContext.flowInfo().namespace())
                .getValue(KV_CONTAINER_KEY)
                .map(KVValue::value)
                .map(Object::toString);
        }

        ScriptOutput out;
        if (reusedContainerId.isPresent() && this.taskRunner instanceof Docker dockerRunner) {
            String containerId = reusedContainerId.get();
            runContext.logger().info("Reusing existing Ansible container: {}", containerId);

            TaskRunnerResult<Docker.DockerTaskRunnerDetailResult> result =
                dockerRunner.execInContainer(runContext, containerId, commandsWrapper);

            Map<String, URI> outputFiles = new HashMap<>();
            if (commandsWrapper.getEnableOutputDirectory()) {
                outputFiles.putAll(ScriptService.uploadOutputFiles(runContext, commandsWrapper.getOutputDirectory()));
            }
            if (commandsWrapper.getOutputFiles() != null) {
                outputFiles.putAll(FilesService.outputFiles(runContext, commandsWrapper.getOutputFiles()));
            }

            out = ScriptOutput.builder()
                .exitCode(result.getExitCode())
                .taskRunner(result.getDetails())
                .vars(result.getLogConsumer().getOutputs())
                .stdOutLineCount(result.getLogConsumer().getStdOutCount())
                .stdErrLineCount(result.getLogConsumer().getStdErrCount())
                .outputFiles(outputFiles)
                .build();
        } else {
            out = commandsWrapper.run();

            if (rReuseContainer && out.getTaskRunner() instanceof Docker.DockerTaskRunnerDetailResult dockerResult) {
                runContext.namespaceKv(runContext.flowInfo().namespace())
                    .put(KV_CONTAINER_KEY, new KVValueAndMetadata(null, dockerResult.getContainerId()));
                runContext.logger().info("Stored Ansible container ID {} for reuse", dockerResult.getContainerId());
            }
        }

        Path logFile = workingDir.resolve("log");
        if (Files.exists(logFile)) {
            try (Stream<String> lines = Files.lines(logFile)) {
                lines.filter(line -> line.startsWith("::") && line.endsWith("::"))
                    .forEach(line -> {
                        String json = line.substring(2, line.length() - 2);
                        try {
                            Map<String, Object> event = JacksonMapper.toMap(json);
                            runContext.logger().info("Ansible event: {}", event);
                        } catch (Exception e) {
                            runContext.logger().warn("Failed to parse ansible event: {}", json, e);
                        }
                    });
            }
        }

        return out;
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

    private Optional<String> getReusedContainerId(RunContext runContext) throws IOException, ResourceExpiredException {
        return runContext.namespaceKv(runContext.flowInfo().namespace())
            .getValue(KV_CONTAINER_KEY)
            .map(KVValue::value)
            .map(Object::toString);
    }

    private void storeContainerId(RunContext runContext, String containerId) throws IOException {
        runContext.namespaceKv(runContext.flowInfo().namespace())
            .put(KV_CONTAINER_KEY, new KVValueAndMetadata(null, containerId));
    }

    private void clearContainerId(RunContext runContext) throws IOException {
        runContext.namespaceKv(runContext.flowInfo().namespace())
            .delete(KV_CONTAINER_KEY);
    }
}
