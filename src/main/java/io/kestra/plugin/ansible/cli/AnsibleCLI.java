package io.kestra.plugin.ansible.cli;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.*;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute Ansible command."
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
                    docker:
                      image: cytopia/ansible:latest-tools
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
                    docker:
                      image: cytopia/ansible:latest-tools
                    commands:
                      - ansible-playbook -i inventory.ini myplaybook.yml"""
        ),
        @Example(
            title = "Execute an ansible playbook and use ansible.builtin.debug command to extract outputs.",
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
                    docker:
                      image: cytopia/ansible:latest-tools
                    commands:
                      - ansible-playbook -i localhost -c local playbook.yml
                """
        )
    }
)
public class AnsibleCLI extends Task implements RunnableTask<ScriptOutput>, NamespaceFilesInterface, InputFilesInterface, OutputFilesInterface {
    private static final String DEFAULT_IMAGE = "cytopia/ansible:latest-tools";
    public static final String ANSIBLE_CFG = "ansible.cfg";
    public static final String PLUGINS_KESTRA_LOGGER_PY = "callback_plugins/kestra_logger.py";

    @Schema(
        title = "The commands to run before the main list of commands."
    )
    protected Property<List<String>> beforeCommands;

    @Schema(
        title = "The commands to run."
    )
    @NotNull
    protected Property<List<String>> commands;

    @Schema(
        title = "Additional environment variables for the current process."
    )
    protected Property<Map<String, String>> env;

    @Schema(
        title = "Deprecated, use 'taskRunner' instead"
    )
    @PluginProperty
    @Deprecated
    private DockerOptions docker;

    @Schema(
        title = "The task runner to use.",
        description = "Task runners are provided by plugins, each have their own properties."
    )
    @PluginProperty
    @Builder.Default
    @Valid
    protected TaskRunner<?> taskRunner = Docker.instance();

    @Schema(title = "The task runner container image, only used if the task runner is container-based.")
    @Builder.Default
    protected Property<String> containerImage = Property.of(DEFAULT_IMAGE);

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
    protected Property<String> ansibleConfig = new Property<>("""
        [defaults]
        log_path={{ workingDir }}/log
        callback_plugins = ./callback_plugins
        stdout_callback = kestra_logger""");

    @Schema(
        title = "Output log file.",
        description = "If true, the ansible log file will be available in the outputs."
    )
    @Builder.Default
    private Property<Boolean> outputLogFile = Property.of(false);

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
            .withInterpreter(Property.of(List.of("/bin/bash", "-c")))
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

        return commandsWrapper.run();
    }

    protected Map<String, String> finalInputFiles(RunContext runContext, Path workingDir) throws IOException, IllegalVariableEvaluationException {
        Map<String, String> map = this.inputFiles != null ? new HashMap<>(PluginUtilsService.transformInputFiles(runContext, this.inputFiles)) : new HashMap<>();

        //Add config file if not exists
        if (map.containsKey(ANSIBLE_CFG)) {
            runContext.logger().warn("Found an existing  ansible.cfg file. Ignoring creation of a new ansible.cfg file.");
        } else {
            String config = runContext.render(this.ansibleConfig).as(String.class, Map.of("workingDir", workingDir)).orElseThrow();
            URI uri = runContext.storage().putFile(new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8)), ANSIBLE_CFG);
            map.put(ANSIBLE_CFG, uri.toString());
        }

        //Add python plugin
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

}
