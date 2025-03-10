package io.kestra.plugin.ansible.cli;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class AnsibleCLITest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storage;

    @Test
    @SuppressWarnings("unchecked")
    void run() throws Exception {
        String envKey = "MY_KEY";
        String envValue = "MY_VALUE";

        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(DockerOptions.builder()
                .image("cytopia/ansible:latest-tools")
                .entryPoint(Collections.emptyList())
                .build())
            .env(new Property<>(JacksonMapper.ofJson().writeValueAsString(Map.of("{{ inputs.envKey }}", "{{ inputs.envValue }}"))))
            .beforeCommands(new Property<>(JacksonMapper.ofJson().writeValueAsString(List.of("echo {{ workingDir }}"))))
            .commands(new Property<>(JacksonMapper.ofJson().writeValueAsString(List.of(
                "echo \"::{\\\"outputs\\\":{" +
                    "\\\"customEnv\\\":\\\"$" + envKey + "\\\"" +
                    "}}::\"",
                "ansible --version",
                "ansible-galaxy collection list | tr -d ' \n' | xargs -0 -I {} echo '::{\"outputs\":{}}::'",
                "echo {{ workingDir }}"))))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of("envKey", envKey, "envValue", envValue));

        ScriptOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        assertThat(runOutput.getVars().get("customEnv"), is(envValue));
    }

    @Test
    @SuppressWarnings("unchecked")
    void run_withPlugin() throws Exception {
        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(DockerOptions.builder()
                .image("cytopia/ansible:latest-tools")
                .entryPoint(Collections.emptyList())
                .build())
            .inputFiles(Map.of(
                "playbook.yml", storage.put(null, null, URI.create("/" + IdUtils.create() + ".ion"), this.getClass().getClassLoader().getResourceAsStream("playbook.yml")).toString()
            ))
            .commands(new Property<>(JacksonMapper.ofJson().writeValueAsString(List.of(
                //"ansible --version"
                "ansible-playbook -i localhost -c local playbook.yml"
            ))))
            .outputLogFile(Property.of(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        ScriptOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        assertThat(runOutput.getOutputFiles().size(), is(1));
        assertThat(runOutput.getOutputFiles().get("log"), is(notNullValue()));

        //Get outputs for 6 tasks
        List<Map<String, Object>> outputs = ((List<Map<String, Object>>) runOutput.getVars().get("outputs"));
        assertThat(outputs.size(), is(6));

        //Verify output via 'withItems' (First task)
        List<String> resultMessage = ((List<Map<String, Object>>) outputs.getFirst().get("results"))
            .stream()
            .map(map -> (String ) map.get("msg"))
            .toList();
        assertThat(resultMessage.size(), is(2));
        assertThat(resultMessage, containsInAnyOrder("another_variable", "a_variable"));

        //Verify output via variable (Fourth task)
        String resultFromVar = (String) ((Map<String, Object>) outputs.get(3).get("myOutput")).get("stdout");
        assertThat(resultFromVar, is("Test output"));

        //verify output list message (5th task)
        List<String> messages = (List<String>) outputs.get(4).get("msg");
        assertThat(messages.size(), is(2));
        assertThat(messages, containsInAnyOrder("Multiline message : line 1", "Multiline message : line 2"));

        //verify output list message (6th task)
        List<String> additionalMessages = (List<String>) outputs.get(5).get("msg");
        assertThat(additionalMessages.size(), is(2));
        assertThat(additionalMessages, containsInAnyOrder("Multiline message : line 3", "Multiline message : line 4"));
    }
}