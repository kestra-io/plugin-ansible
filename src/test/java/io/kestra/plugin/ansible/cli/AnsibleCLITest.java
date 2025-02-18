package io.kestra.plugin.ansible.cli;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class AnsibleCLITest {

    @Inject
    private RunContextFactory runContextFactory;

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
            .env(Map.of("{{ inputs.envKey }}", "{{ inputs.envValue }}"))
            .beforeCommands(TestsUtils.propertyFromList(List.of("echo {{ workingDir }}")))
            .commands(TestsUtils.propertyFromList(List.of(
                "echo \"::{\\\"outputs\\\":{" +
                    "\\\"customEnv\\\":\\\"$" + envKey + "\\\"" +
                    "}}::\"",
                "ansible --version",
                "ansible-galaxy collection list | tr -d ' \n' | xargs -0 -I {} echo '::{\"outputs\":{}}::'",
                "echo {{ workingDir }}"
            )))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of("envKey", envKey, "envValue", envValue));

        ScriptOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        assertThat(runOutput.getVars().get("customEnv"), is(envValue));
    }
}