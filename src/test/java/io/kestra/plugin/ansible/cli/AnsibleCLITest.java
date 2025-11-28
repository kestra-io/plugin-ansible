package io.kestra.plugin.ansible.cli;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
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
            .env(Property.ofExpression(JacksonMapper.ofJson().writeValueAsString(Map.of("{{ inputs.envKey }}", "{{ inputs.envValue }}"))))
            .beforeCommands(Property.ofExpression(JacksonMapper.ofJson().writeValueAsString(List.of("echo {{ workingDir }}"))))
            .commands(Property.ofExpression(JacksonMapper.ofJson().writeValueAsString(List.of(
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
                "playbooks/playbook.yml", storage.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), this.getClass().getClassLoader().getResourceAsStream("playbooks/playbook.yml")).toString()
            ))
            .commands(new Property<>(JacksonMapper.ofJson().writeValueAsString(List.of(
                "ansible --version",
                "ansible-playbook -i localhost -c local playbooks/playbook.yml"
            ))))
            .outputLogFile(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        ScriptOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
        assertThat(runOutput.getOutputFiles().size(), is(1));
        assertThat(runOutput.getOutputFiles().get("log"), is(notNullValue()));

        // Get outputs for 6 tasks
        List<Map<String, Object>> outputs = ((List<Map<String, Object>>) runOutput.getVars().get("outputs"));
        assertThat(outputs.size(), is(6));

        // Verify output via 'withItems' (First task)
        List<String> resultMessage = ((List<Map<String, Object>>) outputs.getFirst().get("results"))
            .stream()
            .map(map -> (String) map.get("msg"))
            .toList();
        assertThat(resultMessage.size(), is(2));
        assertThat(resultMessage, containsInAnyOrder("another_variable", "a_variable"));

        // Verify output via variable (Fourth task)
        String resultFromVar = (String) ((Map<String, Object>) outputs.get(3).get("myOutput")).get("stdout");
        assertThat(resultFromVar, is("Test output"));

        // Verify output list message (5th task)
        List<String> messages = (List<String>) outputs.get(4).get("msg");
        assertThat(messages.size(), is(2));
        assertThat(messages, containsInAnyOrder("Multiline message : line 1", "Multiline message : line 2"));

        // Verify output list message (6th task)
        List<String> additionalMessages = (List<String>) outputs.get(5).get("msg");
        assertThat(additionalMessages.size(), is(2));
        assertThat(additionalMessages, containsInAnyOrder("Multiline message : line 3", "Multiline message : line 4"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void run_withStructuredOutputs() throws Exception {
        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(DockerOptions.builder()
                .image("cytopia/ansible:latest-tools")
                .entryPoint(Collections.emptyList())
                .build())
            .inputFiles(Map.of(
                "playbooks/playbook.yml", storage.put(
                    TenantService.MAIN_TENANT,
                    null,
                    URI.create("/" + IdUtils.create() + ".ion"),
                    this.getClass().getClassLoader().getResourceAsStream("playbooks/playbook.yml")
                ).toString()
            ))
            .commands(new Property<>(
                JacksonMapper.ofJson().writeValueAsString(List.of(
                    "ansible-playbook -i localhost -c local playbooks/playbook.yml"
                ))
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        AnsibleCLI.AnsibleOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));

        List<AnsibleCLI.AnsibleOutput.PlaybookOutput> playbooks = runOutput.getPlaybooks();
        assertThat(playbooks, is(notNullValue()));
        assertThat(playbooks.size(), is(1));

        AnsibleCLI.AnsibleOutput.PlaybookOutput pb0 = playbooks.getFirst();
        assertThat(pb0.getPlays(), is(notNullValue()));
        assertThat(pb0.getPlays().size(), is(1));

        AnsibleCLI.AnsibleOutput.PlayOutput play0 = pb0.getPlays().getFirst();
        // no explicit name in playbook.yml => ansible uses hosts pattern as play name
        assertThat(play0.getName(), anyOf(
            is("localhost"),
            is("unnamed_play"),
            is("implicit_play")
        ));

        assertThat(play0.getTasks(), is(notNullValue()));
        assertThat(play0.getTasks().size(), is(6));

        List<AnsibleCLI.AnsibleOutput.TaskOutput> tasks = play0.getTasks();
        assertThat(tasks.get(0).getName(), is("Print items"));
        assertThat(tasks.get(1).getName(), is("Create file"));
        assertThat(tasks.get(2).getName(), is("Register output file to var"));
        assertThat(tasks.get(3).getName(), is("Print return information from the previous task"));
        assertThat(tasks.get(4).getName(), is("Prints two lines of messages"));
        assertThat(tasks.get(5).getName(), is("Prints two other lines of messages"));

        for (AnsibleCLI.AnsibleOutput.TaskOutput t : tasks) {
            assertThat(t.getHosts(), is(notNullValue()));
            assertThat(t.getHosts().size(), is(1));
            assertThat(t.getHosts().getFirst().getHost(), is("localhost"));
            assertThat(t.getHosts().getFirst().getStatus(), is("ok"));
            assertThat(t.getHosts().getFirst().getResult(), is(instanceOf(Map.class)));
        }

        // Task 1: loop -> results msgs
        Map<String, Object> t1res = (Map<String, Object>) tasks.get(0).getHosts().getFirst().getResult();
        List<Map<String, Object>> results = (List<Map<String, Object>>) t1res.get("results");
        List<String> resultMessage = results.stream()
            .map(m -> (String) m.get("msg"))
            .toList();
        assertThat(resultMessage.size(), is(2));
        assertThat(resultMessage, containsInAnyOrder("another_variable", "a_variable"));

        // Task 4: debug var myOutput -> myOutput.stdout
        Map<String, Object> t4res = (Map<String, Object>) tasks.get(3).getHosts().getFirst().getResult();
        String resultFromVar = (String) ((Map<String, Object>) t4res.get("myOutput")).get("stdout");
        assertThat(resultFromVar, is("Test output"));

        // Task 5: debug msg list
        Map<String, Object> t5res = (Map<String, Object>) tasks.get(4).getHosts().getFirst().getResult();
        List<String> messages = (List<String>) t5res.get("msg");
        assertThat(messages.size(), is(2));
        assertThat(messages, containsInAnyOrder("Multiline message : line 1", "Multiline message : line 2"));

        // Task 6: debug msg list
        Map<String, Object> t6res = (Map<String, Object>) tasks.get(5).getHosts().getFirst().getResult();
        List<String> additionalMessages = (List<String>) t6res.get("msg");
        assertThat(additionalMessages.size(), is(2));
        assertThat(additionalMessages, containsInAnyOrder("Multiline message : line 3", "Multiline message : line 4"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void run_withStructuredOutputs_multipleHosts() throws Exception {
        // inventory with 2 hosts
        String inventory = """
            [local_servers]
            localhost1 ansible_connection=local
            localhost2 ansible_connection=local
            """;

        // playbook from test resources
        URI inventoryUri = storage.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new java.io.ByteArrayInputStream(inventory.getBytes(java.nio.charset.StandardCharsets.UTF_8))
        );

        URI playbookUri = storage.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            this.getClass().getClassLoader().getResourceAsStream("playbooks/playbook_with_multiple_hosts.yml")
        );

        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(DockerOptions.builder()
                .image("cytopia/ansible:latest-tools")
                .entryPoint(Collections.emptyList())
                .build())
            .inputFiles(Map.of(
                "inventory.ini", inventoryUri.toString(),
                "playbook.yml", playbookUri.toString()
            ))
            .commands(new Property<>(
                JacksonMapper.ofJson().writeValueAsString(List.of(
                    "ansible-playbook -i inventory.ini -c local playbook.yml"
                ))
            ))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        AnsibleCLI.AnsibleOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));

        List<AnsibleCLI.AnsibleOutput.PlaybookOutput> playbooks = runOutput.getPlaybooks();
        assertThat(playbooks, is(notNullValue()));
        assertThat(playbooks.size(), is(1));

        AnsibleCLI.AnsibleOutput.PlaybookOutput pb0 = playbooks.getFirst();
        assertThat(pb0.getPlays(), is(notNullValue()));
        assertThat(pb0.getPlays().size(), is(1));

        AnsibleCLI.AnsibleOutput.PlayOutput play0 = pb0.getPlays().getFirst();
        assertThat(play0.getName(), is("Hello World Playbook"));

        assertThat(play0.getTasks(), is(notNullValue()));
        assertThat(play0.getTasks().size(), is(2));

        List<AnsibleCLI.AnsibleOutput.TaskOutput> tasks = play0.getTasks();
        assertThat(tasks.get(0).getName(), is("Task 1"));
        assertThat(tasks.get(1).getName(), is("Task 2"));

        // Each task should have 2 host results
        for (AnsibleCLI.AnsibleOutput.TaskOutput t : tasks) {
            assertThat(t.getHosts(), is(notNullValue()));
            assertThat(t.getHosts().size(), is(2));

            List<String> hosts = t.getHosts().stream()
                .map(AnsibleCLI.AnsibleOutput.HostResult::getHost)
                .toList();
            assertThat(hosts, containsInAnyOrder("localhost1", "localhost2"));

            for (AnsibleCLI.AnsibleOutput.HostResult hr : t.getHosts()) {
                assertThat(hr.getStatus(), is("ok"));
                assertThat(hr.getResult(), is(instanceOf(Map.class)));
            }
        }

        // Task 1 message per host
        Map<String, Object> t1h0 = (Map<String, Object>) tasks.get(0).getHosts().get(0).getResult();
        Map<String, Object> t1h1 = (Map<String, Object>) tasks.get(0).getHosts().get(1).getResult();
        assertThat(List.of(t1h0.get("msg"), t1h1.get("msg")),
            everyItem(is("Hello from task 1")));

        // Task 2 message per host
        Map<String, Object> t2h0 = (Map<String, Object>) tasks.get(1).getHosts().get(0).getResult();
        Map<String, Object> t2h1 = (Map<String, Object>) tasks.get(1).getHosts().get(1).getResult();
        assertThat(List.of(t2h0.get("msg"), t2h1.get("msg")),
            everyItem(is("Hello from task 2")));
    }

    @Test
    @SuppressWarnings("unchecked")
    void run_withStructuredOutputs_multipleCommands_mergesPlaybooksAndLogs() throws Exception {
        // inventory for the multi-host playbook
        String inventory = """
            [local_servers]
            localhost1 ansible_connection=local
            localhost2 ansible_connection=local
            """;

        URI inventoryUri = storage.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new java.io.ByteArrayInputStream(inventory.getBytes(java.nio.charset.StandardCharsets.UTF_8))
        );

        // reuse existing playbooks already used by other tests
        URI pb1Uri = storage.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            this.getClass().getClassLoader().getResourceAsStream("playbooks/playbook.yml")
        );

        URI pb2Uri = storage.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            this.getClass().getClassLoader().getResourceAsStream("playbooks/playbook_with_multiple_hosts.yml")
        );

        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(DockerOptions.builder()
                .image("cytopia/ansible:latest-tools")
                .entryPoint(Collections.emptyList())
                .build())
            .inputFiles(Map.of(
                // first playbook (single host)
                "playbooks/playbook.yml", pb1Uri.toString(),
                // second playbook (multi-host)
                "playbooks/playbook_with_multiple_hosts.yml", pb2Uri.toString(),
                // inventory for second playbook
                "inventory.ini", inventoryUri.toString()
            ))
            // Two distinct ansible-playbook commands => triggers multi-command behavior
            .commands(new Property<>(
                JacksonMapper.ofJson().writeValueAsString(List.of(
                    "ansible-playbook -i localhost -c local playbooks/playbook.yml",
                    "ansible-playbook -i inventory.ini -c local playbooks/playbook_with_multiple_hosts.yml"
                ))
            ))
            .outputLogFile(Property.ofValue(true))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        AnsibleCLI.AnsibleOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));

        // ---------------------------
        // Verify merged "playbooks" in vars
        // ---------------------------
        Object maybePlaybooksVar = runOutput.getVars().get("playbooks");
        assertThat(maybePlaybooksVar, is(instanceOf(List.class)));

        List<Object> playbooksVar = (List<Object>) maybePlaybooksVar;
        assertThat(playbooksVar.size(), is(2));

        // ---------------------------
        // Verify merged raw outputs (backward compatible)
        // ---------------------------
        // playbook.yml => 6 tasks * 1 host = 6 outputs
        // playbook_with_multiple_hosts.yml => 2 tasks * 2 hosts = 4 outputs
        // total = 10
        List<Map<String, Object>> outputs =
            (List<Map<String, Object>>) runOutput.getVars().get("outputs");
        assertThat(outputs, is(notNullValue()));
        assertThat(outputs.size(), is(10));

        // ---------------------------
        // Verify merged structured playbooks
        // ---------------------------
        List<AnsibleCLI.AnsibleOutput.PlaybookOutput> playbooks = runOutput.getPlaybooks();
        assertThat(playbooks, is(notNullValue()));
        assertThat(playbooks.size(), is(2));

        // Playbook 1 (playbooks/playbook.yml)
        AnsibleCLI.AnsibleOutput.PlaybookOutput pb0 = playbooks.get(0);
        assertThat(pb0.getPlays(), is(notNullValue()));
        assertThat(pb0.getPlays().size(), is(1));
        assertThat(pb0.getPlays().getFirst().getTasks().size(), is(6));

        // Playbook 2 (playbooks/playbook_with_multiple_hosts.yml)
        AnsibleCLI.AnsibleOutput.PlaybookOutput pb1 = playbooks.get(1);
        assertThat(pb1.getPlays(), is(notNullValue()));
        assertThat(pb1.getPlays().size(), is(1));
        assertThat(pb1.getPlays().getFirst().getName(), is("Hello World Playbook"));
        assertThat(pb1.getPlays().getFirst().getTasks().size(), is(2));

        // Verify log file content contains traces of BOTH playbooks
        URI logUri = runOutput.getOutputFiles().get("log");
        assertThat(logUri, is(notNullValue()));

        String logContent;
        try (java.io.InputStream is = storage.get(TenantService.MAIN_TENANT, null, logUri)) {
            logContent = new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }

        // From playbook.yml we expect to see task "Print items"
        assertThat(logContent, containsString("\"task\":\"Print items\""));

        // From playbook_with_multiple_hosts.yml we expect to see play "Hello World Playbook"
        assertThat(logContent, containsString("\"play\":\"Hello World Playbook\""));
    }
}