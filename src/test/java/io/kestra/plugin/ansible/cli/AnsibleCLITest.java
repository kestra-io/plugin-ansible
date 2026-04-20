package io.kestra.plugin.ansible.cli;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;

import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.plugin.scripts.runner.docker.PullPolicy;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.assets.AssetIdentifier;
import io.kestra.core.models.assets.AssetsDeclaration;
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
import reactor.core.publisher.Flux;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class AnsibleCLITest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storage;

    @Inject
    private TestAssetManagerFactory assetManagerFactory;

    @Test
    void extractInventoryAssetInputs_shouldParseHostsAsInputsOnly() {
        var inventory = """
            [webservers]
            web1.example.com ansible_user=ubuntu
            web2.example.com

            [webservers:vars]
            ansible_port=22

            [parents:children]
            webservers

            [ungrouped]
            standalone-host # inline comment
            ; full line comment
            invalid:host
            """;

        var inputs = AnsibleCLI.extractInventoryAssetInputs(inventory);

        assertThat(inputs.size(), is(3));
        assertThat(
            inputs.stream().map(AssetIdentifier::id).toList(), contains(
                "web1.example.com",
                "web2.example.com",
                "standalone-host"
            )
        );
        assertThat(
            inputs.stream().map(AssetIdentifier::type).distinct().toList(), contains(
                "io.kestra.plugin.ee.assets.VM"
            )
        );
    }

    @Test
    void extractInventoryAssetInputs_shouldIgnoreEmptyOrVarsOnlyInventory() {
        var inventory = """
            [all:vars]
            ansible_user=ubuntu
            """;

        var inputs = AnsibleCLI.extractInventoryAssetInputs(inventory);

        assertThat(inputs, is(empty()));
    }

    @Test
    @SuppressWarnings("unchecked")
    void run() throws Exception {
        String envKey = "MY_KEY";
        String envValue = "MY_VALUE";

        AnsibleCLI execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .env(Property.ofExpression(JacksonMapper.ofJson().writeValueAsString(Map.of("{{ inputs.envKey }}", "{{ inputs.envValue }}"))))
            .beforeCommands(Property.ofExpression(JacksonMapper.ofJson().writeValueAsString(List.of("echo {{ workingDir }}"))))
            .commands(
                Property.ofExpression(
                    JacksonMapper.ofJson().writeValueAsString(
                        List.of(
                            "echo \"::{\\\"outputs\\\":{" +
                                "\\\"customEnv\\\":\\\"$" + envKey + "\\\"" +
                                "}}::\"",
                            "ansible --version",
                            "ansible-galaxy collection list | tr -d ' \n' | xargs -0 -I {} echo '::{\"outputs\":{}}::'",
                            "echo {{ workingDir }}"
                        )
                    )
                )
            )
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
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .inputFiles(
                Map.of(
                    "playbooks/playbook.yml",
                    storage.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), this.getClass().getClassLoader().getResourceAsStream("playbooks/playbook.yml"))
                        .toString()
                )
            )
            .commands(
                Property.ofValue(
                    List.of(
                        "ansible --version",
                        "ansible-playbook -i localhost -c local playbooks/playbook.yml"
                    )
                )
            )
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
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .inputFiles(
                Map.of(
                    "playbooks/playbook.yml", storage.put(
                        TenantService.MAIN_TENANT,
                        null,
                        URI.create("/" + IdUtils.create() + ".ion"),
                        this.getClass().getClassLoader().getResourceAsStream("playbooks/playbook.yml")
                    ).toString()
                )
            )
            .commands(
                Property.ofValue(
                    List.of(
                        "ansible-playbook -i localhost -c local playbooks/playbook.yml"
                    )
                )
            )
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
        assertThat(
            play0.getName(), anyOf(
                is("localhost"),
                is("unnamed_play"),
                is("implicit_play")
            )
        );

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
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .inputFiles(
                Map.of(
                    "inventory.ini", inventoryUri.toString(),
                    "playbook.yml", playbookUri.toString()
                )
            )
            .commands(
                Property.ofValue(
                    List.of(
                        "ansible-playbook -i inventory.ini -c local playbook.yml"
                    )
                )
            )
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
        assertThat(
            List.of(t1h0.get("msg"), t1h1.get("msg")),
            everyItem(is("Hello from task 1"))
        );

        // Task 2 message per host
        Map<String, Object> t2h0 = (Map<String, Object>) tasks.get(1).getHosts().get(0).getResult();
        Map<String, Object> t2h1 = (Map<String, Object>) tasks.get(1).getHosts().get(1).getResult();
        assertThat(
            List.of(t2h0.get("msg"), t2h1.get("msg")),
            everyItem(is("Hello from task 2"))
        );
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
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .inputFiles(
                Map.of(
                    // first playbook (single host)
                    "playbooks/playbook.yml", pb1Uri.toString(),
                    // second playbook (multi-host)
                    "playbooks/playbook_with_multiple_hosts.yml", pb2Uri.toString(),
                    // inventory for second playbook
                    "inventory.ini", inventoryUri.toString()
                )
            )
            // Two distinct ansible-playbook commands => triggers multi-command behavior
            .commands(
                Property.ofValue(
                    List.of(
                        "ansible-playbook -i localhost -c local playbooks/playbook.yml",
                        "ansible-playbook -i inventory.ini -c local playbooks/playbook_with_multiple_hosts.yml"
                    )
                )
            )
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
        List<Map<String, Object>> outputs = (List<Map<String, Object>>) runOutput.getVars().get("outputs");
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

    @Test
    void run_withAutoAssets_inventory() throws Exception {
        assetManagerFactory.reset();

        var inventory = """
            [webservers]
            web1.example.com
            web2.example.com

            [databases]
            db1.example.com
            """;

        var inventoryUri = storage.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new java.io.ByteArrayInputStream(inventory.getBytes(java.nio.charset.StandardCharsets.UTF_8))
        );

        var execute = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .assets(new AssetsDeclaration(true, null, null))
            .inputFiles(
                Map.of(
                    "inventory.ini", inventoryUri.toString()
                )
            )
            .commands(Property.ofValue(List.of("echo noop")))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        execute.run(runContext);

        var emitted = assetManagerFactory.emitter().emitted();
        assertThat(emitted.size(), is(1));

        var firstEmit = emitted.getFirst();
        assertThat(firstEmit.outputs(), empty());
        assertThat(firstEmit.inputs().size(), is(3));

        var inputIds = firstEmit.inputs().stream()
            .map(AssetIdentifier::id)
            .toList();
        assertThat(
            inputIds, containsInAnyOrder(
                "web1.example.com",
                "web2.example.com",
                "db1.example.com"
            )
        );

        for (var input : firstEmit.inputs()) {
            assertThat(input.type(), is("io.kestra.plugin.ee.assets.VM"));
        }
    }

    @Test
    void run_withAutoAssets_inventory_shouldNotReEmitExistingHosts() throws Exception {
        assetManagerFactory.reset();

        var firstInventory = """
            [webservers]
            web1.example.com ansible_user=ubuntu
            web2.example.com

            [webservers:vars]
            ansible_port=22

            [ungrouped]
            standalone-host
            web1.example.com # duplicate should be deduped
            invalid:host
            """;

        var secondInventory = """
            [webservers]
            web1.example.com
            web2.example.com

            [ungrouped]
            standalone-host
            """;

        var firstInventoryUri = storage.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new java.io.ByteArrayInputStream(firstInventory.getBytes(java.nio.charset.StandardCharsets.UTF_8))
        );
        var secondInventoryUri = storage.put(
            TenantService.MAIN_TENANT,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new java.io.ByteArrayInputStream(secondInventory.getBytes(java.nio.charset.StandardCharsets.UTF_8))
        );

        var firstTask = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .assets(new AssetsDeclaration(true, null, null))
            .inputFiles(
                Map.of(
                    "inventory.ini", firstInventoryUri.toString()
                )
            )
            .commands(Property.ofValue(List.of("echo first run")))
            .build();

        var secondTask = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .docker(
                DockerOptions.builder()
                    .image("cytopia/ansible:latest-tools")
                    .entryPoint(Collections.emptyList())
                    .build()
            )
            .assets(new AssetsDeclaration(true, null, null))
            .inputFiles(
                Map.of(
                    "inventory.ini", secondInventoryUri.toString()
                )
            )
            .commands(Property.ofValue(List.of("echo second run")))
            .build();

        firstTask.run(TestsUtils.mockRunContext(runContextFactory, firstTask, Map.of()));
        secondTask.run(TestsUtils.mockRunContext(runContextFactory, secondTask, Map.of()));

        var emitted = assetManagerFactory.emitter().emitted();
        assertThat(emitted.size(), is(1));
        assertThat(emitted.getFirst().inputs().size(), is(3));
    }

    @Test
    void shouldReproduceVaultSerializationBug() throws Exception {

        AnsibleCLI task = AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName())
            .beforeCommands(Property.ofValue(List.of(
                "pip install --default-timeout=60 \"ansible>=9,<10\""
            )))
            .inputFiles(Map.of(

                "inventory.ini",
                """
                [target]
                localhost ansible_connection=local
                """,

                "vault_vars.yml",
                """
                vault_secret_value: !vault |
                  $ANSIBLE_VAULT;1.1;AES256
                  35363665656162366638396161616466313965383938313366633734306266633433333265313862
                  3633646532336664623966666663386531363262336638360a363033373931376432393761613163
                  35346336383665626335346134613638663561373230616631623538313636306332383431363637
                  6665623938653066390a396563323430326335303164626661623064313234333633313431613666
                  65666131633031653630393066383663373630666532383164303837663735393030
                """,

                "playbook.yml",
                """
                ---
                - name: Reproduce vault serialization bug
                  hosts: target
                  gather_facts: false
                  tasks:
                    - name: Hello world
                      ansible.builtin.debug:
                        msg: Hello!!

                    - name: Load vault-encrypted vars at runtime
                      ansible.builtin.include_vars:
                        file: vault_vars.yml

                    - name: Use vault-encrypted variable in a task
                      ansible.builtin.debug:
                        msg: "Vault value is {{ '{{' }} vault_secret_value {{ '}}' }}"

                    - name: Set fact from vault-encrypted variable
                      ansible.builtin.set_fact:
                        derived_value: "{{ '{{' }} vault_secret_value {{ '}}' }}"

                    - name: Use derived vault fact
                      ansible.builtin.debug:
                        msg: "Derived value is {{ '{{' }} derived_value {{ '}}' }}"
                """
            ))
            .commands(Property.ofValue(List.of(
                "echo \"bugreport\" > /tmp/vault_pass.txt && ansible-playbook -i inventory.ini --vault-password-file /tmp/vault_pass.txt playbook.yml"
            )))

            .taskRunner(
                io.kestra.plugin.scripts.runner.docker.Docker.builder()
                    .type(io.kestra.plugin.scripts.runner.docker.Docker.class.getName())
                    .image("python:3.12-trixie")
                    .pullPolicy(Property.ofValue(PullPolicy.IF_NOT_PRESENT))
                    .build()
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        AnsibleCLI.AnsibleOutput output = task.run(runContext);

        assertThat(output.getExitCode(), is(0));
        assertThat(output.getPlaybooks(), is(notNullValue()));
        assertThat(output.getPlaybooks().getFirst().getPlays(), is(notNullValue()));

    }
}
