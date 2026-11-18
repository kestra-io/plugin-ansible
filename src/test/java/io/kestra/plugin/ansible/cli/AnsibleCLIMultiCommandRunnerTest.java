package io.kestra.plugin.ansible.cli;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.kestra.core.exceptions.KilledException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.runner.docker.Docker;

import jakarta.inject.Inject;
import jakarta.inject.Named;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class AnsibleCLIMultiCommandRunnerTest {
    private static final String ANSIBLE_IMAGE = "cytopia/ansible:latest-tools";

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storage;

    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> logQueue;

    private static AnsibleCLI.AnsibleCLIBuilder<?, ?> base() {
        return AnsibleCLI.builder()
            .id(IdUtils.create())
            .type(AnsibleCLI.class.getName());
    }

    private static String playbook(String playName) {
        return """
            - name: %s
              hosts: localhost
              connection: local
              gather_facts: false
              tasks:
                - name: ping
                  ansible.builtin.ping:
            """.formatted(playName);
    }

    // issue #166: only the default taskRunner path shares one runner across commands, `docker:` bypasses it
    @Test
    void run_multiCommand_withTaskRunnerOnly_runsEveryCommand() throws Exception {
        var task = base()
            .taskRunner(Docker.builder().type(Docker.class.getName()).image(ANSIBLE_IMAGE).entryPoint(Collections.emptyList()).build())
            .inputFiles(Map.of("first.yml", playbook("First play"), "second.yml", playbook("Second play")))
            .commands(
                Property.ofValue(
                    List.of(
                        "ansible-playbook first.yml",
                        "ansible-playbook second.yml"
                    )
                )
            )
            .build();

        var output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getExitCode(), is(0));
        var names = output.getPlaybooks().stream().flatMap(p -> p.getPlays().stream()).map(AnsibleCLI.AnsibleOutput.PlayOutput::getName).toList();
        assertThat(names, contains("First play", "Second play"));

        assertThat(output.getResultsUri(), is(notNullValue()));
        JsonNode stored;
        try (var is = storage.get(TenantService.MAIN_TENANT, null, output.getResultsUri())) {
            stored = JacksonMapper.ofJson().readTree(is);
        }
        assertThat(stored.size(), is(2));
        assertThat(stored.get(0).get("plays").get(0).get("name").asText(), is("First play"));
        assertThat(stored.get(1).get("plays").get(0).get("name").asText(), is("Second play"));
    }

    // the flag is protected in core, read it reflectively
    private static boolean isKilled(TaskRunner<?> runner) throws Exception {
        var field = TaskRunner.class.getDeclaredField("isKilled");
        field.setAccessible(true);
        return ((AtomicBoolean) field.get(runner)).get();
    }

    @Test
    void resolveCommandRunner_returnsAnIndependentCopyEachTime() throws Exception {
        var task = base()
            .taskRunner(Docker.builder().type(Docker.class.getName()).networkMode("custom-net").build())
            .commands(Property.ofValue(List.of("ansible --version")))
            .build();

        var first = task.resolveCommandRunner();
        var second = task.resolveCommandRunner();

        assertThat(first, is(instanceOf(Docker.class)));
        assertThat(first, is(not(sameInstance(task.getTaskRunner()))));
        assertThat(first, is(not(sameInstance(second))));
        assertThat(((Docker) first).getNetworkMode(), is("custom-net"));

        // kill state must not leak to the next command's runner
        first.kill();
        assertThat(isKilled(second), is(false));
        assertThat(isKilled(task.resolveCommandRunner()), is(false));
    }

    @Test
    @SuppressWarnings("deprecation")
    void resolveCommandRunner_deprecatedDockerOptions_getDefaults() throws Exception {
        var task = base()
            .docker(DockerOptions.builder().build())
            .commands(Property.ofValue(List.of("ansible --version")))
            .build();

        var first = task.resolveCommandRunner();

        assertThat(first, is(instanceOf(Docker.class)));
        assertThat(first, is(not(sameInstance(task.resolveCommandRunner()))));
        assertThat(((Docker) first).getImage(), is(ANSIBLE_IMAGE));
        assertThat(((Docker) first).getEntryPoint(), is(List.of("")));
    }

    @Test
    void kill_beforeRun_throwsKilledException() throws Exception {
        var task = base()
            .commands(Property.ofValue(List.of("ansible --version", "ansible --version")))
            .build();
        task.kill();

        assertThrows(KilledException.class, () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of())));
    }

    @Test
    void kill_duringFirstCommand_stopsRunnerAndSkipsNextCommands() throws Exception {
        var logs = new CopyOnWriteArrayList<LogEntry>();
        var receive = TestsUtils.receive(logQueue, l -> logs.add(l.getLeft()));

        // the arithmetic keeps the echoed output distinct from the command text itself
        var task = base()
            .taskRunner(Docker.builder().type(Docker.class.getName()).image(ANSIBLE_IMAGE).entryPoint(Collections.emptyList()).build())
            .commands(Property.ofValue(List.of("echo FIRST_$((1+1)); sleep 20", "echo SECOND_$((1+1))")))
            .build();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        var killedAt = new AtomicLong();
        var killer = new Thread(() ->
        {
            try {
                // the image may have to be pulled first, so give it time to reach the first command
                TestsUtils.awaitLog(logs, l -> l.getMessage() != null && l.getMessage().contains("FIRST_2"));
                killedAt.set(System.nanoTime());
                task.kill();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        killer.start();

        assertThrows(KilledException.class, () -> task.run(runContext));
        // measured from the kill, so a slow image pull doesn't count
        var elapsed = Duration.ofNanos(System.nanoTime() - killedAt.get());
        killer.join(Duration.ofSeconds(10));

        // the sleep lasts 20s, a kill must cut it short
        assertThat(killedAt.get(), is(not(0L)));
        assertThat(elapsed.compareTo(Duration.ofSeconds(15)), is(lessThan(0)));
        assertThat(
            logs.stream().anyMatch(l -> l.getMessage() != null && l.getMessage().contains("SECOND_2")),
            is(false)
        );
        receive.blockLast();
    }
}
