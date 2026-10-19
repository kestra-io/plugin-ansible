package io.kestra.plugin.ansible.runner;

import org.junit.jupiter.api.Test;

import io.kestra.plugin.scripts.runner.docker.Docker;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class AnsibleRunnerKillTest {

    @Test
    void killDelegatesToTaskRunner() throws Exception {
        var taskRunner = mock(Docker.class);
        var task = AnsibleRunner.builder().id("kill").type(AnsibleRunner.class.getName()).build();
        var field = AnsibleRunner.class.getDeclaredField("taskRunner");
        field.setAccessible(true);
        field.set(task, taskRunner);

        task.kill();

        verify(taskRunner).kill();
    }

    @Test
    void killWithoutTaskRunnerIsNoop() {
        var task = AnsibleRunner.builder().id("kill").type(AnsibleRunner.class.getName()).build();
        task.kill();
        assertThat(task != null, is(true));
    }
}
