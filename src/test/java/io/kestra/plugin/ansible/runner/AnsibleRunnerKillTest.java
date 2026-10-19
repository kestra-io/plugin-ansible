package io.kestra.plugin.ansible.runner;

import org.junit.jupiter.api.Test;

import io.kestra.plugin.scripts.runner.docker.Docker;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class AnsibleRunnerKillTest {

    @Test
    void killDelegatesToTaskRunner() {
        var taskRunner = mock(Docker.class);
        var task = AnsibleRunner.builder()
            .id("kill")
            .type(AnsibleRunner.class.getName())
            .taskRunner(taskRunner)
            .build();

        task.kill();

        verify(taskRunner).kill();
    }

    @Test
    void killWithoutTaskRunnerIsNoop() {
        var task = AnsibleRunner.builder()
            .id("kill")
            .type(AnsibleRunner.class.getName())
            .taskRunner(null)
            .build();

        assertDoesNotThrow(task::kill);
    }
}
