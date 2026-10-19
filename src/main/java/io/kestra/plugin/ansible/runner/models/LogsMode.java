package io.kestra.plugin.ansible.runner.models;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Controls logging verbosity mode during Ansible Runner execution.")
public enum LogsMode {
    @Schema(description = "Streams all stdout and stderr lines emitted by Ansible Runner.")
    FULL,

    @Schema(description = "Streams high-level playbook progression (plays, tasks, status changes, recap, errors) and filters out verbose debug chatter.")
    SUMMARY
}
