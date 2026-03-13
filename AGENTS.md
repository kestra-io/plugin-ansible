# Kestra Ansible Plugin

## What

Plugin to orchestrate Ansible playbooks with kestra Exposes 1 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Ansible, allowing orchestration of Ansible-based operations as part of data pipelines and automation workflows.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `ansible`

### Key Plugin Classes

- `io.kestra.plugin.ansible.cli.AnsibleCLI`

### Project Structure

```
plugin-ansible/
├── src/main/java/io/kestra/plugin/ansible/cli/
├── src/test/java/io/kestra/plugin/ansible/cli/
├── build.gradle
└── README.md
```

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
