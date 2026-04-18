# Kestra Ansible Plugin

## What

- Provides plugin components under `io.kestra.plugin.ansible.cli`.
- Includes classes such as `AnsibleCLI`.

## Why

- This plugin integrates Kestra with Ansible CLI.
- It provides tasks that run Ansible CLI commands to execute playbooks and capture results.

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

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
