# Kestra Ansible Plugin

## What

- Provides plugin components under `io.kestra.plugin.ansible.cli`.
- Includes classes such as `AnsibleCLI`.

## Why

- What user problem does this solve? Teams need to orchestrate Ansible playbooks from Kestra flows from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Ansible steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Ansible.

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
