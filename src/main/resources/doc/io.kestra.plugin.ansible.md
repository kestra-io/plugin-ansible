# How to use the Ansible plugin

Run Ansible playbooks and ad-hoc commands from Kestra flows inside a container with the Ansible CLI pre-installed.

## Common properties

`containerImage` defaults to `cytopia/ansible:latest-tools`. `taskRunner` controls where the container runs — defaults to Docker. Pass inventory files, playbooks, and other supporting files via `inputFiles` (inline content) or pull them from [namespace files](https://kestra.io/docs/concepts/namespace-files). Target host credentials (SSH keys, passwords) are supplied through the inventory file using standard Ansible inventory variables rather than plugin-level properties.

## Tasks

`cli.AnsibleCLI` runs one or more Ansible CLI commands set in `commands` (e.g. `ansible-playbook site.yml -i inventory.ini`). Use `beforeCommands` to run setup steps before the main commands, `env` to inject environment variables, and `outputFiles` to capture files produced during execution. Set `ansibleConfig` to supply a custom `ansible.cfg`; if omitted, Kestra generates one automatically with its structured output callback enabled.

## Output capture and custom callbacks

The task captures each playbook's results into `outputs.<taskId>.vars.outputs` and `outputs.<taskId>.vars.playbooks` through a bundled Ansible callback (`kestra_logger`). Ansible loads that callback only if it can find it on its callback search path, so the task adds its bundled callback and module directories to `ANSIBLE_CALLBACK_PLUGINS` and `ANSIBLE_LIBRARY` before running commands. This keeps outputs working even when the playbook is in a subdirectory (e.g. `ansible-playbook ansible/site.yml`) or the runner image sets its own callback path (e.g. ARA-instrumented images).

The callback writes its outputs/playbooks payload to a file in the working directory instead of printing it to stdout, so a large ALL-mode run (many hosts/tasks) never depends on the task runner processing one giant stdout line. `maxOutputsSize` bounds the serialized size of that payload (default 10 MB) and fails the task with an actionable error instead of risking an oversized `WorkerTaskResult`; raise it or switch to `outputsMode: EXPLICIT` if you hit it.

Any value already present on those environment variables is preserved, so your own callbacks or modules keep loading alongside the Kestra ones. If you use your own callbacks or modules, set their paths through the task's `env`:

```yaml
env:
  ANSIBLE_CALLBACK_PLUGINS: "/opt/mycallbacks"   # loads alongside Kestra's
```

Limitation: a callback or module path configured only in a custom `ansible.cfg` (and not via the environment) is superseded by these variables, because in Ansible an environment variable overrides the config file. If you rely on such a path, set it via the task's `env` as shown above so it is preserved.
