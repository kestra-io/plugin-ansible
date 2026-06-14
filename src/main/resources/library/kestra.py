#!/usr/bin/python
# -*- coding: utf-8 -*-

# General-purpose Kestra integration module for Ansible playbooks.
# Currently supports declaring explicit task outputs; designed so future
# capabilities (e.g. metrics) can be added as new parameters without a
# breaking change, analogous to the Kestra Python library.

from __future__ import annotations

from ansible.module_utils.basic import AnsibleModule

DOCUMENTATION = r"""
---
module: kestra
short_description: Declare data to expose to Kestra from a playbook
description:
  - Integration point between an Ansible playbook and the Kestra orchestrator.
  - The C(outputs) parameter declares key/value pairs that the Kestra Ansible
    plugin captures as task outputs, available downstream via
    C({{ outputs.<taskId>.vars.outputs.<key> }}).
  - When the AnsibleCLI task runs with C(outputsMode: EXPLICIT), only values
    declared through this module are emitted; raw per-host results are
    redacted from outputs and logs.
options:
  outputs:
    description:
      - Arbitrary mapping of output names to JSON-serializable values.
    type: dict
    required: false
notes:
  - C(outputs) is currently the only parameter, so it is required in practice.
    The C(required_one_of) rule exists so future parameters can be added without
    a breaking change, where any one of them would satisfy the requirement.
  - Do not invoke this module with a loop. Ansible aggregates per-item results
    under a C(results) key, so declared outputs would not be collected; call
    the module once with all values instead.
  - On multi-host plays, outputs merge by key with last write winning. Use
    C(run_once) for run-level outputs or key by hostname for per-host values.
author:
  - Kestra (@kestra-io)
"""

EXAMPLES = r"""
- name: Expose only what downstream tasks need
  kestra:
    outputs:
      ad_user_created: "{{ ad_result.changed }}"
      ad_task_status: "{{ 'skipped' if ad_result.skipped | default(false) else 'ok' }}"
      records_updated: "{{ cmdb_result.records | length }}"
"""

RETURN = r"""
outputs:
  description: The declared outputs, echoed back for the Kestra callback to collect.
  type: dict
  returned: when outputs is provided
"""


def main():
    module = AnsibleModule(
        argument_spec=dict(
            outputs=dict(type="dict", required=False),
        ),
        required_one_of=[["outputs"]],
        supports_check_mode=True,
    )

    module.exit_json(changed=False, outputs=module.params["outputs"])


if __name__ == "__main__":
    main()
