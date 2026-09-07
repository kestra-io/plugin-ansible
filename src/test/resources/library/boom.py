#!/usr/bin/python
# Raises so ansible populates result['exception'] with a traceback. CallbackBase renders that
# outside _dump_results, so EXPLICIT-mode redaction has to intercept it separately. The frame name
# is deliberate: it appears in the traceback but not in the exception message, which is what
# separates the two rendering paths.
from ansible.module_utils.basic import AnsibleModule


def frame_named_canary_7742():
    raise Exception("module failed on purpose")


def main():
    AnsibleModule(argument_spec={})
    frame_named_canary_7742()


if __name__ == "__main__":
    main()
