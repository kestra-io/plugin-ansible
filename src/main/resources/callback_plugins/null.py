from ansible.plugins.callback import CallbackBase

DOCUMENTATION = '''
    name: null
    type: stdout
    short_description: Suppress all stdout output
    description: Does not display any output on stdout.
'''


class CallbackModule(CallbackBase):
    CALLBACK_VERSION = 2.0
    CALLBACK_TYPE = 'stdout'
    CALLBACK_NAME = 'null'
