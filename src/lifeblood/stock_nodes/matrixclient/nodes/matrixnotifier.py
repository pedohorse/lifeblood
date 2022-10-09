import sys
import os
from lifeblood.basenode import BaseNode, ProcessingResult, ProcessingContext, ProcessingError
from lifeblood.enums import NodeParameterType
from typing import Iterable
import subprocess
import time
import json
import tempfile
import shutil


description = \
'''sends any text notification to any matrix room
matrix is a great decentralized communication protocol
you can register accounts on matrix.org or any other publically hosted servers
or self host a server, federated or isolated.

it is recommended to set up token and room once in config
not to expose them in node parameters
to do that - keep default expressions, and 
set values of token and room in your <home>/lifeblood/nodes/config.toml
matrixnotifier.token = 'your_token_here'
matrixnotifier.room = '!your_room_code:server.org'
'''


def node_class():
    return MatrixNotifier


class MatrixNotifier(BaseNode):
    def __init__(self, name):
        super().__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.051, 0.741, 0.545)
            ui.add_parameter('token', 'token', NodeParameterType.STRING, '`config["token"]`')
            ui.add_parameter('room', 'room', NodeParameterType.STRING, '`config["room"]`')
            ui.add_parameter('retries', 'retries', NodeParameterType.INT, 5).set_value_limits(value_min=1)
            ui.add_parameter('fail on error', 'fail task on notification sending error', NodeParameterType.BOOL, True)
            with ui.collapsable_group_block('custom server block', 'custom server'):
                ui.add_parameter('server', 'matrix server', NodeParameterType.STRING, '`config.get("server", "https://matrix.org")`')
            ui.add_parameter('message', 'message', NodeParameterType.STRING, '').set_text_multiline()
            with ui.parameters_on_same_line_block():
                ui.add_parameter('do attach', 'attach a file', NodeParameterType.BOOL, False)
                ui.add_parameter('attachment', None, NodeParameterType.STRING, '')

    @classmethod
    def label(cls) -> str:
        return 'matrix notifier'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'matrix', 'client', 'notify'

    @classmethod
    def type_name(cls) -> str:
        return 'matrixnotifier'

    @classmethod
    def description(cls) -> str:
        return description

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        print('reporting to matrix')
        retries = context.param_value('retries')
        if retries <= 0:
            retries = sys.maxsize
        for i in range(retries):
            if i > 0:
                time.sleep(2 ** (i - 1))
            print(f'attempt {i+1}...')
            proc = subprocess.Popen([sys.executable, self.my_plugin().package_data() / 'matrixclient.pyz',
                                     'send',
                                     context.param_value('server'),
                                     context.param_value('token'),
                                     context.param_value('room')], stdin=subprocess.PIPE)
            proc.communicate(context.param_value('message').encode('UTF-8'))
            if proc.wait() != 0:
                print(f'attempt {i+1} failed, sleeping for {2**i}')
                continue
            print('reporting to matrix done')
            break
        else:
            print('failed to send notification', file=sys.stderr)
            if context.param_value('fail on error'):
                raise ProcessingError('failed to send notification')
        if context.param_value('do attach'):
            filepath = context.param_value('attachment')
            if not os.path.exists(filepath):
                raise ProcessingError('attachment file does not exist')

            for i in range(retries):
                if i > 0:
                    time.sleep(2 ** (i - 1))
                print(f'attempt {i + 1}...')
                proc = subprocess.Popen([sys.executable, self.my_plugin().package_data() / 'matrixclient.pyz',
                                         'send',
                                         context.param_value('server'),
                                         context.param_value('token'),
                                         context.param_value('room'),
                                         filepath,
                                         '--message-is-file'])
                if proc.wait() != 0:
                    print(f'attempt {i + 1} failed, sleeping for {2 ** i}')
                    continue
                print('reporting to matrix done')
                break
            else:
                print('failed to send attachment', file=sys.stderr)
                if context.param_value('fail on error'):
                    raise ProcessingError('failed to send attachment')
        return ProcessingResult()

