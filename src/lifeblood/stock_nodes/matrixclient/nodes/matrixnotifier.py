import sys
import os
from lifeblood.basenode import BaseNode, ProcessingResult, ProcessingContext, ProcessingError
from lifeblood.enums import NodeParameterType
from lifeblood.paths import config_path
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
            with ui.collapsable_group_block('backend'):
                default_backend = 'fallback'
                if self._is_mc_available():
                    default_backend = 'matrixcommander'
                backend_param = ui.add_parameter('backend', 'backend', NodeParameterType.STRING, default_backend)
                backend_param.add_menu((('fallback', 'fallback'), ('matrix-commander', 'matrixcommander')))
            ui.add_parameter('label_e2ee', None, NodeParameterType.STRING, 'warning: e2e encryption is OFF', can_have_expressions=False, readonly=True)\
                .append_visibility_condition(backend_param, '==', 'fallback')
            ui.add_parameter('token', 'token', NodeParameterType.STRING, '`config["token"]`')\
                .append_visibility_condition(backend_param, '==', 'fallback')
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
    def _encode_hs(cls, hs: str):
        """
        given homeserver address return some fs-friendly folder name
        something safe and not giving collisions, but human-readable, not like hash
        """
        return hs.replace('://', '..').replace('/', '_')

    @classmethod
    def _is_mc_available(cls) -> bool:
        mc_cwd = cls.my_plugin().package_data() / 'matrixcommander'
        mc_script = mc_cwd / 'venv' / 'bin' / 'matrix-commander'
        mc_py = mc_cwd / 'venv' / 'bin' / 'python'
        mc_pyexe = mc_cwd / 'venv' / 'bin' / 'python.exe'

        # res = subprocess.Popen([mc_py, mc_script, '--version'], cwd=mc_cwd).wait()
        return mc_cwd.exists() and mc_script.exists() and (mc_py.exists() or mc_pyexe.exists())

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
        backend = context.param_value('backend')

        mc_script = None
        mc_py = None
        mc_cwd = None
        mc_conf_base = None
        if backend == 'fallback':
            pass
        elif backend == 'matrixcommander':
            mc_cwd = self.my_plugin().package_data() / 'matrixcommander'
            mc_script = mc_cwd / 'venv' / 'bin' / 'matrix-commander'
            mc_py = mc_cwd / 'venv' / 'bin' / 'python'
            mc_conf_base = config_path(self._encode_hs(context.param_value('server')), 'scheduler.nodes.matrixnotifier.matrixcommander')

            if not mc_py.exists():
                mc_py = mc_cwd / 'venv' / 'bin' / 'python.exe'  # windows case
            if not mc_script.exists():
                mc_script = mc_cwd / 'venv' / 'bin' / 'matrix-commander-script.pyw'  # windows case

            if not mc_py.exists() or not mc_script.exists():
                raise ProcessingError(f'backend "{backend}" was not properly set up. please read the doc')
        else:
            raise ProcessingError(f'unknown backend "{backend}"')

        if retries <= 0:
            retries = sys.maxsize
        for i in range(retries):
            if i > 0:
                time.sleep(2 ** (i - 1))
            print(f'attempt {i+1}...')
            if backend == 'fallback':
                proc = subprocess.Popen([sys.executable, self.my_plugin().package_data() / 'matrixclient.pyz',
                                         'send',
                                         context.param_value('server'),
                                         context.param_value('token'),
                                         context.param_value('room')], stdin=subprocess.PIPE)
                proc.communicate(context.param_value('message').encode('UTF-8'))
            elif backend == 'matrixcommander':
                assert mc_py is not None
                assert mc_script is not None
                assert mc_cwd is not None
                assert mc_conf_base is not None

                proc = subprocess.Popen([mc_py, mc_script,
                                         '--homeserver', context.param_value('server'),
                                         '--room', context.param_value('room'),
                                         '-c', str(mc_conf_base / 'credentials.json'),
                                         '-s', str(mc_conf_base / 'store'),
                                         '-m', '-'],
                                        stdin=subprocess.PIPE,
                                        cwd=mc_cwd,
                                        text=True)
                proc.communicate(context.param_value('message'))
            else:
                raise ProcessingError(f'unknown backend "{backend}"')

            if proc.wait() != 0:
                print(f'attempt {i + 1} failed, sleeping for {2 ** i}')
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
                if backend == 'fallback':
                    proc = subprocess.Popen([sys.executable, self.my_plugin().package_data() / 'matrixclient.pyz',
                                             'send',
                                             context.param_value('server'),
                                             context.param_value('token'),
                                             context.param_value('room'),
                                             filepath,
                                             '--message-is-file'])
                elif backend == 'matrixcommander':
                    assert mc_py is not None
                    assert mc_script is not None
                    assert mc_cwd is not None
                    assert mc_conf_base is not None

                    file_flag = '--file'
                    if os.path.splitext(filepath)[1] in ('.jpg', '.jpeg', '.png', '.gif'):
                        file_flag = '--image'

                    proc = subprocess.Popen([mc_py, mc_script,
                                             '--homeserver', context.param_value('server'),
                                             '--room', context.param_value('room'),
                                             '-c', str(mc_conf_base / 'credentials.json'),
                                             '-s', str(mc_conf_base / 'store'),
                                             file_flag, filepath],
                                            stdin=subprocess.DEVNULL,
                                            cwd=mc_cwd)
                    print(proc)
                    print(proc.poll())
                else:
                    raise ProcessingError(f'unknown backend "{backend}"')

                print('about to wait', proc.poll(), mc_cwd)
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

