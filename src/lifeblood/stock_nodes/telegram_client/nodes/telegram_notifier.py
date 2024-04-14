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
'''sends any text notification to any telegram chat with a bot.
Bot must be created beforehand and added to all chats where it needs
to send messages.

it is recommended to set up bot_id and chat_id once in then config,
not to expose them in node parameters.  
to do that - keep default expressions, and
set values of token and room in your <home>/lifeblood/nodes/config.toml  
```
telegram_notifier.bot_id = '<secret_bot_id here>'
telegram_notifier.chat_id = '<chat id here>'
```
'''


def node_class():
    return TelegramNotifier


class TelegramNotifier(BaseNode):
    def __init__(self, name):
        super().__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.165, 0.671, 0.933)
            with ui.collapsable_group_block('bot parameters', 'Bot Parameters'):
                ui.add_parameter('bot_id', 'Bot Secret ID', NodeParameterType.STRING, '`config["bot_id"]`')
            ui.add_parameter('chat_id', 'Chat ID', NodeParameterType.STRING, '`config["chat_id"]`')
            ui.add_parameter('fail on error', 'fail task on notification sending error', NodeParameterType.BOOL, True)
            ui.add_parameter('message', 'message', NodeParameterType.STRING, '').set_text_multiline()
            with ui.parameters_on_same_line_block():
                ui.add_parameter('do attach', 'attach a file', NodeParameterType.BOOL, False)
                ui.add_parameter('attachment', None, NodeParameterType.STRING, '')

    @classmethod
    def label(cls) -> str:
        return 'telegram notifier'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'telegram', 'client', 'notify'

    @classmethod
    def type_name(cls) -> str:
        return 'telegram_notifier'

    @classmethod
    def description(cls) -> str:
        return description

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        print('reporting to telegram')

        args = [
            sys.executable,
            self.my_plugin().package_data() / 'telegram_client.pyz',
            '--message-stdin',
        ]

        if context.param_value('do attach'):
            args += [
                '--attach',
                context.param_value('attachment')
            ]

        args += [context.param_value('chat_id')]

        proc = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={
                'TELEGRAM_CLIENT_BOTID': context.param_value('bot_id'),
            }
        )
        out, err = proc.communicate(context.param_value('message').encode('UTF-8'))
        if not isinstance(out, str):
            try:
                out = out.decode('utf-8')
            except UnicodeDecodeError:
                out = out.decode('latin1')
        if not isinstance(err, str):
            try:
                err = err.decode('utf-8')
            except UnicodeDecodeError:
                err = err.decode('latin1')
        print(out)
        print(err)

        if context.param_value('fail on error') and proc.wait() != 0:
            raise ProcessingError(f'notifier process exited with code {proc.poll()}, {err}')

        return ProcessingResult()

