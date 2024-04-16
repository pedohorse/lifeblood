import sys
import os
from lifeblood.basenode import BaseNodeWithTaskRequirements, ProcessingResult, ProcessingContext, ProcessingError
from lifeblood.enums import NodeParameterType
from lifeblood.invocationjob import InvocationJob, InvocationEnvironment
from lifeblood.paths import config_path
from typing import Iterable
import subprocess
import inspect
import time
import json
import tempfile
import shutil


description = \
'''sends any text notification to any telegram chat with a bot.
Bot must be created beforehand and added to all chats where it needs
to send messages.

BEWARE: when executing on workers - bot_id WILL BE SAVED TO SCHEDULER'S DATABASE
as part of Invocation Job description

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


def run_stuff(args, bot_id: str, message: str, fail_on_error: bool = True):
    print('reporting to telegram')

    proc = subprocess.Popen(
        args,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={
            'TELEGRAM_CLIENT_BOTID': bot_id,
        }
    )
    out, err = proc.communicate(message.encode('UTF-8'))
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

    if fail_on_error and proc.wait() != 0:
        raise RuntimeError(f'notifier process exited with code {proc.poll()}, {err}')


class TelegramNotifier(BaseNodeWithTaskRequirements):
    def __init__(self, name):
        super().__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.165, 0.671, 0.933)
            with ui.collapsable_group_block('bot parameters', 'Bot Parameters'):
                ui.add_parameter('bot_id', 'Bot Secret ID', NodeParameterType.STRING, '`config["bot_id"]`')
            ui.add_parameter('chat_id', 'Chat ID', NodeParameterType.STRING, '`config["chat_id"]`')
            ui.add_parameter('fail on error', 'Fail on error', NodeParameterType.BOOL, True)
            ui.add_separator()
            ui.add_parameter('message formatting', 'Formatting', NodeParameterType.STRING, '').add_menu(
                (
                    ('Plain', ''),
                    ('Markdown', 'Markdown'),
                    ('Markdown V2', 'MarkdownV2'),
                    ('HTML', 'HTML'),
                )
            )
            ui.add_parameter('message', 'message', NodeParameterType.STRING, '').set_text_multiline()
            with ui.parameters_on_same_line_block():
                ui.add_parameter('do attach', 'attach a file', NodeParameterType.BOOL, False)
                ui.add_parameter('attachment', None, NodeParameterType.STRING, '')
            ui.add_separator()
            ui.add_parameter('on worker', 'Use worker to send notification', NodeParameterType.BOOL, False)

        self.param('worker cpu cost').set_value(0.0)
        self.param('worker mem cost').set_value(0.1)

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

        on_worker = context.param_value('on worker')

        args = [
            sys.executable,
            self.my_plugin().package_data() / 'telegram_client.pyz' if not on_worker else '',  # on worker we'll replace it later
            '--message-stdin',
        ]

        if context.param_value('do attach'):
            args += [
                '--attach',
                context.param_value('attachment')
            ]

        if parse_mode := context.param_value('message formatting'):
            args += [
                '--parse_mode',
                parse_mode
            ]

        args += [context.param_value('chat_id')]

        if not on_worker:
            run_stuff(
                args,
                bot_id=context.param_value('bot_id'),
                message=context.param_value('message'),
                fail_on_error=context.param_value('fail on error'),
            )

            return ProcessingResult()

        else:
            script = ('import subprocess\n'
                      'import os\n'
                      'import sys\n'
                      '\n')
            script += inspect.getsource(run_stuff)
            script += ('\n'
                       'def do():\n'
                       f'    args = {repr(args)}\n'
                       f'    args[1] = sys.argv[1]\n'  # will be set to runtime known path of worker-local telegram_client.pyz
                       f'    message = {repr(context.param_value("message"))}\n'
                       f'    fail_on_error = {repr(context.param_value("fail on error"))}\n'
                       '    run_stuff(\n'
                       '        args,\n'
                       '        bot_id=os.environ["TELEGRAM_CLIENT_BOTID"],\n'
                       '        message=message,\n'
                       '        fail_on_error=fail_on_error,\n'
                       '    )'
                       '\n'
                       'if __name__ == "__main__":\n'
                       '    do()\n')

            env = InvocationEnvironment()
            env.set_variable('TELEGRAM_CLIENT_BOTID', context.param_value('bot_id'))
            job = InvocationJob(
                ['python', ':/work.py', ':/telegram_client.pyz'],
                env=env
            )
            job.set_extra_file('work.py', script)
            with open(self.my_plugin().package_data() / 'telegram_client.pyz', 'rb') as f:
                job.set_extra_file('telegram_client.pyz', f.read())

            return ProcessingResult(job)
