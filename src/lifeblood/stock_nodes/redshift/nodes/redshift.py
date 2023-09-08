from copy import copy
from lifeblood.basenode import BaseNodeWithTaskRequirements
from lifeblood.enums import NodeParameterType
from lifeblood.nodethings import ProcessingResult, ProcessingError
from lifeblood.invocationjob import InvocationJob, InvocationEnvironment

from typing import Iterable


def node_class():
    return Redshift


class Redshift(BaseNodeWithTaskRequirements):
    @classmethod
    def label(cls) -> str:
        return 'redshift'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'houdini', 'redshift', 'rs', 'stock'

    @classmethod
    def type_name(cls) -> str:
        return 'redshift'

    def __init__(self, name):
        super(Redshift, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.5, 0.25, 0.125)
            ui.add_parameter('rs path', 'rs file path', NodeParameterType.STRING, "`task['file']`")
            ui.add_parameter('image path', 'output image file path', NodeParameterType.STRING, "`task['outimage']`")
            ui.add_parameter('skip if exists', 'skip if result already exists', NodeParameterType.BOOL, False)

            # ui.parameter('')
            ui.parameter('worker type').set_hidden(True)
            ui.parameter('worker type').set_locked(True)

    def process_task(self, context) -> ProcessingResult:
        args = context.task_attributes()

        env = InvocationEnvironment()

        script = 'import os\n' \
                 'import sys\n' \
                 'from subprocess import Popen, PIPE\n' \
                 'import lifeblood_connection as lbc\n' \
                 '\n' \
                 'if {skip_if_exists}:  # NOT IMPLEMENTED YET\n' \
                 "    print('image file already exists, skipping work')\n" \
                 "    sys.exit(0)\n" \
                 "output_files = []\n" \
                 "p = Popen(['redshiftCmdLine', {rspath}], stdout=PIPE)\n" \
                 'while p.poll() is None:\n' \
                 '    full_line = False\n' \
                 '    part_parts = []\n' \
                 '    while not full_line:\n' \
                 '        part = p.stdout.readline(1024)\n' \
                 '        part_parts.append(part)\n' \
                 '        if part.endswith(b"\\n") or part == b"" or p.poll() is not None:\n' \
                 '            full_line = True\n' \
                 '            break\n' \
                 '    part = b"".join(part_parts)\n' \
                 '\n' \
                 '    if part.startswith(b"Saving: "):\n' \
                 '        output_files.append(part[8:].strip())\n' \
                 '    sys.stdout.buffer.write(part)  # "promote" to stdout\n' \
                 'lbc.set_attributes({{"file": output_files[0].decode("utf-8") if output_files else None,\n' \
                 '                     "files": [x.decode("utf-8") for x in output_files]}}, blocking=True)\n' \
                 "sys.exit()\n" \
                 '' \
                 .format(skip_if_exists=repr(False),  # for now we cannot know output image until after render
                         rspath=repr(context.param_value('rs path')))

        invoc = InvocationJob(['python', ':/rsccall.py'])
        invoc.set_extra_file('rsccall.py', script)
        invoc.set_stdout_progress_regex(rb'^\s*Block\s+(\d+)/(\d+)\s+')

        res = ProcessingResult(invoc)
        return res
