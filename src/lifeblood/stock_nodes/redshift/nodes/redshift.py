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

        # TODO!!!  in case of skip existing "files" is not filled with AOVs !!!

        script = 'import os\n' \
                 'import sys\n' \
                 'from subprocess import Popen, PIPE\n' \
                 'import tempfile\n' \
                 'import shutil\n' \
                 'import lifeblood_connection as lbc\n' \
                 '\n' \
                 'out_beauty = {out_beauty}\n' \
                 'if {skip_if_exists} and os.path.exists(out_beauty):\n' \
                 "    print('image file already exists, skipping work')\n" \
                 '    lbc.set_attributes({{"file": out_beauty, \n' \
                 '                         "files": [out_beauty]}}, blocking=True)\n' \
                 "    sys.exit(0)\n" \
                 "\n" \
                 'temp_render_dir = tempfile.mkdtemp(prefix="redshift_")\n' \
                 "output_files = []\n" \
                 "p = Popen(['redshiftCmdLine', {rspath}, '-oip', temp_render_dir], stdout=PIPE)\n" \
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
                 '\n' \
                 'if not output_files:\n' \
                 '    sys.exit(1)\n' \
                 'output_files = [x.decode("utf-8") for x in output_files]\n' \
                 '\n' \
                 'def cleancopy(src, dst):\n' \
                 '    try:\n' \
                 '        os.unlink(dst)\n' \
                 '    except FileNotFoundError:\n' \
                 '        pass\n' \
                 '    os.makedirs(os.path.dirname(dst), exist_ok=True)\n' \
                 '    shutil.copy2(src, dst)\n' \
                 '\n' \
                 'print("copying locally rendered files to final destination...")\n' \
                 'final_output_files = []\n' \
                 'sys.stdout.buffer.write(f"copying to {{out_beauty}}\\n".encode("UTF-8"))\n' \
                 'cleancopy(output_files[0], out_beauty)\n' \
                 'final_output_files.append(out_beauty)\n' \
                 'out_beauty_dir = os.path.dirname(out_beauty)\n' \
                 'for file_path in output_files:\n' \
                 '    file_path_dst = os.path.join(out_beauty_dir, os.path.basename(file_path))\n' \
                 '    sys.stdout.buffer.write(f"copying to {{file_path_dst}}\\n".encode("UTF-8"))\n' \
                 '    cleancopy(file_path, file_path_dst)\n' \
                 '    final_output_files.append(file_path_dst)\n' \
                 '\n' \
                 'lbc.set_attributes({{"file": final_output_files[0],\n' \
                 '                     "files": final_output_files}}, blocking=True)\n' \
                 'print("cleaning up...")\n' \
                 'shutil.rmtree(temp_render_dir)\n' \
                 'print("all done")\n' \
                 "sys.exit()\n" \
                 '' \
                 .format(skip_if_exists=repr(context.param_value('skip if exists')),  # for now we cannot know output image until after render
                         rspath=repr(context.param_value('rs path')),
                         out_beauty=repr(context.param_value('image path')))

        invoc = InvocationJob(['python', ':/rsccall.py'])
        invoc.set_extra_file('rsccall.py', script)
        invoc.set_stdout_progress_regex(rb'^\s*Block\s+(\d+)/(\d+)\s+')

        res = ProcessingResult(invoc)
        return res
