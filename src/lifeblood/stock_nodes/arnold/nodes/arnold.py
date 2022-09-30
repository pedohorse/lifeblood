from lifeblood.basenode import BaseNodeWithTaskRequirements
from lifeblood.enums import NodeParameterType
from lifeblood.nodethings import ProcessingResult, ProcessingError
from lifeblood.invocationjob import InvocationJob, InvocationEnvironment

from typing import Iterable


def node_class():
    return Arnold


class Arnold(BaseNodeWithTaskRequirements):
    @classmethod
    def label(cls) -> str:
        return 'arnold'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'arnold', 'ass', 'stock'

    @classmethod
    def type_name(cls) -> str:
        return 'arnold'

    def __init__(self, name):
        super(Arnold, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.0, 0.54, 0.56)
            ui.add_parameter('ass path', 'ass file path', NodeParameterType.STRING, "`task['file']`")
            ui.add_parameter('image path', 'output image file path', NodeParameterType.STRING, "`task['outimage']`")
            ui.add_parameter('skip if exists', 'skip if result already exists', NodeParameterType.BOOL, False)

            ui.parameter('worker type').set_hidden(True)
            ui.parameter('worker type').set_locked(True)

    def process_task(self, context) -> ProcessingResult:
        args = context.task_attributes()
        if 'file' not in args or 'outimage' not in args:
            raise ProcessingError('required attributes not found')

        env = InvocationEnvironment()
        done_regex = r'.*?\|\s*(\d+)% done'

        if context.param_value('skip if exists'):
            script = 'import os\n' \
                     'if not os.path.exists({imgpath}):\n' \
                     '    import sys\n' \
                     '    from subprocess import Popen\n' \
                     "    sys.exit(Popen(['arnold',  '-dw', '-dp', '-v', '2', '-i', {asspath}, '-o', {imgpath}]).wait())\n" \
                     "else:\n" \
                     "    print('image file already exists, skipping work')\n" \
                    .format(imgpath=repr(context.param_value('image path')),
                            asspath=repr(context.param_value('ass path')))

            invoc = InvocationJob(['python', ':/arnoldcall.py'])
            invoc.set_stdout_progress_regex(done_regex)
            invoc.set_extra_file('arnoldcall.py', script)
        else:
            invoc = InvocationJob(['kick', '-dw', '-dp', '-v', '2', '-i', context.param_value('ass path'), '-o', context.param_value('image path')], env=env)
            invoc.set_stdout_progress_regex(done_regex)
        res = ProcessingResult(invoc)
        return res

    def postprocess_task(self, context) -> ProcessingResult:
        args = context.task_attributes()
        res = ProcessingResult()
        res.set_attribute('file', args['outimage'])
        return res
