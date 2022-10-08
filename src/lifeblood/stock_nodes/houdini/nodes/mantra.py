from copy import copy
from lifeblood.basenode import BaseNodeWithTaskRequirements
from lifeblood.enums import NodeParameterType
from lifeblood.nodethings import ProcessingResult, ProcessingError
from lifeblood.invocationjob import InvocationJob, InvocationEnvironment

from typing import Iterable


def node_class():
    return Mantra


class Mantra(BaseNodeWithTaskRequirements):
    @classmethod
    def label(cls) -> str:
        return 'mantra'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'houdini', 'mantra', 'ifd', 'stock'

    @classmethod
    def type_name(cls) -> str:
        return 'mantra'

    def __init__(self, name):
        super(Mantra, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.5, 0.25, 0.125)
            ui.add_parameter('ifd path', 'ifd file path', NodeParameterType.STRING, "`task['file']`")
            ui.add_parameter('image path', 'output image file path', NodeParameterType.STRING, "`task['outimage']`")
            ui.add_parameter('skip if exists', 'skip if result already exists', NodeParameterType.BOOL, False)

            ui.parameter('worker type').set_hidden(True)
            ui.parameter('worker type').set_locked(True)

    def process_task(self, context) -> ProcessingResult:
        args = context.task_attributes()
        if 'file' not in args or 'outimage' not in args:
            raise ProcessingError('required attributes not found')

        env = InvocationEnvironment()

        if context.param_value('skip if exists'):
            script = 'import os\n' \
                     'if not os.path.exists({imgpath}):\n' \
                     '    import sys\n' \
                     '    from subprocess import Popen\n' \
                     "    sys.exit(Popen(['mantra', '-V', '2a', {ifdpath}, {imgpath}]).wait())\n" \
                     "else:\n" \
                     "    print('image file already exists, skipping work')\n" \
                    .format(imgpath=repr(context.param_value('image path')),
                            ifdpath=repr(context.param_value('ifd path')))

            invoc = InvocationJob(['python', ':/mantracall.py'])
            invoc.set_extra_file('mantracall.py', script)
        else:
            invoc = InvocationJob(['mantra', '-V', '2a', context.param_value('ifd path'), context.param_value('image path')], env=env)
        res = ProcessingResult(invoc)
        return res

    def postprocess_task(self, context) -> ProcessingResult:
        args = context.task_attributes()
        res = ProcessingResult()
        res.set_attribute('file', args['outimage'])
        return res
