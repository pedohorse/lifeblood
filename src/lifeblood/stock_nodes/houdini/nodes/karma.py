from lifeblood.basenode import BaseNodeWithTaskRequirements
from lifeblood.enums import NodeParameterType
from lifeblood.nodethings import ProcessingResult, ProcessingError
from lifeblood.invocationjob import InvocationJob, InvocationEnvironment

from typing import Iterable


description = '''render USD file with Karma

usd file path: path to USD file to render
output image file path: path of final image. If AOVs are set up - their render location is not affected by this parameter
skip if result already exists: skip rendering if file defined by "output image file path" already exists
'''


def node_class():
    return Karma


class Karma(BaseNodeWithTaskRequirements):
    @classmethod
    def label(cls) -> str:
        return 'karma'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'houdini', 'karma', 'usd', 'stock'

    @classmethod
    def type_name(cls) -> str:
        return 'karma'

    @classmethod
    def description(cls) -> str:
        return description

    def __init__(self, name):
        super(Karma, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.5, 0.25, 0.125)
            ui.add_parameter('usd path', 'usd file path', NodeParameterType.STRING, "`task['file']`")
            ui.add_parameter('image path', 'output image file path', NodeParameterType.STRING, "`task['outimage']`")
            ui.add_parameter('skip if exists', 'skip if result already exists', NodeParameterType.BOOL, False)

            ui.parameter('worker type').set_hidden(True)
            ui.parameter('worker type').set_locked(True)

    def process_task(self, context) -> ProcessingResult:
        args = context.task_attributes()

        env = InvocationEnvironment()

        if context.param_value('skip if exists'):
            script = 'import os\n' \
                     'if not os.path.exists({imgpath}):\n' \
                     '    import sys\n' \
                     '    from subprocess import Popen\n' \
                     "    sys.exit(Popen(['husk', '-V', '2a', '--make-output-path',{doframe} '-o', {imgpath}, {usdpath}]).wait())\n" \
                     "else:\n" \
                     "    print('image file already exists, skipping work')\n" \
                    .format(imgpath=repr(context.param_value('image path')),
                            usdpath=repr(context.param_value('usd path')),
                            doframe=f" '-f', {repr(str(args['frames'][0]))}," if 'frames' in args else '',
                            )

            invoc = InvocationJob(['python', ':/karmacall.py'])
            invoc.set_extra_file('karmacall.py', script)
        else:  # TODO: -f there is testing, if succ - make a parameter out of it on the node or smth
            invoc = InvocationJob(['husk', '-V', '2a',
                                   '--make-output-path'] +
                                  (['-f', str(args['frames'][0])] if 'frames' in args else []) +
                                  ['-o', context.param_value('image path'), context.param_value('usd path')],
                                  env=env)
        res = ProcessingResult(invoc)
        return res

    def postprocess_task(self, context) -> ProcessingResult:
        res = ProcessingResult()
        res.set_attribute('file', context.param_value('image path'))
        return res
