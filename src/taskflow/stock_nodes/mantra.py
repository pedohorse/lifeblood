from copy import copy
from taskflow.basenode import BaseNode
from taskflow.enums import NodeParameterType
from taskflow.nodethings import ProcessingResult, ProcessingError
from taskflow.invocationjob import InvocationJob, InvocationEnvironment

from typing import Iterable


def node_class():
    return Mantra


class Mantra(BaseNode):
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

    def process_task(self, context) -> ProcessingResult:
        args = context.task_attributes()
        if 'file' not in args or 'outimage' not in args:
            raise ProcessingError('required attributes not found')

        env = InvocationEnvironment()

        invoc = InvocationJob(['mantra', '-V', '2a', context.param_value('ifd path'), context.param_value('image path')], env=env)
        res = ProcessingResult(invoc)
        return res

    def postprocess_task(self, context) -> ProcessingResult:
        args = context.task_attributes()
        res = ProcessingResult()
        res.set_attribute('file', args['outimage'])
        return res
