from taskflow.basenode import BaseNode
from taskflow.nodethings import ProcessingResult, ProcessingError
from taskflow.processingcontext import ProcessingContext
from taskflow.enums import NodeParameterType
from taskflow.uidata import NodeUi, MultiGroupLayout, Parameter
from taskflow.node_visualization_classes import NodeColorScheme

from typing import Iterable


def node_class():
    return SwitchTasks


class SwitchTasks(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'switch tasks'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'switch', 'core'

    @classmethod
    def type_name(cls) -> str:
        return 'swtich'

    def __init__(self, name: str):
        super(SwitchTasks, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('output', 'select output', NodeParameterType.INT, 0, can_have_expressions=True)

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        switch = context.param_value('output')
        outputs = self.get_ui().outputs_names()

        res = ProcessingResult()
        res.set_node_output_name(outputs[min(len(outputs) - 1, max(0, switch))])
        return res
