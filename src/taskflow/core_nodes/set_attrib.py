from taskflow.basenode import BaseNode
from taskflow.nodethings import ProcessingResult
from taskflow.taskspawn import TaskSpawn
from taskflow.exceptions import NodeNotReadyToProcess
from taskflow.enums import NodeParameterType
from taskflow.uidata import NodeUi

from typing import Iterable


def node_class():
    return SetAttributes


class SetAttributes(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'set attributes'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'set', 'attribute', 'core'

    def __init__(self, name: str):
        super(SetAttributes, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('name', 'Name', NodeParameterType.STRING, '')
            ui.next_parameter_same_line()
            ui.add_parameter('type', '', NodeParameterType.INT, 0)
            ui.add_menu_to_parameter('type', (('int', NodeParameterType.INT.value),
                                              ('bool', NodeParameterType.BOOL.value),
                                              ('float', NodeParameterType.FLOAT.value),
                                              ('string', NodeParameterType.STRING.value)))
            ui.next_parameter_same_line()
            ui.add_parameter('value', 'val', NodeParameterType.STRING, '')
            ui.add_visibility_condition('value', f'type=={NodeParameterType.STRING.value}')

    def process_task(self, task_dict) -> ProcessingResult:
        return ProcessingResult()

    def postprocess_task(self, task_dict) -> ProcessingResult:
        return ProcessingResult()
