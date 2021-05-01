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
            ui.add_parameter('svalue', 'val', NodeParameterType.STRING, '')
            ui.add_parameter('ivalue', 'val', NodeParameterType.INT, 0)
            ui.add_parameter('fvalue', 'val', NodeParameterType.FLOAT, 0.0)
            ui.add_parameter('bvalue', 'val', NodeParameterType.BOOL, False)
            ui.add_visibility_condition('svalue', f'type=={NodeParameterType.STRING.value}')
            ui.add_visibility_condition('ivalue', f'type=={NodeParameterType.INT.value}')
            ui.add_visibility_condition('fvalue', f'type=={NodeParameterType.FLOAT.value}')
            ui.add_visibility_condition('bvalue', f'type=={NodeParameterType.BOOL.value}')

    def process_task(self, task_dict) -> ProcessingResult:
        return ProcessingResult()

    def postprocess_task(self, task_dict) -> ProcessingResult:
        return ProcessingResult()
