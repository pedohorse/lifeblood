from taskflow.basenode import BaseNode
from taskflow.nodethings import ProcessingResult
from taskflow.taskspawn import TaskSpawn
from taskflow.exceptions import NodeNotReadyToProcess
from taskflow.enums import NodeParameterType
from taskflow.uidata import NodeUi


def create_node_object(name: str):
    return SetAttributes(name)


class SetAttributes(BaseNode):
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

    def process_task(self, task_dict) -> ProcessingResult:
        return ProcessingResult()

    def postprocess_task(self, task_dict) -> ProcessingResult:
        return ProcessingResult()
