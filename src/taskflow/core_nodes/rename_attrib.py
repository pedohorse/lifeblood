from taskflow.basenode import BaseNode
from taskflow.nodethings import ProcessingResult
from taskflow.taskspawn import TaskSpawn
from taskflow.exceptions import NodeNotReadyToProcess
from taskflow.enums import NodeParameterType
from taskflow.uidata import NodeUi

from typing import Iterable


def node_class():
    return RenameAttributes


class RenameAttributes(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'rename attributes'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'set', 'attribute', 'core'

    def __init__(self, name: str):
        super(RenameAttributes, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('oldname', 'from', NodeParameterType.STRING, 'from')
            ui.next_parameter_same_line()
            ui.add_parameter('newname', 'to', NodeParameterType.STRING, 'to')

    def process_task(self, task_dict) -> ProcessingResult:
        ui = self.get_ui()
        attrs = self._get_task_attributes(task_dict)
        attr_oldname = ui.parameter_value('oldname')
        if attr_oldname not in attrs:
            return ProcessingResult()
        attr_newname = ui.parameter_value('newname')
        res = ProcessingResult()
        res.set_attribute(attr_newname, attrs[attr_oldname])
        res.remove_attribute(attr_oldname)
        return res

    def postprocess_task(self, task_dict) -> ProcessingResult:
        return ProcessingResult()
