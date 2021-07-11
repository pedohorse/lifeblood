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
            with ui.parameters_on_same_line_block():
                ui.add_parameter('oldname', 'from', NodeParameterType.STRING, 'from')
                ui.add_parameter('newname', 'to', NodeParameterType.STRING, 'to')

    def process_task(self, context) -> ProcessingResult:
        attr_oldname = context.param_value('oldname')
        attrs = context.task_attributes()
        if attr_oldname not in attrs:
            return ProcessingResult()
        attr_newname = context.param_value('newname')
        res = ProcessingResult()
        res.set_attribute(attr_newname, attrs[attr_oldname])
        res.remove_attribute(attr_oldname)
        return res

    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()
