from lifeblood.basenode import BaseNode
from lifeblood.nodethings import ProcessingResult
from lifeblood.taskspawn import TaskSpawn
from lifeblood.exceptions import NodeNotReadyToProcess
from lifeblood.enums import NodeParameterType
from lifeblood.uidata import NodeUi

from typing import Iterable


def node_class():
    return RenameAttributes


class RenameAttributes(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'rename attributes'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'rename', 'attribute', 'core'

    @classmethod
    def type_name(cls) -> str:
        return 'rename_attrib'

    def __init__(self, name: str):
        super(RenameAttributes, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            with ui.multigroup_parameter_block('num'):
                with ui.parameters_on_same_line_block():
                    ui.add_parameter('oldname', '<from / to>', NodeParameterType.STRING, 'from')
                    ui.add_parameter('newname', None, NodeParameterType.STRING, 'to')

    def process_task(self, context) -> ProcessingResult:
        res = ProcessingResult()
        attrs = dict(context.task_attributes())
        for i in range(context.param_value('num')):
            attr_oldname = context.param_value(f'oldname_{i}')

            if attr_oldname not in attrs:
                continue

            attr_newname = context.param_value(f'newname_{i}')
            if attr_newname == attr_oldname:
                continue

            res.set_attribute(attr_newname, attrs[attr_oldname])
            res.remove_attribute(attr_oldname)
            attrs[attr_newname] = attrs[attr_oldname]
            del attrs[attr_oldname]

        return res

    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()
