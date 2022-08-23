from lifeblood.basenode import BaseNode
from lifeblood.nodethings import ProcessingResult
from lifeblood.enums import NodeParameterType

from typing import Iterable


def node_class():
    return DeleteAttributes


class DeleteAttributes(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'delete attributes'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'delete', 'attribute', 'core'

    @classmethod
    def type_name(cls) -> str:
        return 'delete_attrib'

    def __init__(self, name: str):
        super(DeleteAttributes, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            with ui.multigroup_parameter_block('num'):
                with ui.parameters_on_same_line_block():
                    ui.add_parameter('name', 'delete', NodeParameterType.STRING, '')

    def process_task(self, context) -> ProcessingResult:
        res = ProcessingResult()
        attrs = set(context.task_attributes().keys())
        for i in range(context.param_value('num')):
            attr_name = context.param_value(f'name_{i}')
            if attr_name == '' or attr_name not in attrs:
                continue
            attrs.remove(attr_name)
            res.remove_attribute(attr_name)
        return res
