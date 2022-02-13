from lifeblood.basenode import BaseNode
from lifeblood.nodethings import ProcessingResult
from lifeblood.enums import NodeParameterType
from lifeblood.taskspawn import TaskSpawn
from lifeblood.text import match_pattern

from typing import Iterable


def node_class():
    return SpawnChildren


class SpawnChildren(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'spawn children'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'spawn', 'child', 'children', 'born', 'birth', 'create'

    @classmethod
    def type_name(cls) -> str:
        return 'spawn_children'

    def __init__(self, name: str):
        super(SpawnChildren, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_output('spawned')
            ui.add_parameter('count', 'spawn this many children', NodeParameterType.INT, 0)
            ui.add_parameter('number attribute', 'child number attribute to create', NodeParameterType.STRING, '')
            ui.add_parameter('child basename', 'children base name', NodeParameterType.STRING, "`task.name`_child")
            ui.add_parameter('inherit attributes', 'attribute pattern to inherit', NodeParameterType.STRING, '*')

    def process_task(self, context) -> ProcessingResult:
        res = ProcessingResult()
        basename = context.param_value('child basename')
        pattern = context.param_value('inherit attributes')
        new_attributes_base = {attr: val for attr, val in context.task_attributes().items() if match_pattern(pattern, attr)}
        num_attr_name = context.param_value('number attribute')
        for i in range(context.param_value('count')):
            new_attributes = new_attributes_base.copy()
            if num_attr_name:
                new_attributes[num_attr_name] = i
            newtask = TaskSpawn(f'{basename}{i}', None, task_attributes=new_attributes)
            newtask.force_set_node_task_id(self.id(), context.task_id())
            res.add_spawned_task(newtask)

        return res
