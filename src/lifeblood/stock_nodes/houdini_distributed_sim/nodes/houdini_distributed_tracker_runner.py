import uuid
from lifeblood.basenode import BaseNode
from lifeblood.nodethings import ProcessingResult, InvocationJob
from lifeblood.invocationjob import InvocationRequirements
from lifeblood.enums import NodeParameterType, WorkerType
from lifeblood.attribute_serialization import serialize_attributes_core


from typing import Iterable


def node_class():
    return HoudiniDistributedTracker


class HoudiniDistributedTracker(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'houdini sim tracker'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'houdini', 'tracker', 'distributed'

    @classmethod
    def type_name(cls) -> str:
        return 'houdini_distributed_tracker'

    @classmethod
    def description(cls) -> str:
        return 'creates a running houdini distributed simulation tracker\n' \
               'when tracker is up and running - a child task is created\n' \
               'child task inherits all attributes from parent\n' \
               'additionally these attribs are created:\n' \
               '    SIMTRACKER_HOST: hostname of the tracker\n' \
               '    SIMTRACKER_PORT: port of the tracker\n' \
               '    tracker kill port: port of the tracker for control commands'

    def __init__(self, name: str):
        super(HoudiniDistributedTracker, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.5, 0.25, 0.125)
            ui.add_output('spawned')
            ui.add_parameter('port', 'sim port', NodeParameterType.INT, 19375)
            ui.add_parameter('wport', 'web port', NodeParameterType.INT, 19376)
            ui.add_parameter('use helper', 'run on scheduler helper', NodeParameterType.BOOL, True)

    def process_task(self, context) -> ProcessingResult:
        code = (self.my_plugin().package_data() / 'server.py').read_text()
        addressee_name = str(uuid.uuid4())
        invoc = InvocationJob(['python', ':/work_to_do.py', context.param_value('port'), context.param_value('wport'), addressee_name, ':/task_base_attrs.json'])
        invoc.set_extra_file('work_to_do.py', code)
        invoc.set_extra_file('task_base_attrs.json', serialize_attributes_core(dict(context.task_attributes())))

        if context.param_value('use helper'):
            invoc.set_requirements(InvocationRequirements(worker_type=WorkerType.SCHEDULER_HELPER))
        res = ProcessingResult(invoc)

        return res
