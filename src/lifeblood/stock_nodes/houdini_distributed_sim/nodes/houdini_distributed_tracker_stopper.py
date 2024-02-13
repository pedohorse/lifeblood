import sys

from lifeblood.basenode import BaseNode
from lifeblood.nodethings import ProcessingResult, InvocationJob
from lifeblood.invocationjob import InvocationRequirements
from lifeblood.enums import NodeParameterType, WorkerType

from typing import Iterable


def node_class():
    return HoudiniDistributedTrackerStopper


class HoudiniDistributedTrackerStopper(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'houdini sim tracker stopper'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'houdini', 'tracker', 'distributed', 'stopper'

    @classmethod
    def type_name(cls) -> str:
        return 'houdini_distributed_tracker_stopper'

    def __init__(self, name: str):
        super(HoudiniDistributedTrackerStopper, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.5, 0.25, 0.125)
            ui.add_parameter('target_iid', 'simtracker invocation id', NodeParameterType.INT, 0).set_expression("task['tracker_control_iid']")
            ui.add_parameter('target_addressee', 'simtracker identifier', NodeParameterType.STRING, "`task['tracker_control_addressee']`")
            ui.add_parameter('message_timeout', 'timeout', NodeParameterType.FLOAT, 90)

    def process_task(self, context) -> ProcessingResult:
        invoc = InvocationJob([
            sys.executable,  # we can use sys.executable only because we run killer only on SCHEDULER_HELPERs
            ':/work_to_do.py',
            str(context.param_value('target_iid')),
            str(context.param_value('target_addressee')),
            str(context.param_value('message_timeout')),
        ])
        code = (self.my_plugin().package_data() / 'killer.py').read_text()
        invoc.set_extra_file('work_to_do.py', code)
        invoc.set_requirements(InvocationRequirements(worker_type=WorkerType.SCHEDULER_HELPER))
        res = ProcessingResult(invoc)
        return res
