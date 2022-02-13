import socket

from lifeblood.basenode import BaseNode
from lifeblood.nodethings import ProcessingResult, InvocationJob
from lifeblood.invocationjob import InvocationRequirements
from lifeblood.enums import NodeParameterType, WorkerType
from lifeblood.taskspawn import TaskSpawn

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
            ui.add_parameter('host', 'sim tracker host', NodeParameterType.STRING, "`task['SIMTRACKER_HOST']`")
            ui.add_parameter('kport', 'kill port', NodeParameterType.INT, 19377)  # TODO: make default to be expression returning attrib kill port
            #ui.add_parameter('use helper', 'run on scheduler helper', NodeParameterType.BOOL, True)

    def process_task(self, context) -> ProcessingResult:
        addr = (context.param_value('host'), context.param_value('kport'))
        s = socket.socket()
        s.connect(addr)
        try:
            s.sendall(b'\0')
        finally:
            s.close()

        res = ProcessingResult()
        return res
