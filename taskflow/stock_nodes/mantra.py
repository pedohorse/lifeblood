from copy import copy
from taskflow.basenode import BaseNode
from taskflow.enums import NodeParameterType
from taskflow.nodethings import ProcessingResult
from taskflow.invocationjob import InvocationJob, InvocationEnvironment


def create_node_object(name: str, parent_scheduler):
    return Mantra(name, parent_scheduler)


class Mantra(BaseNode):
    def __init__(self, name, scheduler):
        super(Mantra, self).__init__(name, scheduler)

    def process_task(self, task_dict) -> ProcessingResult:
        args = self.get_attributes(task_dict)
        if 'ifdpath' not in args:
            return ProcessingResult()

        env = InvocationEnvironment()
        env.prepend('PATH', '/opt/hfs18.5/bin/')  # TODO: !!! this is hardcoded here purely for short lived test purposes

        invoc = InvocationJob(['mantra', args['ifdpath']], env)
        res = ProcessingResult(invoc)
        return res

    def postprocess_task(self, task_dict) -> ProcessingResult:
        return ProcessingResult()
