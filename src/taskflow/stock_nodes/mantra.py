from copy import copy
from taskflow.basenode import BaseNode
from taskflow.enums import NodeParameterType
from taskflow.nodethings import ProcessingResult
from taskflow.invocationjob import InvocationJob, InvocationEnvironment

from typing import Iterable


def node_class():
    return Mantra


class Mantra(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'mantra'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'houdini', 'mantra', 'ifd', 'stock'

    def __init__(self, name):
        super(Mantra, self).__init__(name)

    def process_task(self, task_dict) -> ProcessingResult:
        args = self._get_task_attributes(task_dict)
        if 'file' not in args:
            return ProcessingResult()

        env = InvocationEnvironment()

        invoc = InvocationJob(['mantra', '-V', '2a', args['file']], env)
        res = ProcessingResult(invoc)
        return res

    def postprocess_task(self, task_dict) -> ProcessingResult:
        return ProcessingResult()