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

    def process_task(self, context) -> ProcessingResult:
        args = context.task_attributes()
        if 'file' not in args or 'outimage' not in args:
            return ProcessingResult()

        env = InvocationEnvironment()

        invoc = InvocationJob(['mantra', '-V', '2a', args['file'], args['outimage']], env)
        res = ProcessingResult(invoc)
        res.set_attribute('file', args['outimage'])
        return res

    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()
