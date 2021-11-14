from taskflow.basenode import BaseNode
from taskflow.nodethings import ProcessingResult

from typing import Iterable


def node_class():
    return NullNode


class NullNode(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'null'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'null', 'passthrough', 'core'

    @classmethod
    def type_name(cls) -> str:
        return 'null'

    def __init__(self, name: str):
        super(NullNode, self).__init__(name)

    def process_task(self, context) -> ProcessingResult:
        return ProcessingResult()

    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()
