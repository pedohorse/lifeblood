from lifeblood.basenode import BaseNode
from lifeblood.nodethings import ProcessingResult

from typing import Iterable


def node_class():
    return KillNode


class KillNode(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'killer'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'kill', 'die', 'core'

    @classmethod
    def type_name(cls) -> str:
        return 'killer'

    def __init__(self, name: str):
        super(KillNode, self).__init__(name)

    def process_task(self, context) -> ProcessingResult:
        res = ProcessingResult()
        res.kill_task()
        return res
