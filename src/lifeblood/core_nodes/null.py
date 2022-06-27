from lifeblood.basenode import BaseNode
from lifeblood.nodethings import ProcessingResult

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
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.22, 0.24, 0.22)

    def process_task(self, context) -> ProcessingResult:
        return ProcessingResult()

    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()
