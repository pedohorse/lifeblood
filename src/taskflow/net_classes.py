from typing import TYPE_CHECKING, Type
if TYPE_CHECKING:
    from basenode import BaseNode


class NodeTypeMetadata:
    def __init__(self, node_type: Type["BaseNode"]):
        self.type_name = node_type.type_name()
        self.label = node_type.label()
        self.tags = set(node_type.tags())
        self.description = node_type.description()
