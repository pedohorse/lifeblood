from .drawable_node import DrawableNode
from PySide6.QtCore import QPointF


class SnapPoint:
    def pos(self) -> QPointF:
        raise NotImplementedError()


class NodeConnSnapPoint(SnapPoint):
    def __init__(self, node: DrawableNode, connection_name: str, connection_is_input: bool):
        super().__init__()
        self.__node = node
        self.__conn_name = connection_name
        self.__isinput = connection_is_input

    def node(self) -> DrawableNode:
        return self.__node

    def connection_name(self) -> str:
        return self.__conn_name

    def connection_is_input(self) -> bool:
        return self.__isinput

    def pos(self) -> QPointF:
        if self.__isinput:
            return self.__node.get_input_position(self.__conn_name)
        return self.__node.get_output_position(self.__conn_name)
