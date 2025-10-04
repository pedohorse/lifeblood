from PySide6.QtWidgets import QGraphicsItem
from .drawable_node import DrawableNode


class NodeDecorator(QGraphicsItem):
    def __init__(self, node: DrawableNode):
        super().__init__()
        self.__node = node
        self.setZValue(-2)  # draw behind everything

    def node(self) -> DrawableNode:
        return self.__node

    def node_updated(self):
        """
        called by the owning node when it's data changes
        """
        pass


class NodeDecoratorFactoryBase:
    def make_decorator(self, node: DrawableNode) -> NodeDecorator:
        raise NotImplementedError()
