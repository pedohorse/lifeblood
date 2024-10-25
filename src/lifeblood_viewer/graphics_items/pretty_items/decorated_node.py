from .node_decorator_base import NodeDecorator, NodeDecoratorFactoryBase
from .drawable_node_with_snap_points import DrawableNodeWithSnapPoints

from ..graphics_scene_base import GraphicsSceneBase

from typing import Iterable, List


class DecoratedNode(DrawableNodeWithSnapPoints):
    def __init__(self, scene: GraphicsSceneBase, id: int, type: str, name: str, node_decorator_factories: Iterable[NodeDecoratorFactoryBase] = ()):
        super().__init__(scene, id, type, name)

        # decorators
        self.__decorators: List[NodeDecorator] = [fac.make_decorator(self) for fac in node_decorator_factories]
        for decorator in self.__decorators:
            decorator.setParentItem(self)

    def item_updated(self):
        super().item_updated()
        for decorator in self.__decorators:
            decorator.node_updated()
