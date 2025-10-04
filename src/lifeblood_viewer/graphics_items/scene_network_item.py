from .network_item import NetworkItemWithUI, NetworkItem
from .qextended_graphics_item import QGraphicsItemExtended
from .graphics_scene_base import GraphicsSceneBase

from PySide6.QtWidgets import QGraphicsItem


class SceneItemCommon(QGraphicsItemExtended):
    def itemChange(self, change: QGraphicsItem.GraphicsItemChange, value):
        if change == QGraphicsItem.GraphicsItemChange.ItemSceneChange:  # just before scene change
            if self.scene() is not None and value is not None:
                raise RuntimeError('changing scenes is not supported')
        return super().itemChange(change, value)

    def item_updated(self):
        """
        should be called when item's state is changed
        """
        self.update()


class SceneNetworkItem(NetworkItem, SceneItemCommon):
    def __init__(self, scene: GraphicsSceneBase, id: int):
        super().__init__(id)
        self.__scene = scene

    def graphics_scene(self) -> GraphicsSceneBase:
        return self.__scene


class SceneNetworkItemWithUI(NetworkItemWithUI, SceneItemCommon):
    def __init__(self, scene: GraphicsSceneBase, id: int):
        super().__init__(id)
        self.__scene = scene

    def graphics_scene(self) -> GraphicsSceneBase:
        return self.__scene
