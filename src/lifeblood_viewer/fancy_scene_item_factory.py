from .graphics_items import Node, Task, NodeConnection
from .graphics_scene_base import GraphicsSceneBase
from lifeblood.ui_protocol_data import TaskData

from .scene_data_controller import SceneDataController
from .scene_item_factory_base import SceneItemFactoryBase
from .graphics_items.dataaware_graphics_items import SceneNode, SceneTask, SceneNodeConnection


class FancySceneItemFactory(SceneItemFactoryBase):
    def __init__(self, data_controller: SceneDataController):
        self.__data_controller = data_controller

    def set_data_controller(self, data_controller: SceneDataController):
        self.__data_controller = data_controller

    def make_task(self, scene: GraphicsSceneBase, task_data: TaskData) -> Task:
        return SceneTask(scene, task_data, self.__data_controller)

    def make_node(self, scene: GraphicsSceneBase, id: int, type: str, name: str) -> Node:
        return SceneNode(scene, id, type, name, self.__data_controller)

    def make_node_connection(self, scene: GraphicsSceneBase, id: int, nodeout: Node, nodein: Node, outname: str, inname: str) -> NodeConnection:
        return SceneNodeConnection(scene, id, nodeout, nodein, outname, inname, self.__data_controller)
