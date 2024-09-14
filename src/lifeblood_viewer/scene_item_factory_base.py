from .graphics_items import Node, Task, NodeConnection, GraphicsSceneBase
from lifeblood.ui_protocol_data import TaskData


class SceneItemFactoryBase:
    def make_task(self, scene: GraphicsSceneBase, task_data: TaskData) -> Task:
        raise NotImplementedError()

    def make_node(self, scene: GraphicsSceneBase, id: int, type: str, name: str) -> Node:
        raise NotImplementedError()

    def make_node_connection(self, scene: GraphicsSceneBase, id: int, nodeout: Node, nodein: Node, outname: str, inname: str) -> NodeConnection:
        raise NotImplementedError()
