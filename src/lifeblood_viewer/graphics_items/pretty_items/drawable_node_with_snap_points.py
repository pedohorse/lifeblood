from .drawable_node import DrawableNode
from .node_connection_snap_point import NodeConnSnapPoint
from ..graphics_scene_base import GraphicsSceneBase

from typing import List


class DrawableNodeWithSnapPoints(DrawableNode):
    def __init__(self, scene: GraphicsSceneBase, id: int, type: str, name: str):
        super().__init__(scene, id, type, name)

    def input_snap_points(self) -> List[NodeConnSnapPoint]:
        # TODO: cache snap points, don't recalc them every time
        if self.get_nodeui() is None:
            return []
        inputs = []
        for input_name in self.get_nodeui().inputs_names():
            inputs.append(NodeConnSnapPoint(self, input_name, True))
        return inputs

    def output_snap_points(self) -> List[NodeConnSnapPoint]:
        # TODO: cache snap points, don't recalc them every time
        if self.get_nodeui() is None:
            return []
        outputs = []
        for output_name in self.get_nodeui().outputs_names():
            outputs.append(NodeConnSnapPoint(self, output_name, False))
        return outputs
