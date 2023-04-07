from lifeblood.logging import get_logger
from lifeblood.uidata import Parameter
from .undo_stack import UndoableOperation
from .long_op import LongOperationData
from .ui_snippets import UiNodeSnippetData
from PySide2.QtCore import QPointF

from typing import Callable, TYPE_CHECKING, Optional, List, Mapping, Tuple, Dict, Set, Iterable, Union, Any, Sequence

if TYPE_CHECKING:
    from .graphics_scene import QGraphicsImguiScene

logger = get_logger('scene_op')

__all__ = ['CreateNodeOp', 'CreateNodesOp', 'RemoveNodeOp', 'RemoveNodesOp', 'RenameNodeOp',
           'MoveNodesOp', 'AddConnectionOp', 'RemoveConnectionOp', 'ParameterChangeOp']


class CreateNodeOp(UndoableOperation):
    def __init__(self, scene: "QGraphicsImguiScene", node_sid: int, node_type: str, node_name: str, pos):
        self.__scene: "QGraphicsImguiScene" = scene
        self.__node_sid = node_sid
        self.__node_name = node_name

    def undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def undoop(longop):
            node_id = self.__scene._session_node_id_to_id(self.__node_sid)
            self.__scene.request_remove_node(node_id, LongOperationData(longop))
            yield
            if callback:
                callback(self)
        self.__scene.add_long_operation(undoop)

    def __str__(self):
        return f'Create Node "{self.__node_name}"'


class CreateNodesOp(UndoableOperation):
    def __init__(self, scene: "QGraphicsImguiScene", node_sids: List[int]):
        self.__scene: "QGraphicsImguiScene" = scene
        self.__node_sids = node_sids

    def undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def undoop(longop):
            node_ids = [x for x in (self.__scene._session_node_id_to_id(sid) for sid in self.__node_sids) if x is not None]
            if not node_ids:
                return
            self.__scene.request_remove_nodes(node_ids, LongOperationData(longop))
            yield
            if callback:
                callback(self)
        self.__scene.add_long_operation(undoop)

    def __str__(self):
        return f'Create Nodes "{self.__node_sids}"'


class RemoveNodeOp(UndoableOperation):
    def __init__(self, scene: "QGraphicsImguiScene", node_sid: int, restoration_snippet: UiNodeSnippetData):
        self.__scene = scene
        self.__node_sid = node_sid
        self.__restoration_snippet = restoration_snippet

    def undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def undoop(longop):
            self.__scene.nodes_from_snippet(self.__restoration_snippet, QPointF(*self.__restoration_snippet.pos), longop)
            created_ids = yield
            assert len(created_ids) == 1
            if self.__node_sid != self.__scene.get_node(created_ids[0]).get_session_id():
                logger.warning('undo: couldn\'t restore node session id, updating it...')
                self.__scene._session_node_update_id(self.__node_sid, created_ids[0])
            if callback:
                callback(self)
        self.__scene.add_long_operation(undoop)

    def __str__(self):
        return f'Remove Node {self.__node_sid}'


class RemoveNodesOp(UndoableOperation):
    def __init__(self, scene: "QGraphicsImguiScene", node_sids: Tuple[int, ...], restoration_snippet: UiNodeSnippetData):
        self.__scene = scene
        self.__node_sids = node_sids
        self.__restoration_snippet = restoration_snippet

    def undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def undoop(longop):
            self.__scene.nodes_from_snippet(self.__restoration_snippet, QPointF(*self.__restoration_snippet.pos), longop)
            created_ids = yield
            # HOPEFULLY nodes_from_snippet was able to restore session node ids...
            if callback:
                callback(self)
        self.__scene.add_long_operation(undoop)

    def __str__(self):
        return f'Remove Nodes {",".join(str(x) for x in self.__node_sids)}'


class RenameNodeOp(UndoableOperation):
    def __init__(self, scene: "QGraphicsImguiScene", node_sid: int, old_name: str, new_name: str):
        self.__scene = scene
        self.__node_sid = node_sid
        self.__old_name = old_name
        self.__new_name = new_name

    def undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def undoop(longop):
            node_id = self.__scene._session_node_id_to_id(self.__node_sid)
            self.__scene.request_set_node_name(node_id, self.__old_name, LongOperationData(longop))
            yield
            if callback:
                callback(self)
        self.__scene.add_long_operation(undoop)

    def __str__(self):
        return f'Rename Node {self.__node_sid} {self.__old_name}->{self.__new_name}'


class MoveNodesOp(UndoableOperation):
    def __init__(self, scene: "QGraphicsImguiScene", info: Iterable[Tuple[int, QPointF, QPointF]]):
        self.__scene = scene
        self.__node_info = tuple(info)

    def undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        for node_sid, old_pos, new_pos in self.__node_info:
            node_id = self.__scene._session_node_id_to_id(node_sid)
            node = self.__scene.get_node(node_id)
            if node is None:
                continue
            node.setPos(old_pos)
        if callback:
            callback(self)

    def __str__(self):
        return f'Move Node(s) {",".join(str(x) for x,_,_ in self.__node_info)}'


class AddConnectionOp(UndoableOperation):
    def __init__(self, scene: "QGraphicsImguiScene", out_node_sid: int, out_name: str, in_node_sid: int, in_name: str):
        self.__scene = scene
        self.__out_sid = out_node_sid
        self.__out_name = out_name
        self.__in_sid = in_node_sid
        self.__in_name = in_name

    def undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def undoop(longop):
            out_id = self.__scene._session_node_id_to_id(self.__out_sid)
            in_id = self.__scene._session_node_id_to_id(self.__in_sid)
            if out_id is None or in_id is None \
              or self.__scene.get_node(out_id) is None \
              or self.__scene.get_node(in_id) is None:
                logger.warning(f'could not perform undo: added connection not found: {out_id} {in_id}')
                return
            con = self.__scene.get_node_connection_from_ends(out_id, self.__out_name, in_id, self.__in_name)
            if con is None:
                logger.warning('could not perform undo: added connection not found')
                return
            self.__scene.request_node_connection_remove(con.get_id(), LongOperationData(longop))
            yield
            if callback:
                callback(self)
        self.__scene.add_long_operation(undoop)

    def __str__(self):
        return f'Wire Add {self.__out_sid}:{self.__out_name}->{self.__in_sid}:{self.__in_name}'


class RemoveConnectionOp(UndoableOperation):
    def __init__(self, scene: "QGraphicsImguiScene", out_node_sid: int, out_name: str, in_node_sid: int, in_name: str):
        self.__scene = scene
        self.__out_sid = out_node_sid
        self.__out_name = out_name
        self.__in_sid = in_node_sid
        self.__in_name = in_name

    def undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def undoop(longop):
            out_id = self.__scene._session_node_id_to_id(self.__out_sid)
            in_id = self.__scene._session_node_id_to_id(self.__in_sid)
            if out_id is None or in_id is None \
              or self.__scene.get_node(out_id) is None \
              or self.__scene.get_node(in_id) is None:
                logger.warning('could not perform undo: added connection not found')
                return
            self.__scene.request_node_connection_add(out_id, self.__out_name, in_id, self.__in_name, LongOperationData(longop))
            yield
            if callback:
                callback(self)
        self.__scene.add_long_operation(undoop)

    def __str__(self):
        return f'Wire Remove {self.__out_sid}:{self.__out_name}->{self.__in_sid}:{self.__in_sid}'


class ParameterChangeOp(UndoableOperation):
    def __init__(self, scene: "QGraphicsImguiScene", node_id: int, parameter_name: str, new_value=..., new_expression=...):
        """

        :param scene:
        :param node_id:
        :param parameter:
        :param new_value: ...(Ellipsis) means no change
        :param new_expression: ...(Ellipsis) means no change
        """
        super().__init__()
        self.__scene = scene
        self.__param_name = parameter_name
        param = scene.get_node(node_id).get_nodeui().parameter(parameter_name)
        self.__old_value = param.unexpanded_value() if new_value is not ... else ...
        self.__old_expression = param.expression() if new_expression is not ... else ...
        self.__new_value = new_value
        self.__new_expression = new_expression
        self.__node_id = node_id

    def do(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def doop(longop):
            param = self.__scene.get_node(self.__node_id).get_nodeui().parameter(self.__param_name)
            if self.__new_value is not ...:
                param.set_value(self.__new_value)
            if self.__new_expression is not ...:
                param.set_expression(self.__new_expression)
            self.__scene._send_node_parameters_change(self.__node_id, [param], LongOperationData(longop))
            node = self.__scene.get_node(self.__node_id)
            if node:
                node.update_ui()
            yield
            if callback:
                callback(self)
        self.__scene.add_long_operation(doop)

    def undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def undoop(longop):
            param = self.__scene.get_node(self.__node_id).get_nodeui().parameter(self.__param_name)
            if self.__old_value is not ...:
                param.set_value(self.__old_value)
            if self.__old_expression is not ...:
                param.set_expression(self.__old_expression)
            self.__scene._send_node_parameters_change(self.__node_id, [param], LongOperationData(longop))
            yield
            # update node ui, just in case
            node = self.__scene.get_node(self.__node_id)
            if node:
                node.update_ui()
            # do callbacks
            if callback:
                callback(self)
        self.__scene.add_long_operation(undoop)

    def __str__(self):
        return f'Param Changed {self.__param_name} @ {self.__node_id}'
