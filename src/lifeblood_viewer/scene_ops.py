from lifeblood.logging import get_logger
from lifeblood.uidata import Parameter
from .undo_stack import UndoableOperation, OperationError
from .long_op import LongOperationData
from .ui_snippets import UiNodeSnippetData
from .graphics_items import Node, NodeConnection
from lifeblood.snippets import NodeSnippetData
from PySide2.QtCore import QPointF

from typing import Callable, TYPE_CHECKING, Optional, List, Mapping, Tuple, Dict, Set, Iterable, Union, Any, Sequence

if TYPE_CHECKING:
    from .graphics_scene import QGraphicsImguiScene

logger = get_logger('scene_op')

__all__ = ['CreateNodeOp', 'CreateNodesOp', 'RemoveNodesOp', 'RenameNodeOp',
           'MoveNodesOp', 'AddConnectionOp', 'RemoveConnectionOp', 'ParameterChangeOp']


class CreateNodeOp(UndoableOperation):
    def __init__(self, scene: "QGraphicsImguiScene", node_type: str, node_name: str, pos):
        self.__scene: "QGraphicsImguiScene" = scene
        self.__node_sid = None
        self.__node_name = node_name
        self.__node_type = node_type
        self.__node_pos = pos

    def do(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def doop(longop):
            self.__scene._request_create_node(self.__node_type, self.__node_name, self.__node_pos, LongOperationData(longop))
            node_id, node_type, node_name = yield
            self.__node_sid = self.__scene._session_node_id_from_id(node_id)
            if callback:
                callback(self)
        if self.__node_sid is not None:
            raise RuntimeError('cannot do an already done op')
        self.__scene.add_long_operation(doop)

    def undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def undoop(longop):
            node_id = self.__scene._session_node_id_to_id(self.__node_sid)
            self.__scene.request_remove_node(node_id, LongOperationData(longop))
            yield
            self.__node_sid = None
            if callback:
                callback(self)
        if self.__node_sid is None:
            raise RuntimeError('cannot undo what has not been done!')
        self.__scene.add_long_operation(undoop)

    def __str__(self):
        return f'Create Node "{self.__node_name}"'


class CreateNodesOp(UndoableOperation):
    def __init__(self, scene: "QGraphicsImguiScene", creation_snippet: NodeSnippetData, pos: QPointF):
        self.__scene: "QGraphicsImguiScene" = scene
        self.__node_sids = None
        self.__creation_snippet = creation_snippet
        self.__pos = pos

    def do(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def doop(longop):
            self.__scene._request_create_nodes_from_snippet(self.__creation_snippet, self.__pos, longop)
            created_ids = yield
            print(created_ids)
            self.__node_sids = set(self.__scene._session_node_id_from_id(nid) for nid in created_ids)
            print(self.__node_sids)
            if callback:
                callback(self)
        self.__scene.add_long_operation(doop)

    def undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def undoop(longop):
            print(self.__node_sids)
            node_ids = [x for x in (self.__scene._session_node_id_to_id(sid) for sid in self.__node_sids) if x is not None]
            print(node_ids)
            if not node_ids:
                return
            self.__scene._request_remove_nodes(node_ids, LongOperationData(longop))
            yield
            if callback:
                callback(self)
        self.__scene.add_long_operation(undoop)

    def __str__(self):
        return f'Create Nodes "{self.__node_sids}"'


class RemoveNodesOp(UndoableOperation):
    def __init__(self, scene: "QGraphicsImguiScene", nodes: Iterable[Node]):
        self.__scene = scene
        self.__node_sids = tuple(node.get_session_id() for node in nodes)
        self.__restoration_snippet = None

    def do(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def doop(longop):
            node_ids = [self.__scene._session_node_id_to_id(sid) for sid in self.__node_sids]
            nodes = [self.__scene.get_node(nid) for nid in node_ids]
            if any(n is None for n in nodes):
                raise OperationError('some nodes disappeared before operation was done')
            self.__restoration_snippet = UiNodeSnippetData.from_viewer_nodes(nodes, include_dangling_connections=True)
            self.__scene._request_remove_nodes(node_ids, LongOperationData(longop))
            yield
            if callback:
                callback(self)
        self.__scene.add_long_operation(doop)

    def undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def undoop(longop):
            self.__scene._request_create_nodes_from_snippet(self.__restoration_snippet, QPointF(*self.__restoration_snippet.pos), longop)
            created_ids = yield
            sids = set(self.__scene._session_node_id_from_id(nid) for nid in created_ids)
            assert set(self.__node_sids) == sids
            # HOPEFULLY _request_create_nodes_from_snippet was able to restore session node ids...
            if callback:
                callback(self)
        self.__scene.add_long_operation(undoop)

    def __str__(self):
        return f'Remove Nodes {",".join(str(x) for x in self.__node_sids)}'


class RenameNodeOp(UndoableOperation):
    def __init__(self, scene: "QGraphicsImguiScene", node: Node, new_name: str):
        self.__scene = scene
        self.__node_sid = scene._session_node_id_from_id(node.get_id())
        self.__old_name = None
        self.__new_name = new_name

    def do(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def doop(longop):
            node_id = self.__scene._session_node_id_to_id(self.__node_sid)
            node = self.__scene.get_node(node_id)
            if node is None:
                raise OperationError(f'node with session id {self.__node_sid} was not found')
            self.__old_name = node.node_name()
            self.__scene._request_set_node_name(node_id, self.__new_name, LongOperationData(longop))
            yield
            if callback:
                callback(self)

        self.__scene.add_long_operation(doop)

    def undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def undoop(longop):
            node_id = self.__scene._session_node_id_to_id(self.__node_sid)
            node = self.__scene.get_node(node_id)
            if node is None:
                raise OperationError(f'node with session id {self.__node_sid} was not found')
            self.__scene._request_set_node_name(node_id, self.__old_name, LongOperationData(longop))
            yield
            if callback:
                callback(self)
        self.__scene.add_long_operation(undoop)

    def __str__(self):
        return f'Rename Node {self.__node_sid} {self.__old_name}->{self.__new_name}'


class MoveNodesOp(UndoableOperation):
    def __init__(self, scene: "QGraphicsImguiScene", info: Iterable[Tuple[Node, QPointF, Optional[QPointF]]]):
        self.__scene = scene
        self.__node_info = tuple((scene._session_node_id_from_id(node.get_id()), new_pos, old_pos) for node, new_pos, old_pos in info)

    def do(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        for node_sid, new_pos, old_pos in self.__node_info:
            node_id = self.__scene._session_node_id_to_id(node_sid)
            node = self.__scene.get_node(node_id)
            if node is None:
                raise OperationError(f'node with session id {node_sid} was not found')
            node.setPos(new_pos)
        if callback:
            callback(self)

    def undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        for node_sid, new_pos, old_pos in self.__node_info:
            node_id = self.__scene._session_node_id_to_id(node_sid)
            node = self.__scene.get_node(node_id)
            if node is None:
                raise OperationError(f'node with session id {node_sid} was not found')
            node.setPos(old_pos)
        if callback:
            callback(self)

    def __str__(self):
        return f'Move Node(s) {",".join(str(x) for x,_,_ in self.__node_info)}'


class AddConnectionOp(UndoableOperation):
    def __init__(self, scene: "QGraphicsImguiScene", out_node: Node, out_name: str, in_node: Node, in_name: str):
        self.__scene = scene
        self.__out_sid = out_node.get_session_id()
        self.__out_name = out_name
        self.__in_sid = in_node.get_session_id()
        self.__in_name = in_name

    def do(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def doop(longop):
            out_id = self.__scene._session_node_id_to_id(self.__out_sid)
            in_id = self.__scene._session_node_id_to_id(self.__in_sid)
            if out_id is None or in_id is None \
              or self.__scene.get_node(out_id) is None \
              or self.__scene.get_node(in_id) is None:
                logger.warning(f'could not perform op: nodes not found {out_id}, {in_id}')
                return
            self.__scene._request_node_connection_add(out_id, self.__out_name, in_id, self.__in_name, LongOperationData(longop))
            yield
            if callback:
                callback(self)
        self.__scene.add_long_operation(doop)

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
            self.__scene._request_node_connection_remove(con.get_id(), LongOperationData(longop))
            yield
            if callback:
                callback(self)
        self.__scene.add_long_operation(undoop)

    def __str__(self):
        return f'Wire Add {self.__out_sid}:{self.__out_name}->{self.__in_sid}:{self.__in_name}'


class RemoveConnectionOp(UndoableOperation):
    def __init__(self, scene: "QGraphicsImguiScene", out_node: Node, out_name: str, in_node: Node, in_name: str):
        self.__scene = scene
        self.__out_sid = out_node.get_session_id()
        self.__out_name = out_name
        self.__in_sid = in_node.get_session_id()
        self.__in_name = in_name

    def do(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def doop(longop):
            out_id = self.__scene._session_node_id_to_id(self.__out_sid)
            in_id = self.__scene._session_node_id_to_id(self.__in_sid)
            if out_id is None or in_id is None \
                    or self.__scene.get_node(out_id) is None \
                    or self.__scene.get_node(in_id) is None:
                logger.warning(f'could not perform op: added connection not found: {out_id} {in_id}')
                return
            con = self.__scene.get_node_connection_from_ends(out_id, self.__out_name, in_id, self.__in_name)
            if con is None:
                logger.warning(f'could not perform op: added connection not found for {out_id}, {self.__out_name}, {in_id}, {self.__in_name}')
                return
            self.__scene._request_node_connection_remove(con.get_id(), LongOperationData(longop))
            yield
            if callback:
                callback(self)
        self.__scene.add_long_operation(doop)

    def undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def undoop(longop):
            out_id = self.__scene._session_node_id_to_id(self.__out_sid)
            in_id = self.__scene._session_node_id_to_id(self.__in_sid)
            if out_id is None or in_id is None \
              or self.__scene.get_node(out_id) is None \
              or self.__scene.get_node(in_id) is None:
                logger.warning('could not perform undo: added connection not found')
                return
            self.__scene._request_node_connection_add(out_id, self.__out_name, in_id, self.__in_name, LongOperationData(longop))
            yield
            if callback:
                callback(self)
        self.__scene.add_long_operation(undoop)

    def __str__(self):
        return f'Wire Remove {self.__out_sid}:{self.__out_name}->{self.__in_sid}:{self.__in_name}'


class ParameterChangeOp(UndoableOperation):
    def __init__(self, scene: "QGraphicsImguiScene", node: Node, parameter_name: str, new_value=..., new_expression=...):
        """

        :param scene:
        :param node:
        :param parameter_name:
        :param new_value: ...(Ellipsis) means no change
        :param new_expression: ...(Ellipsis) means no change
        """
        super().__init__()
        self.__scene = scene
        self.__param_name = parameter_name
        node_sid = node.get_session_id()
        param = scene.get_node(scene._session_node_id_to_id(node_sid)).get_nodeui().parameter(parameter_name)
        self.__old_value = param.unexpanded_value() if new_value is not ... else ...
        self.__old_expression = param.expression() if new_expression is not ... else ...
        self.__new_value = new_value
        self.__new_expression = new_expression
        self.__node_sid = node_sid

    def do(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def doop(longop):
            node_id = self.__scene._session_node_id_to_id(self.__node_sid)
            param = self.__scene.get_node(node_id).get_nodeui().parameter(self.__param_name)
            if self.__new_value is not ...:
                param.set_value(self.__new_value)
            if self.__new_expression is not ...:
                param.set_expression(self.__new_expression)
            self.__scene._send_node_parameters_change(node_id, [param], LongOperationData(longop))
            node = self.__scene.get_node(node_id)
            if node:
                node.update_ui()
            yield
            if callback:
                callback(self)
        self.__scene.add_long_operation(doop)

    def undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def undoop(longop):
            node_id = self.__scene._session_node_id_to_id(self.__node_sid)
            param = self.__scene.get_node(node_id).get_nodeui().parameter(self.__param_name)
            if self.__old_value is not ...:
                param.set_value(self.__old_value)
            if self.__old_expression is not ...:
                param.set_expression(self.__old_expression)
            self.__scene._send_node_parameters_change(node_id, [param], LongOperationData(longop))
            yield
            # update node ui, just in case
            node = self.__scene.get_node(node_id)
            if node:
                node.update_ui()
            # do callbacks
            if callback:
                callback(self)
        self.__scene.add_long_operation(undoop)

    def __str__(self):
        return f'Param Changed {self.__param_name} @ {self.__node_sid}'
