from lifeblood.logging import get_logger
from .undo_stack import UndoableOperation, StackAwareOperation, SimpleUndoableOperation, OperationError, AsyncOperation
from .long_op import LongOperation, LongOperationData
from .ui_snippets import UiNodeSnippetData
from .graphics_items import Node, NodeConnection
from lifeblood.snippets import NodeSnippetData
from PySide2.QtCore import QPointF

from typing import Callable, TYPE_CHECKING, Optional, List, Mapping, Tuple, Dict, Set, Iterable, Union, Any, Sequence

if TYPE_CHECKING:
    from .graphics_scene import QGraphicsImguiScene

logger = get_logger('scene_op')

__all__ = ['CompoundAsyncSceneOperation', 'CreateNodeOp', 'CreateNodesOp', 'RemoveNodesOp', 'RenameNodeOp',
           'MoveNodesOp', 'AddConnectionOp', 'RemoveConnectionOp', 'ParameterChangeOp']


class AsyncSceneOperation(AsyncOperation):
    def __init__(self, scene: "QGraphicsImguiScene"):
        super().__init__(scene._undo_stack(), scene)


class CompoundAsyncSceneOperation(AsyncSceneOperation):
    def __init__(self, scene: "QGraphicsImguiScene", operations: Iterable[AsyncSceneOperation]):
        super().__init__(scene)
        self.__ops = tuple(operations)

    def _my_do_longop(self, longop: LongOperation):
        for op in self.__ops:
            yield from op._my_do_longop(longop)

    def _my_undo_longop(self, longop: LongOperation):
        for op in reversed(self.__ops):
            yield from op._my_undo_longop(longop)


class CreateNodeOp(AsyncSceneOperation):
    def __init__(self, scene: "QGraphicsImguiScene", node_type: str, node_name: str, pos):
        super().__init__(scene)
        self.__scene: "QGraphicsImguiScene" = scene
        self.__node_sid = None
        self.__node_name = node_name
        self.__node_type = node_type
        self.__node_pos = pos

    def _my_do_longop(self, longop: LongOperation):
        self.__scene._request_create_node(self.__node_type, self.__node_name, self.__node_pos, LongOperationData(longop))
        node_id, node_type, node_name = yield
        self.__node_sid = self.__scene._session_node_id_from_id(node_id)

    def _my_undo_longop(self, longop: LongOperation):
        node_id = self.__scene._session_node_id_to_id(self.__node_sid)
        self.__scene.request_remove_node(node_id, LongOperationData(longop))
        yield
        self.__node_sid = None

    def __str__(self):
        return f'Create Node "{self.__node_name}"'


class CreateNodesOp(AsyncSceneOperation):
    def __init__(self, scene: "QGraphicsImguiScene", creation_snippet: NodeSnippetData, pos: QPointF):
        super().__init__(scene)
        self.__scene: "QGraphicsImguiScene" = scene
        self.__node_sids = None
        self.__creation_snippet = creation_snippet
        self.__pos = pos

    def _my_do_longop(self, longop: LongOperation):
        self.__scene._request_create_nodes_from_snippet(self.__creation_snippet, self.__pos, longop)
        created_ids = yield
        self.__node_sids = set(self.__scene._session_node_id_from_id(nid) for nid in created_ids)

    def _my_undo_longop(self, longop: LongOperation):
        print(self.__node_sids)
        node_ids = [x for x in (self.__scene._session_node_id_to_id(sid) for sid in self.__node_sids) if x is not None]
        print(node_ids)
        if not node_ids:
            return
        self.__scene._request_remove_nodes(node_ids, LongOperationData(longop))
        yield

    def __str__(self):
        return f'Create Nodes "{self.__node_sids}"'


class RemoveNodesOp(AsyncSceneOperation):
    def __init__(self, scene: "QGraphicsImguiScene", nodes: Iterable[Node]):
        super().__init__(scene)
        self.__scene = scene
        self.__node_sids = tuple(node.get_session_id() for node in nodes)
        self.__restoration_snippet = None
        self.__is_a_noop = False

    def _my_do_longop(self, longop: LongOperation):
        node_ids = [self.__scene._session_node_id_to_id(sid) for sid in self.__node_sids]
        nodes = [self.__scene.get_node(nid) for nid in node_ids]
        if any(n is None for n in nodes):
            raise OperationError('some nodes disappeared before operation was done')
        self.__restoration_snippet = UiNodeSnippetData.from_viewer_nodes(nodes, include_dangling_connections=True)
        self.__scene._request_remove_nodes(node_ids, LongOperationData(longop))
        removed_ids, = yield
        # now filter snippet to remove nodes that scheduler failed to remove
        not_removed = set(node_ids) - set(removed_ids)
        not_removed_sids = set(self.__scene.get_node(nid).get_session_id() for nid in not_removed)
        self.__node_sids = tuple(sid for sid in self.__node_sids if sid not in not_removed_sids)
        if len(not_removed) > 0:
            self.__restoration_snippet.remove_node_temp_ids_from_snippet(not_removed_sids)

        # if nothing was deleted:
        if len(self.__restoration_snippet.nodes_data) == 0:
            self.__is_a_noop = True

    def _my_undo_longop(self, longop: LongOperation):
        if self.__is_a_noop:
            return
        self.__scene._request_create_nodes_from_snippet(self.__restoration_snippet, QPointF(*self.__restoration_snippet.pos), longop)
        created_ids = yield
        sids = set(self.__scene._session_node_id_from_id(nid) for nid in created_ids)
        assert set(self.__node_sids) == sids, (sids, set(self.__node_sids))

    def __str__(self):
        if self.__is_a_noop:
            return 'failed attempt to delete nodes'
        return f'Remove Nodes {",".join(str(x) for x in self.__node_sids)}'


class RenameNodeOp(AsyncSceneOperation):
    def __init__(self, scene: "QGraphicsImguiScene", node: Node, new_name: str):
        super().__init__(scene)
        self.__scene = scene
        self.__node_sid = scene._session_node_id_from_id(node.get_id())
        self.__old_name = None
        self.__new_name = new_name

    def _my_do_longop(self, longop: LongOperation):
        node_id = self.__scene._session_node_id_to_id(self.__node_sid)
        node = self.__scene.get_node(node_id)
        if node is None:
            raise OperationError(f'node with session id {self.__node_sid} was not found')
        self.__old_name = node.node_name()
        self.__scene._request_set_node_name(node_id, self.__new_name, LongOperationData(longop))
        yield

    def _my_undo_longop(self, longop: LongOperation):
        node_id = self.__scene._session_node_id_to_id(self.__node_sid)
        node = self.__scene.get_node(node_id)
        if node is None:
            raise OperationError(f'node with session id {self.__node_sid} was not found')
        self.__scene._request_set_node_name(node_id, self.__old_name, LongOperationData(longop))
        yield

    def __str__(self):
        return f'Rename Node {self.__node_sid} {self.__old_name}->{self.__new_name}'


class MoveNodesOp(SimpleUndoableOperation):
    def __init__(self, scene: "QGraphicsImguiScene", info: Iterable[Tuple[Node, QPointF, Optional[QPointF]]]):
        super().__init__(scene._undo_stack(), self._doop, self._undoop)
        self.__scene = scene
        self.__node_info = tuple((scene._session_node_id_from_id(node.get_id()), new_pos, old_pos) for node, new_pos, old_pos in info)

    def _doop(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        for node_sid, new_pos, old_pos in self.__node_info:
            node_id = self.__scene._session_node_id_to_id(node_sid)
            node = self.__scene.get_node(node_id)
            if node is None:
                raise OperationError(f'node with session id {node_sid} was not found')
            node.setPos(new_pos)
        if callback:
            callback(self)

    def _undoop(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
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


class AddConnectionOp(AsyncSceneOperation):
    def __init__(self, scene: "QGraphicsImguiScene", out_node: Node, out_name: str, in_node: Node, in_name: str):
        super().__init__(scene)
        self.__scene = scene
        self.__out_sid = out_node.get_session_id()
        self.__out_name = out_name
        self.__in_sid = in_node.get_session_id()
        self.__in_name = in_name

    def _my_do_longop(self, longop: LongOperation):
        out_id = self.__scene._session_node_id_to_id(self.__out_sid)
        in_id = self.__scene._session_node_id_to_id(self.__in_sid)
        if out_id is None or in_id is None \
          or self.__scene.get_node(out_id) is None \
          or self.__scene.get_node(in_id) is None:
            logger.warning(f'could not perform op: nodes not found {out_id}, {in_id}')
            return
        self.__scene._request_node_connection_add(out_id, self.__out_name, in_id, self.__in_name, LongOperationData(longop))
        yield

    def _my_undo_longop(self, longop: LongOperation):
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

    def __str__(self):
        return f'Wire Add {self.__out_sid}:{self.__out_name}->{self.__in_sid}:{self.__in_name}'


class RemoveConnectionOp(AsyncSceneOperation):
    def __init__(self, scene: "QGraphicsImguiScene", out_node: Node, out_name: str, in_node: Node, in_name: str):
        super().__init__(scene)
        self.__scene = scene
        self.__out_sid = out_node.get_session_id()
        self.__out_name = out_name
        self.__in_sid = in_node.get_session_id()
        self.__in_name = in_name

    def _my_do_longop(self, longop: LongOperation):
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

    def _my_undo_longop(self, longop: LongOperation):
        out_id = self.__scene._session_node_id_to_id(self.__out_sid)
        in_id = self.__scene._session_node_id_to_id(self.__in_sid)
        if out_id is None or in_id is None \
          or self.__scene.get_node(out_id) is None \
          or self.__scene.get_node(in_id) is None:
            logger.warning('could not perform undo: added connection not found')
            return
        self.__scene._request_node_connection_add(out_id, self.__out_name, in_id, self.__in_name, LongOperationData(longop))
        yield

    def __str__(self):
        return f'Wire Remove {self.__out_sid}:{self.__out_name}->{self.__in_sid}:{self.__in_name}'


class ParameterChangeOp(AsyncSceneOperation):
    def __init__(self, scene: "QGraphicsImguiScene", node: Node, parameter_name: str, new_value=..., new_expression=...):
        """

        :param scene:
        :param node:
        :param parameter_name:
        :param new_value: ...(Ellipsis) means no change
        :param new_expression: ...(Ellipsis) means no change
        """
        super().__init__(scene)
        self.__scene = scene
        self.__param_name = parameter_name
        node_sid = node.get_session_id()
        param = scene.get_node(scene._session_node_id_to_id(node_sid)).get_nodeui().parameter(parameter_name)
        self.__old_value = param.unexpanded_value() if new_value is not ... else ...
        self.__old_expression = param.expression() if new_expression is not ... else ...
        self.__new_value = new_value
        self.__new_expression = new_expression
        self.__node_sid = node_sid

    def _my_do_longop(self, longop: LongOperation):
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

    def _my_undo_longop(self, longop: LongOperation):
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

    def __str__(self):
        return f'Param Changed {self.__param_name} @ {self.__node_sid}'
