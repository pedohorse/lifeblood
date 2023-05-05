import sqlite3

import grandalf.graphs
import grandalf.layouts

from types import MappingProxyType
from .graphics_items import Task, Node, NodeConnection
from .db_misc import sql_init_script_nodes
from .long_op import LongOperation, LongOperationData, LongOperationProcessor
from .connection_worker import SchedulerConnectionWorker
from .undo_stack import UndoStack, UndoableOperation, StackLockedError
from .ui_snippets import UiNodeSnippetData
from .scene_ops import *

from lifeblood.misc import timeit, performance_measurer
from lifeblood.uidata import NodeUi, Parameter
from lifeblood.ui_protocol_data import UiData, TaskGroupBatchData, TaskBatchData, NodeGraphStructureData, TaskDelta, DataNotSet, IncompleteInvocationLogData, InvocationLogData
from lifeblood.enums import TaskState, NodeParameterType, TaskGroupArchivedState
from lifeblood import logging
from lifeblood.net_classes import NodeTypeMetadata
from lifeblood.taskspawn import NewTask
from lifeblood.invocationjob import InvocationJob
from lifeblood.snippets import NodeSnippetData, NodeSnippetDataPlaceholder
from lifeblood.environment_resolver import EnvironmentResolverArguments
from lifeblood.ui_events import TaskEvent, TasksRemoved, TasksUpdated, TasksChanged, TaskFullState

from PySide2.QtWidgets import *
from PySide2.QtCore import Slot, Signal, QThread, QRectF, QPointF
from PySide2.QtGui import QKeyEvent

from typing import Callable, Generator, Optional, List, Mapping, Tuple, Dict, Set, Iterable, Union, Any, Sequence

logger = logging.get_logger('viewer')


class QGraphicsImguiScene(QGraphicsScene, LongOperationProcessor):
    # these are private signals to invoke shit on worker in another thread. QMetaObject's invokemethod is broken in pyside2
    _signal_log_has_been_requested = Signal(int)
    _signal_log_meta_has_been_requested = Signal(int)
    _signal_node_ui_has_been_requested = Signal(int)
    _signal_task_ui_attributes_has_been_requested = Signal(int, object)
    _signal_task_invocation_job_requested = Signal(int)
    _signal_node_has_parameter_requested = Signal(int, str, object)
    _signal_node_parameter_change_requested = Signal(int, object, object)
    _signal_node_parameter_expression_change_requested = Signal(int, object, object)
    _signal_node_parameters_change_requested = Signal(int, object, object)
    _signal_node_apply_settings_requested = Signal(int, str, object)
    _signal_node_save_custom_settings_requested = Signal(str, str, object, object)
    _signal_node_set_settings_default_requested = Signal(str, object, object)
    _signal_nodetypes_update_requested = Signal()
    _signal_nodepresets_update_requested = Signal()
    _signal_set_node_name_requested = Signal(int, str, object)
    _signal_nodepreset_requested = Signal(str, str, object)
    _signal_create_node_requested = Signal(str, str, QPointF, object)
    _signal_remove_nodes_requested = Signal(list, object)
    _signal_wipe_node_requested = Signal(int)
    _signal_change_node_connection_requested = Signal(int, object, object, object, object)
    _signal_remove_node_connections_requested = Signal(list, object)
    _signal_add_node_connection_requested = Signal(int, str, int, str, object)
    _signal_set_task_group_filter = Signal(set)
    _signal_set_task_state = Signal(list, TaskState)
    _signal_set_tasks_paused = Signal(object, bool)  # object is Union[List[int], int, str]
    _signal_set_task_group_state_requested = Signal(str, TaskGroupArchivedState)
    _signal_set_task_node_requested = Signal(int, int)
    _signal_set_task_name_requested = Signal(int, str)
    _signal_set_task_groups_requested = Signal(int, set)
    _signal_update_task_attributes_requested = Signal(int, dict, set)
    _signal_cancel_task_requested = Signal(int)
    _signal_add_task_requested = Signal(NewTask)
    _signal_duplicate_nodes_requested = Signal(dict, QPointF)
    _signal_set_skip_dead = Signal(bool)
    _signal_set_skip_archived_groups = Signal(bool)
    _signal_set_environment_resolver_arguments = Signal(int, EnvironmentResolverArguments)
    _signal_unset_environment_resolver_arguments = Signal(int)
    #
    _signal_poke_graph_and_tasks_update = Signal()
    _signal_poke_task_groups_update = Signal()
    _signal_poke_workers_update = Signal()
    #
    #
    nodetypes_updated = Signal(dict)  # TODO: separate worker-oriented "private" signals for readability
    nodepresets_updated = Signal(dict)
    nodepreset_received = Signal(str, str, NodeSnippetData)
    task_invocation_job_fetched = Signal(int, InvocationJob)

    #
    operation_progress_updated = Signal(str, float)  # operation name, progress 0.0 - 1.0

    def __init__(self, db_path: str = None, worker: Optional["SchedulerConnectionWorker"] = None, parent=None):
        super(QGraphicsImguiScene, self).__init__(parent=parent)
        # to debug fuching bsp # self.setItemIndexMethod(QGraphicsScene.NoIndex)
        self.__task_dict: Dict[int, Task] = {}
        self.__node_dict: Dict[int, Node] = {}
        self.__node_connections_dict: Dict[int, NodeConnection] = {}
        self.__db_path = db_path
        self.__nodes_table_name = None
        self.__cached_nodetypes: Dict[str, NodeTypeMetadata] = {}
        self.__cached_nodepresets: Dict[str, Dict[str, Union[NodeSnippetData, NodeSnippetDataPlaceholder]]] = {}
        self.__task_group_filter = None
        self.__db_uid: Optional[int] = None  # this is unique id of the scheduler's db. we use this to determine where to save node positions locally, not to mix and collide

        self.__tasks_to_try_reparent_during_node_update = {}

        self.__undo_stack = UndoStack(max_undos=100)  # TODO: config
        self.__session_node_id_mapping = {}  # for consistent redo-undo involving node creation/deletion, as node_id will change on repetition
        self.__session_node_id_mapping_rev = {}
        self.__next_session_node_id = -1
        self.__node_snapshots = {}  # for undo/redo

        if worker is None:
            self.__ui_connection_thread = QThread(self)  # SchedulerConnectionThread(self)
            self.__ui_connection_worker = SchedulerConnectionWorker()
            self.__ui_connection_worker.moveToThread(self.__ui_connection_thread)

            self.__ui_connection_thread.started.connect(self.__ui_connection_worker.start)
            self.__ui_connection_thread.finished.connect(self.__ui_connection_worker.finish)
        else:
            self.__ui_connection_thread = None
            self.__ui_connection_worker = worker

        self.__long_operations: Dict[int, Tuple[LongOperation, Optional[str]]] = {}
        self.__long_op_queues: Dict[str, List[Callable[["LongOperation"], Generator]]] = {}

        self.__ui_connection_worker.graph_full_update.connect(self.graph_full_update)
        self.__ui_connection_worker.tasks_full_update.connect(self.tasks_full_update)
        self.__ui_connection_worker.tasks_events_arrived.connect(self.tasks_process_events)
        self.__ui_connection_worker.db_uid_update.connect(self.db_uid_update)
        self.__ui_connection_worker.log_fetched.connect(self.log_fetched)
        self.__ui_connection_worker.nodeui_fetched.connect(self.nodeui_fetched)
        self.__ui_connection_worker.task_attribs_fetched.connect(self._task_attribs_fetched)
        self.__ui_connection_worker.task_invocation_job_fetched.connect(self._task_invocation_job_fetched)
        self.__ui_connection_worker.nodetypes_fetched.connect(self._nodetypes_fetched)
        self.__ui_connection_worker.nodepresets_fetched.connect(self._nodepresets_fetched)
        self.__ui_connection_worker.nodepreset_fetched.connect(self._nodepreset_fetched)
        self.__ui_connection_worker.node_has_parameter.connect(self._node_has_parameter)
        self.__ui_connection_worker.node_parameter_changed.connect(self._node_parameter_changed)
        self.__ui_connection_worker.node_parameters_changed.connect(self._node_parameters_changed)
        self.__ui_connection_worker.node_parameter_expression_changed.connect(self._node_parameter_expression_changed)
        self.__ui_connection_worker.node_settings_applied.connect(self._node_settings_applied)
        self.__ui_connection_worker.node_custom_settings_saved.connect(self._node_custom_settings_saved)
        self.__ui_connection_worker.node_default_settings_set.connect(self._node_default_settings_set)
        self.__ui_connection_worker.node_created.connect(self._node_created)
        self.__ui_connection_worker.nodes_removed.connect(self._nodes_removed)
        self.__ui_connection_worker.node_renamed.connect(self._node_renamed)
        self.__ui_connection_worker.nodes_copied.connect(self._nodes_duplicated)
        self.__ui_connection_worker.node_connections_removed.connect(self._node_connections_removed)
        self.__ui_connection_worker.node_connections_added.connect(self._node_connections_added)

        self._signal_log_has_been_requested.connect(self.__ui_connection_worker.get_log)
        self._signal_log_meta_has_been_requested.connect(self.__ui_connection_worker.get_invocation_metadata)
        self._signal_node_ui_has_been_requested.connect(self.__ui_connection_worker.get_nodeui)
        self._signal_task_ui_attributes_has_been_requested.connect(self.__ui_connection_worker.get_task_attribs)
        self._signal_node_has_parameter_requested.connect(self.__ui_connection_worker.send_node_has_parameter)
        self._signal_node_parameter_change_requested.connect(self.__ui_connection_worker.send_node_parameter_change)
        self._signal_node_parameter_expression_change_requested.connect(self.__ui_connection_worker.send_node_parameters_change)
        self._signal_node_parameters_change_requested.connect(self.__ui_connection_worker.send_node_parameters_change)
        self._signal_node_apply_settings_requested.connect(self.__ui_connection_worker.apply_node_settings)
        self._signal_node_save_custom_settings_requested.connect(self.__ui_connection_worker.node_save_custom_settings)
        self._signal_node_set_settings_default_requested.connect(self.__ui_connection_worker.node_set_settings_default)
        self._signal_nodetypes_update_requested.connect(self.__ui_connection_worker.get_nodetypes)
        self._signal_nodepresets_update_requested.connect(self.__ui_connection_worker.get_nodepresets)
        self._signal_nodepreset_requested.connect(self.__ui_connection_worker.get_nodepreset)
        self._signal_set_node_name_requested.connect(self.__ui_connection_worker.set_node_name)
        self._signal_create_node_requested.connect(self.__ui_connection_worker.create_node)
        self._signal_remove_nodes_requested.connect(self.__ui_connection_worker.remove_nodes)
        self._signal_wipe_node_requested.connect(self.__ui_connection_worker.wipe_node)
        self._signal_duplicate_nodes_requested.connect(self.__ui_connection_worker.duplicate_nodes)
        self._signal_change_node_connection_requested.connect(self.__ui_connection_worker.change_node_connection)
        self._signal_remove_node_connections_requested.connect(self.__ui_connection_worker.remove_node_connections)
        self._signal_add_node_connection_requested.connect(self.__ui_connection_worker.add_node_connection)
        self._signal_set_task_state.connect(self.__ui_connection_worker.set_task_state)
        self._signal_set_tasks_paused.connect(self.__ui_connection_worker.set_tasks_paused)
        self._signal_set_task_group_state_requested.connect(self.__ui_connection_worker.set_task_group_archived_state)
        self._signal_set_task_group_filter.connect(self.__ui_connection_worker.set_task_group_filter)
        self._signal_set_task_node_requested.connect(self.__ui_connection_worker.set_task_node)
        self._signal_set_task_name_requested.connect(self.__ui_connection_worker.set_task_name)
        self._signal_set_task_groups_requested.connect(self.__ui_connection_worker.set_task_groups)
        self._signal_update_task_attributes_requested.connect(self.__ui_connection_worker.update_task_attributes)
        self._signal_cancel_task_requested.connect(self.__ui_connection_worker.cancel_task)
        self._signal_add_task_requested.connect(self.__ui_connection_worker.add_task)
        self._signal_task_invocation_job_requested.connect(self.__ui_connection_worker.get_task_invocation_job)
        self._signal_set_skip_dead.connect(self.__ui_connection_worker.set_skip_dead)
        self._signal_set_skip_archived_groups.connect(self.__ui_connection_worker.set_skip_archived_groups)
        self._signal_set_environment_resolver_arguments.connect(self.__ui_connection_worker.set_environment_resolver_arguments)
        self._signal_unset_environment_resolver_arguments.connect(self.__ui_connection_worker.unset_environment_resolver_arguments)
        #
        self._signal_poke_graph_and_tasks_update.connect(self.__ui_connection_worker.poke_graph_and_tasks_update)
        self._signal_poke_task_groups_update.connect(self.__ui_connection_worker.poke_task_groups_update)
        self._signal_poke_workers_update.connect(self.__ui_connection_worker.poke_workers_update)

    def request_log(self, invocation_id: int):
        self._signal_log_has_been_requested.emit(invocation_id)

    def request_log_meta(self, task_id: int):
        self._signal_log_meta_has_been_requested.emit(task_id)

    def request_attributes(self, task_id: int, operation_data: Optional["LongOperationData"] = None):
        self._signal_task_ui_attributes_has_been_requested.emit(task_id, operation_data)

    def request_invocation_job(self, task_id: int):
        self._signal_task_invocation_job_requested.emit(task_id)

    def request_node_ui(self, node_id: int):
        self._signal_node_ui_has_been_requested.emit(node_id)

    def query_node_has_parameter(self, node_id: int, param_name: str, operation_data: Optional["LongOperationData"] = None):
        self._signal_node_has_parameter_requested.emit(node_id, param_name, operation_data)

    def send_node_parameter_change(self, node_id: int, param: Parameter, operation_data: Optional["LongOperationData"] = None):
        self._signal_node_parameter_change_requested.emit(node_id, param, operation_data)

    def send_node_parameter_expression_change(self, node_id: int, param: Parameter, operation_data: Optional["LongOperationData"] = None):
        self._signal_node_parameter_expression_change_requested.emit(node_id, [param], operation_data)

    def _send_node_parameters_change(self, node_id: int, params: Iterable[Parameter], operation_data: Optional["LongOperationData"] = None):
        self._signal_node_parameters_change_requested.emit(node_id, params, operation_data)

    def request_apply_node_settings(self, node_id: int, settings_name: str, operation_data: Optional["LongOperationData"] = None):
        self._signal_node_apply_settings_requested.emit(node_id, settings_name, operation_data)

    def request_save_custom_settings(self, node_type_name: str, settings_name: str, settings: dict, operation_data: Optional["LongOperationData"] = None):
        self._signal_node_save_custom_settings_requested.emit(node_type_name, settings_name, settings, operation_data)

    def request_set_settings_default(self, node_type_name: str, settings_name: Optional[str], operation_data: Optional["LongOperationData"] = None):
        self._signal_node_set_settings_default_requested.emit(node_type_name, settings_name, operation_data)

    def request_node_types_update(self):
        self._signal_nodetypes_update_requested.emit()

    def request_node_presets_update(self):
        self._signal_nodepresets_update_requested.emit()

    def request_node_preset(self, packagename: str, presetname: str, operation_data: Optional["LongOperationData"] = None):
        self._signal_nodepreset_requested.emit(packagename, presetname, operation_data)

    def _request_set_node_name(self, node_id: int, name: str, operation_data: Optional["LongOperationData"] = None):
        self._signal_set_node_name_requested.emit(node_id, name, operation_data)

    def request_node_connection_change(self,  connection_id: int, outnode_id: Optional[int] = None, outname: Optional[str] = None, innode_id: Optional[int] = None, inname: Optional[str] = None):
        self._signal_change_node_connection_requested.emit(connection_id, outnode_id, outname, innode_id, inname)

    def _request_node_connection_remove(self, connection_id: int, operation_data: Optional["LongOperationData"] = None):
        self._signal_remove_node_connections_requested.emit([connection_id], operation_data)

    def _request_node_connection_add(self, outnode_id: int , outname: str, innode_id: int, inname: str,  operation_data: Optional["LongOperationData"] = None):
        self._signal_add_node_connection_requested.emit(outnode_id, outname, innode_id, inname, operation_data)

    def _request_create_node(self, typename: str, nodename: str, pos: QPointF, operation_data: Optional["LongOperationData"] = None):
        self._signal_create_node_requested.emit(typename, nodename, pos, operation_data)

    def request_remove_node(self, node_id: int, operation_data: Optional["LongOperationData"] = None):
        if operation_data is None:
            node = self.get_node(node_id)
            if node is not None:
                self.__node_snapshots[node_id] = UiNodeSnippetData.from_viewer_nodes([node])
        self._signal_remove_nodes_requested.emit([node_id], operation_data)

    def _request_remove_nodes(self, node_ids: List[int], operation_data: Optional["LongOperationData"] = None):
        if operation_data is None:
            for node_id in node_ids:
                node = self.get_node(node_id)
                if node is not None:
                    self.__node_snapshots[node_id] = UiNodeSnippetData.from_viewer_nodes([node])
        self._signal_remove_nodes_requested.emit(node_ids, operation_data)

    def request_wipe_node(self, node_id: int):
        self._signal_wipe_node_requested.emit(node_id)

    def request_duplicate_nodes(self, node_ids: List[int], shift: QPointF):
        self._signal_duplicate_nodes_requested.emit(node_ids, shift)

    def set_task_group_filter(self, groups):
        self._signal_set_task_group_filter.emit(groups)

    def set_task_state(self, task_ids: List[int], state: TaskState):
        self._signal_set_task_state.emit(task_ids, state)

    def set_tasks_paused(self, task_ids_or_groups: List[Union[int, str]], paused: bool):
        if all(isinstance(x, int) for x in task_ids_or_groups):
            self._signal_set_tasks_paused.emit(task_ids_or_groups, paused)
        else:
            for tid_or_group in task_ids_or_groups:
                self._signal_set_tasks_paused.emit(tid_or_group, paused)

    def set_task_group_archived_state(self, group_names: List[str], state: TaskGroupArchivedState):
        for group_name in group_names:
            self._signal_set_task_group_state_requested.emit(group_name, state)

    def request_task_cancel(self, task_id: int):
        self._signal_cancel_task_requested.emit(task_id)

    def request_set_task_node(self, task_id: int, node_id: int):
        self._signal_set_task_node_requested.emit(task_id, node_id)

    def request_add_task(self, new_task: NewTask):
        self._signal_add_task_requested.emit(new_task)

    def request_rename_task(self, task_id: int, new_name: str):
        self._signal_set_task_name_requested.emit(task_id, new_name)

    def request_set_task_groups(self, task_id: int, new_groups: Set[str]):
        self._signal_set_task_groups_requested.emit(task_id, new_groups)

    def request_update_task_attributes(self, task_id: int, attribs_to_update: dict, attribs_to_delete: Set[str]):
        self._signal_update_task_attributes_requested.emit(task_id, attribs_to_update, attribs_to_delete)

    def set_skip_dead(self, do_skip: bool) -> None:
        self._signal_set_skip_dead.emit(do_skip)

    def set_skip_archived_groups(self, do_skip: bool) -> None:
        self._signal_set_skip_archived_groups.emit(do_skip)

    def request_set_environment_resolver_arguments(self, task_id, env_args):
        self._signal_set_environment_resolver_arguments.emit(task_id, env_args)

    def request_unset_environment_resolver_arguments(self, task_id):
        self._signal_unset_environment_resolver_arguments.emit(task_id)

    #

    def request_graph_and_tasks_update(self):
        """
        send a request to the scheduler to update node graph and tasks state immediately
        """
        self._signal_poke_graph_and_tasks_update.emit()

    def request_task_groups_update(self):
        """
        send a request to the scheduler to update task groups state immediately
        """
        self._signal_poke_task_groups_update.emit()

    def request_workers_update(self):
        """
        send a request to the scheduler to update workers state immediately
        """
        self._signal_poke_workers_update.emit()

    #
    #
    #

    def _session_node_id_to_id(self, session_id: int) -> Optional[int]:
        """
        the whole idea of session id is to have it consistent through undo-redos
        """
        node_id = self.__session_node_id_mapping.get(session_id)
        if node_id is not None and self.get_node(node_id) is None:
            self.__session_node_id_mapping.pop(session_id)
            self.__session_node_id_mapping_rev.pop(node_id)
            node_id = None
        return node_id

    def _session_node_update_id(self, session_id: int, new_node_id: int):
        prev_node_id = self.__session_node_id_mapping.get(session_id)
        self.__session_node_id_mapping[session_id] = new_node_id
        if prev_node_id is not None:
            self.__session_node_id_mapping_rev.pop(prev_node_id)
        self.__session_node_id_mapping_rev[new_node_id] = session_id
        # TODO: self.__session_node_id_mapping should be cleared when undo stack is truncated, but so far it's a little "memory leak"

    def _session_node_update_session_id(self, new_session_id: int, node_id: int):
        if new_session_id in self.__session_node_id_mapping:
            raise RuntimeError(f'given session id {new_session_id} is already assigned')
        old_session_id = self.__session_node_id_mapping_rev.get(node_id)
        self.__session_node_id_mapping_rev[node_id] = new_session_id
        if old_session_id is not None:
            self.__session_node_id_mapping.pop(old_session_id)
        self.__session_node_id_mapping[new_session_id] = node_id

    def _session_node_id_from_id(self, node_id: int):
        if node_id not in self.__session_node_id_mapping_rev:
            self._session_node_update_id(self.__next_session_node_id, node_id)
            self.__next_session_node_id -= 1
        return self.__session_node_id_mapping_rev[node_id]

    #
    #

    #
    #

    def skip_dead(self) -> bool:
        return self.__ui_connection_worker.skip_dead()  # should be fine and thread-safe in eyes of python

    def skip_archived_groups(self) -> bool:
        return self.__ui_connection_worker.skip_archived_groups()  # should be fine and thread-safe in eyes of python

    def _nodes_were_moved(self, nodes_datas: Sequence[Tuple[Node, QPointF]]):
        """
        item needs to notify the scene that move operation has happened,
        scene needs to create an undo entry for that
        """

        op = MoveNodesOp(self,
                         ((node, node.pos(), old_pos) for node, old_pos in nodes_datas)
                         )
        op.do()

    def node_position(self, node_id: int):
        if self.__db_path is not None:
            if self.__nodes_table_name is None:
                raise RuntimeError('node positions requested before db uid set')
            with sqlite3.connect(self.__db_path) as con:
                con.row_factory = sqlite3.Row
                cur = con.execute(f'SELECT * FROM "{self.__nodes_table_name}" WHERE "id" = ?', (node_id,))
                row = cur.fetchone()
                if row is not None:
                    return row['posx'], row['posy']

        raise ValueError(f'node id {node_id} has no stored position')

    def set_node_position(self, node_id: int, pos: Union[Tuple[float, float], QPointF]):
        if isinstance(pos, QPointF):
            pos = pos.toTuple()
        if self.__db_path is not None:
            if self.__nodes_table_name is None:
                raise RuntimeError('node positions requested before db uid set')
            with sqlite3.connect(self.__db_path) as con:
                con.row_factory = sqlite3.Row
                cur = con.execute(f'INSERT INTO "{self.__nodes_table_name}" ("id", "posx", "posy") VALUES (?, ?, ?) ON CONFLICT("id") DO UPDATE SET posx = ?, posy = ?', (node_id, *pos, *pos))
                row = cur.fetchone()
                if row is not None:
                    return row['posx'], row['posy']

    def node_types(self) -> MappingProxyType:
        return MappingProxyType(self.__cached_nodetypes)

    #
    # async operations
    #

    def create_node(self, typename: str, nodename: str, pos: QPointF):
        op = CreateNodeOp(self, typename, nodename, pos)
        op.do()

    def delete_selected_nodes(self):
        nodes: List[Node] = []
        for item in self.selectedItems():
            if isinstance(item, Node):
                nodes.append(item)
        if not nodes:
            return

        op = RemoveNodesOp(self, nodes)
        op.do()

    def add_connection(self,  outnode_id: int, outname: str, innode_id: int, inname: str):
        outnode = self.get_node(outnode_id)
        innode = self.get_node(innode_id)

        op = AddConnectionOp(self, outnode, outname, innode, inname)
        op.do()

    def cut_connection(self, outnode_id: int, outname: str, innode_id: int, inname: str):
        outnode = self.get_node(outnode_id)
        innode = self.get_node(innode_id)

        op = RemoveConnectionOp(self, outnode, outname, innode, inname)
        op.do()

    def cut_connection_by_id(self, con_id):
        con = self.get_node_connection(con_id)
        if con is None:
            return
        cin = con.input()
        cout = con.output()

        return self.cut_connection(cout[0].get_id(), cout[1], cin[0].get_id(), cin[1])

    def change_connection(self, from_outnode_id: int, from_outname: str, from_innode_id: int, from_inname: str, *,
                          to_outnode_id: Optional[int] = None, to_outname: Optional[str] = None,
                          to_innode_id: Optional[int] = None, to_inname: Optional[str] = None):
        # TODO: make proper ChangeConnectionOp
        from_outnode = self.get_node(from_outnode_id)
        from_innode = self.get_node(from_innode_id)
        to_outnode = self.get_node(to_outnode_id) if to_outnode_id is not None else None
        to_innode = self.get_node(to_innode_id) if to_innode_id is not None else None

        op1 = RemoveConnectionOp(self, from_outnode, from_outname, from_innode, from_inname)
        op2 = AddConnectionOp(self, to_outnode or from_outnode, to_outname or from_outname,
                              to_innode or from_innode, to_inname or from_inname)

        op = CompoundAsyncSceneOperation(self, (op1, op2))
        op.do()

    def change_connection_by_id(self, con_id, *,
                                to_outnode_id: Optional[int] = None, to_outname: Optional[str] = None,
                                to_innode_id: Optional[int] = None, to_inname: Optional[str] = None):
        con = self.get_node_connection(con_id)
        if con is None:
            return
        cin = con.input()
        cout = con.output()

        return self.change_connection(cout[0].get_id(), cout[1], cin[0].get_id(), cin[1],
                                      to_outnode_id=to_outnode_id, to_outname=to_outname,
                                      to_innode_id=to_innode_id, to_inname=to_inname)

    def change_node_parameter(self, node_id: int, item: Parameter, value: Any = ..., expression=...):
        """

        :param node_id:
        :param item:
        :param value: ... means no change
        :param expression: ... means no change
        :return:
        """
        logger.debug(f'node:{node_id}, changing "{item.name()}" to {repr(value)}/({expression})')
        node_sid = self._session_node_id_from_id(node_id)
        op = ParameterChangeOp(self, self.get_node(node_id), item.name(), value, expression)
        op.do()

    def rename_node(self, node_id: int, new_name: str):
        node = self.get_node(node_id)
        if node is None:
            logger.warning(f'cannot move node: node not found')

        op = RenameNodeOp(self, node, new_name)
        op.do()

    # undoes, also async

    def _undo_stack(self):
        return self.__undo_stack

    def undo(self, count=1) -> List[UndoableOperation]:
        return self.__undo_stack.perform_undo(count)

    def undo_stack_names(self) -> List[str]:
        return self.__undo_stack.operation_names()

    #
    # scheduler update events
    #

    @Slot(object)
    def db_uid_update(self, new_db_uid: int):
        if self.__db_uid is not None and self.__db_uid != new_db_uid:
            logger.info('scheduler\'s database changed. resetting the view...')
            self.save_node_layout()
            self.clear()
            self.__db_uid = None
            self.__nodes_table_name = None
            # this means we probably reconnected to another scheduler, so existing nodes need to be dropped

        if self.__db_uid is None:
            self.__db_uid = new_db_uid
            self.__nodes_table_name = f'nodes_{self.__db_uid}'
            with sqlite3.connect(self.__db_path) as con:
                con.executescript(sql_init_script_nodes.format(db_uid=self.__db_uid))

    @timeit(0.05)
    @Slot(object)
    def graph_full_update(self, graph_data: NodeGraphStructureData):
        if self.__db_uid != graph_data.db_uid:
            logger.warning(f'received node graph update with a differend db uid. Maybe a ghost if scheduler has just switched db. Ignoring. expect: {self.__db_uid}, got: {graph_data.db_uid}')
            return

        to_del = []
        existing_node_ids: Dict[int, Node] = {}
        existing_conn_ids: Dict[int, NodeConnection] = {}

        for item in self.items():
            if isinstance(item, Node):
                if item.get_id() not in graph_data.nodes or item.node_type() != graph_data.nodes[item.get_id()].type:
                    to_del.append(item)
                    continue
                existing_node_ids[item.get_id()] = item
            elif isinstance(item, NodeConnection):
                if item.get_id() not in graph_data.connections:
                    to_del.append(item)
                    continue
                existing_conn_ids[item.get_id()] = item

        # delete things
        for item in to_del:
            self.removeItem(item)

        # removing items might cascade things, like removing node will remove connections to that node
        # so now we need to recheck existing items validity
        for existings in (existing_node_ids, existing_conn_ids):
            for item_id, item in tuple(existings.items()):
                if item.scene() != self:
                    del existings[item_id]

        # create new nodes, update node names (node parameters are NOT part of graph data)
        nodes_to_layout = []
        for id, new_node_data in graph_data.nodes.items():
            if id in existing_node_ids:
                existing_node_ids[id].set_name(new_node_data.name)
                continue
            new_node = Node(id, new_node_data.type, new_node_data.name or f'node #{id}')
            try:
                new_node.setPos(*self.node_position(id))
            except ValueError:
                nodes_to_layout.append(new_node)
            existing_node_ids[id] = new_node
            self.addItem(new_node)

        # now check if there are task updates that we received before node updates
        for task_id, node_id in self.__tasks_to_try_reparent_during_node_update.items():
            if node_id in existing_node_ids:
                task = self.get_task(task_id)
                if task is None:  # may has already been deleted by another tasks update
                    continue
                existing_node_ids[node_id].add_task(task)
            else:
                task = self.get_task(task_id)
                logger.warning(f'could not find node_id {node_id} for an orphaned during update task {task_id} ({task})')


        # add connections
        for id, new_conn_data in graph_data.connections.items():
            if id in existing_conn_ids:
                # ensure connections
                innode, inname = existing_conn_ids[id].input()
                outnode, outname = existing_conn_ids[id].output()
                if innode.get_id() != new_conn_data.in_id or inname != new_conn_data.in_name:
                    existing_conn_ids[id].set_input(existing_node_ids[new_conn_data.in_id], new_conn_data.in_name)
                    existing_conn_ids[id].update()
                if outnode.get_id() != new_conn_data.out_id or outname != new_conn_data.out_name:
                    existing_conn_ids[id].set_output(existing_node_ids[new_conn_data.out_id], new_conn_data.out_name)
                    existing_conn_ids[id].update()
                continue
            new_conn = NodeConnection(id, existing_node_ids[new_conn_data.out_id],
                                      existing_node_ids[new_conn_data.in_id],
                                      new_conn_data.out_name, new_conn_data.in_name)
            existing_conn_ids[id] = new_conn
            self.addItem(new_conn)

        if nodes_to_layout:
            self.layout_nodes(nodes_to_layout)

    @timeit(0.05)
    @Slot(object, bool)
    def tasks_process_events(self, events: List[TaskEvent], first_time_getting_events: bool):
        """

        :param events:
        :param first_time_getting_events: True if it's a first event batch since filter change
        :return:
        """
        for event in events:
            logger.debug(f'event: {event.tiny_repr()}')
            if event.database_uid != self.__db_uid:
                logger.warning(f'received event with a differend db uid. Maybe a ghost if scheduler has just switched db. Ignoring. expect: {self.__db_uid}, got: {event.database_uid}')
                continue

            if isinstance(event, TaskFullState):
                self.tasks_full_update(event.task_data)
            elif isinstance(event, TasksUpdated):
                self.tasks_update(event.task_data)
            elif isinstance(event, TasksChanged):
                self.tasks_deltas_apply(event.task_deltas, animated=not first_time_getting_events)
            elif isinstance(event, TasksRemoved):
                existing_tasks = dict(self.tasks_dict())
                for task_id in event.task_ids:
                    if task_id in existing_tasks:
                        self.removeItem(existing_tasks[task_id])
                        existing_tasks.pop(task_id)

    def tasks_deltas_apply(self, task_deltas: List[TaskDelta], animated: bool = True):
        for task_delta in task_deltas:
            task_id = task_delta.id
            task = self.get_task(task_id)
            if task is None:  # this would be unusual
                logger.warning(f'cannot apply task delta: task {task_id} does not exist')
                continue
            if task_delta.node_id is not DataNotSet:
                node = self.get_node(task_delta.node_id)
                if node is None:
                    logger.warning('node not found during task delta processing, this will probably be fixed during next update')
                    self.__tasks_to_try_reparent_during_node_update[task_id] = task_delta.node_id
            task.apply_task_delta(task_delta, animated=animated)

    @timeit(0.05)
    @Slot(object)
    def tasks_full_update(self, tasks_data: TaskBatchData):
        if self.__db_uid != tasks_data.db_uid:
            logger.warning(f'received node graph update with a differend db uid. Maybe a ghost if scheduler has just switched db. Ignoring. expect: {self.__db_uid}, got: {tasks_data.db_uid}')
            return

        to_del = []
        to_del_tasks = {}
        existing_task_ids: Dict[int, Task] = {}

        for item in self.tasks():
            if item.get_id() not in tasks_data.tasks:
                to_del.append(item)
                if item.node() is not None:
                    if not item.node() in to_del_tasks:
                        to_del_tasks[item.node()] = []
                    to_del_tasks[item.node()].append(item)
                continue
            existing_task_ids[item.get_id()] = item

        for node, tasks in to_del_tasks.items():
            node.remove_tasks(tasks)

        for item in to_del:
            self.removeItem(item)

        # we don't need that cuz scene already takes care of upkeeping task dict
        # # removing items might cascade things, like removing node will remove connections to that node
        # # so now we need to recheck existing items validity
        # # though not consistent scene states should not come in uidata at all
        # for item_id, item in tuple(existing_task_ids.items()):
        #     if item.scene() != self:
        #         del existing_task_ids[item_id]

        self.tasks_update(tasks_data)

    def tasks_update(self, tasks_data: TaskBatchData):
        """
        unlike tasks_full_update - this ONLY applies updates, does not delete anything

        :param tasks_data:
        :param existing_tasks:  optional already computed dict of existing tasks. if none - it will be computed
        :return:
        """

        existing_tasks = dict(self.tasks_dict())

        for id, new_task_data in tasks_data.tasks.items():
            if id not in existing_tasks:
                new_task = Task(new_task_data)
                existing_tasks[id] = new_task
                if new_task_data.split_origin_task_id is not None and new_task_data.split_origin_task_id in existing_tasks:  # TODO: bug: this and below will only work if parent/original tasks were created during previous updates
                    origin_task = existing_tasks[new_task_data.split_origin_task_id]
                    new_task.setPos(origin_task.scenePos())
                elif new_task_data.parent_id is not None and new_task_data.parent_id in existing_tasks:
                    origin_task = existing_tasks[new_task_data.parent_id]
                    new_task.setPos(origin_task.scenePos())
                self.addItem(new_task)
            task = existing_tasks[id]
            existing_node = self.get_node(new_task_data.node_id)
            if existing_node:
                existing_node.add_task(task)
            else:
                self.__tasks_to_try_reparent_during_node_update[id] = new_task_data.node_id
            task.set_task_data(new_task_data)

    @timeit(0.05)
    @Slot(object)
    def full_update(self, uidata: UiData):
        raise DeprecationWarning('no use')
        # logger.debug('full_update')

        if self.__db_uid is not None and self.__db_uid != uidata.db_uid:
            logger.info('scheduler\'s database changed. resetting the view...')
            self.save_node_layout()
            self.clear()
            self.__db_uid = None
            self.__nodes_table_name = None
            # this means we probably reconnected to another scheduler, so existing nodes need to be dropped

        if self.__db_uid is None:
            self.__db_uid = uidata.db_uid
            self.__nodes_table_name = f'nodes_{self.__db_uid}'
            with sqlite3.connect(self.__db_path) as con:
                con.executescript(sql_init_script_nodes.format(db_uid=self.__db_uid))

        to_del = []
        to_del_tasks = {}
        existing_node_ids: Dict[int, Node] = {}
        existing_conn_ids: Dict[int, NodeConnection] = {}
        existing_task_ids: Dict[int, Task] = {}
        _perf_total = 0.0
        graph_data = uidata.graph_data
        with performance_measurer() as pm:
            for item in self.items():
                if isinstance(item, Node):  # TODO: unify this repeating code and move the setting attribs to after all elements are created
                    if item.get_id() not in graph_data.nodes or item.node_type() != graph_data.nodes[item.get_id()].type:
                        to_del.append(item)
                        continue
                    existing_node_ids[item.get_id()] = item
                    # TODO: update all kind of attribs here, for now we just don't have any
                elif isinstance(item, NodeConnection):
                    if item.get_id() not in graph_data.connections:
                        to_del.append(item)
                        continue
                    existing_conn_ids[item.get_id()] = item
                    # TODO: update all kind of attribs here, for now we just don't have any
                elif isinstance(item, Task):
                    if item.get_id() not in uidata.tasks.tasks:
                        to_del.append(item)
                        if item.node() is not None:
                            if not item.node() in to_del_tasks:
                                to_del_tasks[item.node()] = []
                            to_del_tasks[item.node()].append(item)
                        continue
                    existing_task_ids[item.get_id()] = item
        _perf_item_classify = pm.elapsed()
        _perf_total += pm.elapsed()

        # before we delete everything - we'll remove tasks from nodes to avoid deleting tasks one by one triggering tonns of animation
        with performance_measurer() as pm:
            for node, tasks in to_del_tasks.items():
                node.remove_tasks(tasks)
        _perf_remove_tasks = pm.elapsed()
        _perf_total += pm.elapsed()
        with performance_measurer() as pm:
            for item in to_del:
                self.removeItem(item)
        _perf_remove_items = pm.elapsed()
        _perf_total += pm.elapsed()
        # removing items might cascade things, like removing node will remove connections to that node
        # so now we need to recheck existing items validity
        # though not consistent scene states should not come in uidata at all
        with performance_measurer() as pm:
            for existings in (existing_node_ids, existing_task_ids, existing_conn_ids):
                for item_id, item in tuple(existings.items()):
                    if item.scene() != self:
                        del existings[item_id]
        _perf_revalidate = pm.elapsed()
        _perf_total += pm.elapsed()

        nodes_to_layout = []
        with performance_measurer() as pm:
            for id, new_node_data in graph_data.nodes.items():
                if id in existing_node_ids:
                    existing_node_ids[id].set_name(new_node_data.name)
                    continue
                new_node = Node(id, new_node_data.type, new_node_data.name or f'node #{id}')
                try:
                    new_node.setPos(*self.node_position(id))
                except ValueError:
                    nodes_to_layout.append(new_node)
                existing_node_ids[id] = new_node
                self.addItem(new_node)
        _perf_create_nodes = pm.elapsed()
        _perf_total += pm.elapsed()

        with performance_measurer() as pm:
            for id, new_conn_data in graph_data.connections.items():
                if id in existing_conn_ids:
                    # ensure connections
                    innode, inname = existing_conn_ids[id].input()
                    outnode, outname = existing_conn_ids[id].output()
                    if innode.get_id() != new_conn_data.in_id or inname != new_conn_data.in_name:
                        existing_conn_ids[id].set_input(existing_node_ids[new_conn_data.in_id],  new_conn_data.in_name)
                        existing_conn_ids[id].update()
                    if outnode.get_id() != new_conn_data.out_id or outname != new_conn_data.out_name:
                        existing_conn_ids[id].set_output(existing_node_ids[new_conn_data.out_id], new_conn_data.out_name)
                        existing_conn_ids[id].update()
                    continue
                new_conn = NodeConnection(id, existing_node_ids[new_conn_data.out_id],
                                          existing_node_ids[new_conn_data.in_id],
                                          new_conn_data.out_name, new_conn_data.in_name)
                existing_conn_ids[id] = new_conn
                self.addItem(new_conn)
        _perf_create_connections = pm.elapsed()
        _perf_total += pm.elapsed()

        with performance_measurer() as pm:
            for id, new_task_data in uidata.tasks.tasks.items():
                if id not in existing_task_ids:
                    new_task = Task(new_task_data)
                    existing_task_ids[id] = new_task
                    if new_task_data.split_origin_task_id is not None and new_task_data.split_origin_task_id in existing_task_ids:  # TODO: bug: this and below will only work if parent/original tasks were created during previous updates
                        origin_task = existing_task_ids[new_task_data.split_origin_task_id]
                        new_task.setPos(origin_task.scenePos())
                    elif new_task_data.parent_id is not None and new_task_data.parent_id in existing_task_ids:
                        origin_task = existing_task_ids[new_task_data.parent_id]
                        new_task.setPos(origin_task.scenePos())
                    self.addItem(new_task)
                task = existing_task_ids[id]
                existing_node_ids[new_task_data.node_id].add_task(task)
                task.set_task_data(new_task_data)
        _perf_create_tasks = pm.elapsed()
        _perf_total += pm.elapsed()

        # now layout nodes that need it
        with performance_measurer() as pm:
            if nodes_to_layout:
                self.layout_nodes(nodes_to_layout)
        _perf_layout = pm.elapsed()
        _perf_total += pm.elapsed()

        with performance_measurer() as pm:
            if self.__all_task_groups != uidata.task_groups:
                self.__all_task_groups = uidata.task_groups
                self.task_groups_updated.emit(uidata.task_groups)
        _perf_task_groups_update = pm.elapsed()
        _perf_total += pm.elapsed()

        if _perf_total > 0.04:  # arbitrary threshold ~ 1/25 of a sec
            logger.debug(f'update performed:\n'
                         f'{_perf_item_classify:.04f}:\tclassify\n'
                         f'{_perf_remove_tasks:.04f}:\tremove tasks\n'
                         f'{_perf_remove_items:.04f}:\tremove items\n'
                         f'{_perf_revalidate:.04f}:\trevalidate\n'
                         f'{_perf_create_nodes:.04f}:\tcreate nodes\n'
                         f'{_perf_create_connections:.04f}:\tcreate connections\n'
                         f'{_perf_create_tasks:.04f}:\tcreate tasks\n'
                         f'{_perf_layout:.04f}:\tlayout\n'
                         f'{_perf_task_groups_update:.04f}:\ttask group update')

    @Slot(object, object)
    def log_fetched(self, task_id: int, log: Dict[int, Dict[int, Union[IncompleteInvocationLogData, InvocationLogData]]]):
        task = self.get_task(task_id)
        if task is None:
            logger.warning(f'log fetched, but task not found! {task_id}')
            return
        task.update_log(log)

    @Slot(object, object)
    def nodeui_fetched(self, node_id: int, nodeui: NodeUi):
        node = self.get_node(node_id)
        if node is None:
            logger.warning('node ui fetched for non existant node')
            return
        node.update_nodeui(nodeui)

    @Slot(object, object, object)
    def _task_attribs_fetched(self, task_id: int, all_attribs: Tuple[dict, Optional[EnvironmentResolverArguments]], data: Optional["LongOperationData"] = None):
        task = self.get_task(task_id)
        if task is None:
            logger.warning('attribs fetched, but task not found!')
            return
        attribs, env_attribs = all_attribs
        task.update_attributes(attribs)
        task.set_environment_attributes(env_attribs)
        if data is not None:
            data.data = attribs
            self.process_operation(data)

    @Slot(object, object)
    def _task_invocation_job_fetched(self, task_id: int, invjob: InvocationJob):
        self.task_invocation_job_fetched.emit(task_id, invjob)

    @Slot(int, str, bool, object)
    def _node_has_parameter(self, node_id, param_name, exists, data: Optional["LongOperationData"] = None):
        if data is not None:
            data.data = (node_id, param_name, exists)
            self.process_operation(data)

    @Slot(int, object, object, object)
    def _node_parameter_changed(self, node_id, param, newval, data: Optional["LongOperationData"] = None):
        if data is not None:
            data.data = (node_id, param.name(), newval)
            self.process_operation(data)

    @Slot(int, object, object, object)
    def _node_parameters_changed(self, node_id, params, newvals, data: Optional["LongOperationData"] = None):
        if data is not None:
            data.data = (node_id, tuple(param.name() for param in params), newvals)
            self.process_operation(data)

    @Slot(int, object, object)
    def _node_parameter_expression_changed(self, node_id, param, data: Optional["LongOperationData"] = None):
        if data is not None:
            data.data = (node_id, param.name())
            self.process_operation(data)

    @Slot(int, object, object)
    def _node_settings_applied(self, node_id, settings_name, data: Optional["LongOperationData"] = None):
        node = self.get_node(node_id)
        if node is not None:
            self.request_node_ui(node_id)
        if data is not None:
            data.data = (node_id, settings_name)  # TODO: add return status here?
            self.process_operation(data)

    @Slot(str, str, object)
    def _node_custom_settings_saved(self, type_name: str, settings_name: str, data: Optional["LongOperationData"] = None):
        if data is not None:
            data.data = (type_name, settings_name)  # TODO: add return status here?
            self.process_operation(data)

    @Slot(str, str, object)
    def _node_default_settings_set(self, type_name: str, settings_name: Optional[str], data: Optional["LongOperationData"] = None):
        if data is not None:
            data.data = (type_name, settings_name)  # TODO: add return status here?
            self.process_operation(data)

    @Slot(int, str, str, object, object)
    def _node_created(self, node_id, node_type, node_name, pos, data: Optional["LongOperationData"] = None):
        node = Node(node_id, node_type, node_name)
        node.setPos(pos)
        self.addItem(node)
        if data is not None:
            data.data = (node_id, node_type, node_name)
            self.process_operation(data)

    def _nodes_removed(self, node_ids: List[int], data: Optional["LongOperationData"] = None):
        for node_id in node_ids:
            node = self.get_node(node_id)
            if node is not None:
                self.removeItem(node)
        if data is not None:
            data.data = (node_ids,)
            self.process_operation(data)

    def _node_renamed(self, node_id: int, new_name: str, data: Optional["LongOperationData"] = None):
        node = self.get_node(node_id)
        if node is not None:
            old_name = node.node_name()
            node.set_name(new_name)
        if data is not None:
            data.data = (node_id, new_name)
            self.process_operation(data)

    @Slot(object, object)
    def _nodes_duplicated(self, old_to_new: Dict[int, int], shift: QPointF):
        for old_id, new_id in old_to_new.items():
            old_pos = QPointF()
            old_node = self.get_node(old_id)
            if old_node is not None:
                old_pos = old_node.pos()
            self.set_node_position(new_id, old_pos + shift)

    @Slot(list, object)
    def _node_connections_removed(self, con_ids: List[int], data: Optional["LongOperationData"] = None):
        for con_id in con_ids:
            con = self.get_node_connection(con_id)
            if con is not None:
                self.removeItem(con)
        if data is not None:
            data.data = (con_ids,)
            self.process_operation(data)

    @Slot(list, object)
    def _node_connections_added(self, cons: List[Tuple[int, int, str, int, str]], data: Optional["LongOperationData"] = None):
        for new_id, outnode_id, outname, innode_id, inname in cons:
            outnode = self.get_node(outnode_id)
            innode = self.get_node(innode_id)
            if outnode is None or innode is None:
                return
            new_conn = NodeConnection(new_id, outnode, innode, outname, inname)
            self.addItem(new_conn)
        if data is not None:
            data.data = ([x[0] for x in cons],)
            self.process_operation(data)

    @Slot(object)
    def _nodetypes_fetched(self, nodetypes):
        self.__cached_nodetypes = nodetypes
        self.nodetypes_updated.emit(nodetypes)

    @Slot(object)
    def _nodepresets_fetched(self, nodepresets: Dict[str, Dict[str, Union[NodeSnippetData, NodeSnippetDataPlaceholder]]]):
        # here we receive just the list of names, no contents, so we dont just update dicts
        for package, presets in nodepresets.items():
            self.__cached_nodepresets.setdefault(package, {})
            for preset_name, preset_meta in presets.items():
                if preset_name not in self.__cached_nodepresets[package]:
                    self.__cached_nodepresets[package][preset_name] = preset_meta
            presets_set = set(presets)
            keys_to_del = []
            for key in self.__cached_nodepresets[package]:
                if key not in presets_set:
                    keys_to_del.append(key)
            for key in keys_to_del:
                del self.__cached_nodepresets[package][key]

        self.nodepresets_updated.emit(self.__cached_nodepresets)
        # {(pack, label): snippet for pack, packpres in self.__cached_nodepresets.items() for label, snippet in packpres.items()}

    @Slot(object, object)
    def _nodepreset_fetched(self, package: str, preset: str, snippet: NodeSnippetData, data: Optional["LongOperationData"] = None):
        self.nodepreset_received.emit(package, preset, snippet)
        if data is not None:
            data.data = (package, preset, snippet)
            self.process_operation(data)

    def addItem(self, item):
        logger.debug('adding item %s', item)
        super(QGraphicsImguiScene, self).addItem(item)
        if isinstance(item, Task):
            self.__task_dict[item.get_id()] = item
        elif isinstance(item, Node):
            self.__node_dict[item.get_id()] = item
        elif isinstance(item, NodeConnection):
            self.__node_connections_dict[item.get_id()] = item
        logger.debug('added item')

    def removeItem(self, item):
        logger.debug('removing item %s', item)
        if item.scene() != self:
            logger.debug('item was already removed, just removing ids from internal caches')
        else:
            super(QGraphicsImguiScene, self).removeItem(item)
        if isinstance(item, Task):
            assert item.get_id() in self.__task_dict, 'inconsistency in internal caches. maybe item was doubleremoved?'
            del self.__task_dict[item.get_id()]
        elif isinstance(item, Node):
            assert item.get_id() in self.__node_dict, 'inconsistency in internal caches. maybe item was doubleremoved?'
            del self.__node_dict[item.get_id()]
        elif isinstance(item, NodeConnection):
            assert item.get_id() in self.__node_connections_dict
            self.__node_connections_dict.pop(item.get_id())
        logger.debug('item removed')

    def clear(self):
        logger.debug('clearing the scene...')
        super(QGraphicsImguiScene, self).clear()
        self.__task_dict = {}
        self.__node_dict = {}
        logger.debug('scene cleared')

    @Slot(NodeSnippetData, QPointF)
    def nodes_from_snippet(self, snippet: NodeSnippetData, pos: QPointF, containing_long_op: Optional[LongOperation] = None):
        op = CreateNodesOp(self, snippet, pos)
        op.do()

    def _request_create_nodes_from_snippet(self, snippet: NodeSnippetData, pos: QPointF, containing_long_op: Optional[LongOperation] = None):
        def pasteop(longop):

            tmp_to_new: Dict[int, int] = {}
            created_nodes = []  # select delayed to ensure it happens after all changes to parameters

            # for ui progress
            total_elements = len(snippet.nodes_data) + len(snippet.connections_data)
            current_element = 0
            opname = 'pasting nodes'

            for nodedata in snippet.nodes_data:
                current_element += 1
                if total_elements > 1:
                    self.operation_progress_updated.emit(opname, current_element/(total_elements-1))
                self._request_create_node(nodedata.type, nodedata.name, QPointF(*nodedata.pos) + pos - QPointF(*snippet.pos), LongOperationData(longop, None))
                # NOTE: there is currently no mechanism to ensure order of results when more than one things are requested
                #  from the same operation. So we request and wait things one by one
                node_id, _, _ = yield
                tmp_to_new[nodedata.tmpid] = node_id
                created_nodes.append(node_id)

                proxy_params = []
                for param_name, param_data in nodedata.parameters.items():
                    proxy_param = Parameter(param_name, None, param_data.type, param_data.uvalue)
                    if param_data.expr is not None:
                        proxy_param.set_expression(param_data.expr)
                    proxy_params.append(proxy_param)
                self._send_node_parameters_change(node_id, proxy_params, LongOperationData(longop, None))
                yield

            for node_id in created_nodes:  # selecting
                self.get_node(node_id).setSelected(True)

            # assign session ids to new nodes, prefer tmp ids from the snippet
            for tmp_id, node_id in tmp_to_new.items():
                if self._session_node_id_to_id(tmp_id) is None:  # session id is free
                    self._session_node_update_session_id(tmp_id, node_id)

            for conndata in snippet.connections_data:
                current_element += 1
                if total_elements > 1:
                    self.operation_progress_updated.emit(opname, current_element / (total_elements - 1))

                con_out = tmp_to_new.get(conndata.tmpout, self._session_node_id_to_id(conndata.tmpout))
                con_in = tmp_to_new.get(conndata.tmpin, self._session_node_id_to_id(conndata.tmpin))
                if con_out is None or con_in is None:
                    logger.warning('failed to create connection during snippet creation!')
                    continue
                self._request_node_connection_add(con_out, conndata.out_name,
                                                  con_in, conndata.in_name, LongOperationData(longop))
                yield

            if total_elements > 1:
                self.operation_progress_updated.emit(opname, 1.0)
            if containing_long_op is not None:
                self.process_operation(LongOperationData(containing_long_op, tuple(created_nodes)))

        self.clearSelection()
        self.add_long_operation(pasteop)

    @Slot(str, str, dict)
    def save_nodetype_settings(self, node_type_name: str, settings_name: str, settings: Dict[str, Any]):
        def savesettingsop(longop):
            self.request_save_custom_settings(node_type_name, settings_name, settings, longop.new_op_data())
            yield  # wait for operation to complete
            self.request_node_types_update()

        self.add_long_operation(savesettingsop)

    @Slot(LongOperationData)
    def process_operation(self, op: LongOperationData):
        assert op.op.opid() in self.__long_operations
        done = op.op._progress(op.data)
        if done:
            self.__end_long_operation(op.op.opid())

    def add_long_operation(self, generator_to_call, queue_name: Optional[str] = None):
        newop = LongOperation(generator_to_call)
        if queue_name is not None:
            queue = self.__long_op_queues.setdefault(queue_name, [])
            queue.insert(0, generator_to_call)
            if len(queue) > 1:  # if there is already something in there beside us
                return
        self.__long_operations[newop.opid()] = (newop, queue_name)
        if not newop._start():
            self.__end_long_operation(newop.opid())

    def __end_long_operation(self, opid):
        op, queue_name = self.__long_operations.pop(opid)
        if queue_name is None:
            return
        queue = self.__long_op_queues[queue_name]
        assert len(queue) > 0
        queue.pop()  # popping ourserves
        if len(queue) > 0:
            newop = LongOperation(queue[-1])
            self.__long_operations[newop.opid()] = (newop, queue_name)
            if not newop._start():
                self.__end_long_operation(newop.opid())

    #
    # query
    #

    def get_task(self, task_id) -> Optional[Task]:
        return self.__task_dict.get(task_id, None)

    def get_node(self, node_id) -> Optional[Node]:
        return self.__node_dict.get(node_id, None)

    def get_node_by_session_id(self, node_session_id) -> Optional[Node]:
        node_id = self._session_node_id_to_id(node_session_id)
        if node_id is None:
            return None
        return self.__node_dict.get(node_id, None)

    def get_node_connection(self, con_id) -> Optional[NodeConnection]:
        return self.__node_connections_dict.get(con_id, None)

    def get_node_connection_from_ends(self, out_id, out_name, in_id, in_name) -> Optional[NodeConnection]:
        for con in self.__node_connections_dict.values():
            onode, oname = con.output()
            inode, iname = con.input()
            if (onode.get_id(), oname) == (out_id, out_name) \
               and (inode.get_id(), iname) == (in_id, in_name):
                return con

    def nodes(self) -> Tuple[Node]:
        return tuple(self.__node_dict.values())

    def tasks(self) -> Tuple[Task]:
        return tuple(self.__task_dict.values())

    def tasks_dict(self) -> Mapping[int, Task]:
        return MappingProxyType(self.__task_dict)

    def find_nodes_by_name(self, name: str, match_partly=False) -> Set[Node]:
        if match_partly:
            match_fn = lambda x,y: x in y
        else:
            match_fn = lambda x,y: x == y
        matched = set()
        for node in self.__node_dict.values():
            if match_fn(name, node.node_name()):
                matched.add(node)

        return matched

    def get_inspected_item(self) -> Optional[QGraphicsItem]:
        """
        returns item that needs to be inspected.
        It's parameters should be displayed
        generally, it's the first selected item
        """
        sel = self.selectedItems()
        if len(sel) == 0:
            return None
        return sel[0]

    #
    #
    #

    def start(self):
        if self.__ui_connection_thread is None:
            return
        self.__ui_connection_thread.start()

    def stop(self):
        if self.__ui_connection_thread is None:
            for meth in dir(self):  # disconnect all signals from worker slots
                if not meth.startswith('_signal_'):
                    continue
                try:
                    getattr(self, meth).disconnect()
                except RuntimeError as e:
                    logger.warning(f'error disconnecting signal {meth}: {e}')

            # disconnect from worker's signals too
            self.__ui_connection_worker.disconnect(self)
            return
        # if thread is not none - means we created thread AND worker, so we manage them both
        self.__ui_connection_worker.request_interruption()
        self.__ui_connection_thread.exit()
        self.__ui_connection_thread.wait()

    def save_node_layout(self):
        if self.__db_path is None:
            return

        nodes_to_save = [item for item in self.items() if isinstance(item, Node)]
        if len(nodes_to_save) == 0:
            return
        if self.__db_uid is None:
            logger.warning('db uid is not set while saving nodes')

        if self.__nodes_table_name is None:
            raise RuntimeError('node positions requested before db uid set')

        with sqlite3.connect(self.__db_path) as con:
            con.row_factory = sqlite3.Row
            for item in nodes_to_save:
                con.execute(f'INSERT OR REPLACE INTO "{self.__nodes_table_name}" ("id", "posx", "posy") '
                            f'VALUES (?, ?, ?)', (item.get_id(), *item.pos().toTuple()))
            con.commit()

    def keyPressEvent(self, event: QKeyEvent) -> None:
        for item in self.selectedItems():
            item.keyPressEvent(event)
        event.accept()
        #return super(QGraphicsImguiScene, self).keyPressEvent(event)

    def keyReleaseEvent(self, event: QKeyEvent) -> None:
        for item in self.selectedItems():
            item.keyReleaseEvent(event)
        event.accept()
        #return super(QGraphicsImguiScene, self).keyReleaseEvent(event)

    # this will also catch accumulated events that wires ignore to determine the losest wire
    def mousePressEvent(self, event: QGraphicsSceneMouseEvent) -> None:
        event.wire_candidates = []
        super(QGraphicsImguiScene, self).mousePressEvent(event)
        logger.debug(f'press mouse grabber={self.mouseGrabberItem()}')
        if not event.isAccepted() and len(event.wire_candidates) > 0:
            print([x[0] for x in event.wire_candidates])
            closest = min(event.wire_candidates, key=lambda x: x[0])
            closest[1].post_mousePressEvent(event)  # this seem a bit unsafe, at least not typed statically enough

    def mouseReleaseEvent(self, event: QGraphicsSceneMouseEvent) -> None:
        super(QGraphicsImguiScene, self).mouseReleaseEvent(event)
        logger.debug(f'release mouse grabber={self.mouseGrabberItem()}')

    def setSelectionArea(self, *args, **kwargs):
        pass

    #
    # layout
    def layout_nodes(self, nodes: Optional[Iterable[Node]] = None, center: Optional[QPointF] = None):
        if center is None:
            center = QPointF(0, 0)
        if nodes is None:
            nodes = self.nodes()

        if not nodes:
            return

        nodes_to_vertices = {x: grandalf.graphs.Vertex(x) for x in nodes}
        graph = grandalf.graphs.Graph(nodes_to_vertices.values())
        lower_fixed = []
        upper_fixed = []

        for node in nodes:
            for output_name in node.output_names():
                for conn in node.output_connections(output_name):
                    nextnode, _ = conn.input()
                    if nextnode not in nodes_to_vertices and nextnode not in lower_fixed:
                        lower_fixed.append(nextnode)
                    if nextnode not in lower_fixed and nextnode not in upper_fixed:
                        graph.add_edge(grandalf.graphs.Edge(nodes_to_vertices[node], nodes_to_vertices[nextnode]))
            for input_name in node.input_names():
                for conn in node.input_connections(input_name):
                    prevnode, _ = conn.output()
                    if prevnode not in nodes_to_vertices and prevnode not in upper_fixed:
                        upper_fixed.append(prevnode)
                    if prevnode not in lower_fixed and prevnode not in upper_fixed:
                        # double edges will be filtered by networkx, and we wont miss any connection to external nodes this way
                        graph.add_edge(grandalf.graphs.Edge(nodes_to_vertices[prevnode], nodes_to_vertices[node]))

        upper_middle_point = QPointF(0, float('inf'))
        lower_middle_point = None
        if len(lower_fixed) > 0:
            for lower in lower_fixed:
                upper_middle_point.setX(upper_middle_point.x() + lower.pos().x())
                upper_middle_point.setY(min(upper_middle_point.y(), lower.pos().y()))
            upper_middle_point.setX(upper_middle_point.x() / len(lower_fixed))
        else:
            upper_middle_point = center

        if len(upper_fixed) > 0:
            lower_middle_point = QPointF(0, -float('inf'))
            for upper in upper_fixed:
                lower_middle_point.setX(lower_middle_point.x() + upper.pos().x())
                lower_middle_point.setY(max(lower_middle_point.y(), upper.pos().y()))
            lower_middle_point.setX(lower_middle_point.x() / len(upper_fixed))

        class _viewhelper:
            def __init__(self, w, h):
                self.w = w
                self.h = h

        for node, vert in nodes_to_vertices.items():
            bounds = node.boundingRect()  # type: QRectF
            vert.view = _viewhelper(*bounds.size().toTuple())
            vert.view.h *= 1.5

        vertices_to_nodes = {v: k for k, v in nodes_to_vertices.items()}

        xshift = 0
        nodewidgh = next(graph.V()).view.w  # just take first for now
        nodeheight = nodewidgh
        upper_middle_point -= QPointF(0, 1.5*nodeheight)
        if lower_middle_point is not None:
            lower_middle_point += QPointF(0, 1.5*nodeheight)
        #graph.C[0].layers[0].sV[0]
        for component in graph.C:
            layout = grandalf.layouts.SugiyamaLayout(component)
            layout.init_all()
            layout.draw()

            xmax = -float('inf')
            ymax = -float('inf')
            xmin = float('inf')
            ymin = float('inf')
            xshiftpoint = QPointF(xshift, 0)
            for vertex in component.sV:
                xmax = max(xmax, vertex.view.xy[0])
                ymax = max(ymax, vertex.view.xy[1])
                xmin = min(xmin, vertex.view.xy[0])
                ymin = min(ymin, vertex.view.xy[1])
            if len(lower_fixed) > 0 or lower_middle_point is None:
                for vertex in component.sV:
                    vertices_to_nodes[vertex].setPos(QPointF(*vertex.view.xy) + xshiftpoint - QPointF((xmax + xmin)/2, 0) + (upper_middle_point - QPointF(0, ymax)))
            else:
                for vertex in component.sV:
                    vertices_to_nodes[vertex].setPos(QPointF(*vertex.view.xy) + xshiftpoint - QPointF((xmax + xmin)/2, 0) + (lower_middle_point - QPointF(0, ymin)))
            xshift += (xmax - xmin) + 2 * nodewidgh


    # def layout_nodes(self, nodes: Optional[Iterable[Node]] = None):
    #     if nodes is None:
    #         nodes = self.nodes()
    #
    #     nodes_set = set(nodes)
    #     graph = networkx.Graph()  # wierdly digraph here works way worse for layout
    #     graph.add_nodes_from(nodes)
    #     fixed = []
    #     for node in nodes:
    #         for output_name in node.output_names():
    #             for conn in node.output_connections(output_name):
    #                 nextnode, _ = conn.input()
    #                 if nextnode not in nodes_set:
    #                     nodes_set.add(nextnode)
    #                     fixed.append(nextnode)
    #                 graph.add_edge(node, nextnode)
    #         for input_name in node.input_names():
    #             for conn in node.input_connections(input_name):
    #                 prevnode, _ = conn.output()
    #                 if prevnode not in nodes_set:
    #                     nodes_set.add(prevnode)
    #                     fixed.append(prevnode)
    #                 # double edges will be filtered by networkx, and we wont miss any connection to external nodes this way
    #                 graph.add_edge(prevnode, node)
    #     print(len(nodes_set), len(graph), len(fixed))
    #     init_pos = {node: (node.pos()).toTuple() for node in nodes_set}
    #     print(graph)
    #     print(graph.edges)
    #     if not fixed:
    #         fixed.append(next(iter(nodes_set)))
    #     final_pos = networkx.drawing.layout.spring_layout(graph, 150, pos=init_pos, fixed=fixed or None, iterations=5)
    #     from pprint import pprint
    #     pprint(final_pos)
    #     for node, pos in final_pos.items():
    #         node.setPos(QPointF(*pos))

