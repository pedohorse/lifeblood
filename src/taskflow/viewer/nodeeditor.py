import sqlite3
from dataclasses import dataclass
from types import MappingProxyType
from enum import Enum
from .graphics_items import Task, Node, NodeConnection, NetworkItem, NetworkItemWithUI
from ..uidata import UiData, NodeUi, Parameter
from ..enums import TaskState, NodeParameterType
from ..config import get_config
from .. import logging
from .. import paths
from ..net_classes import NodeTypeMetadata
from ..taskspawn import NewTask
from ..invocationjob import InvocationJob

from ..misc import generate_name

import PySide2.QtCore
import PySide2.QtGui
from PySide2.QtWidgets import *
from PySide2.QtCore import Qt, Slot, Signal, QThread, QRectF, QPointF, QEvent, QSize
from PySide2.QtGui import QKeyEvent, QSurfaceFormat, QPainter, QTransform, QKeySequence

from .dialogs import MessageWithSelectableText
from .create_task_dialog import CreateTaskDialog
from .connection_worker import SchedulerConnectionWorker

import imgui
from imgui.integrations.opengl import ProgrammablePipelineRenderer

import grandalf.graphs
import grandalf.layouts

from typing import Optional, List, Tuple, Dict, Set, Callable, Generator, Iterable, Union, Any

logger = logging.get_logger('viewer')


def call_later(callable, *args, **kwargs):
    if len(args) == 0 and len(kwargs) == 0:
        PySide2.QtCore.QTimer.singleShot(0, callable)
    else:
        PySide2.QtCore.QTimer.singleShot(0, lambda: callable(*args, **kwargs))


class QOpenGLWidgetWithSomeShit(QOpenGLWidget):
    def __init__(self, *args, **kwargs):
        super(QOpenGLWidgetWithSomeShit, self).__init__(*args, **kwargs)
        fmt = QSurfaceFormat()
        fmt.setSamples(4)
        self.setFormat(fmt)

    def initializeGL(self) -> None:
        super(QOpenGLWidgetWithSomeShit, self).initializeGL()
        logger.debug('init')


class Clipboard:
    class ClipboardContentsType(Enum):
        NOTHING = 0
        NODES = 1

    def __init__(self):
        self.__contents: Dict[Clipboard.ClipboardContentsType, Tuple[int, Any]] = {}
        self.__copy_operation_id = 0

    def set_contents(self, ctype: ClipboardContentsType, contents: Any):
        self.__contents[ctype] = (self.__copy_operation_id, contents)

    def contents(self, ctype: ClipboardContentsType) -> Tuple[Optional[int], Optional[Any]]:
        return self.__contents.get(ctype, (None, None))


class NodeSnippetData:
    """
    class containing enough information to reproduce a certain snippet of nodes, with parameter values and connections ofc
    """
    @dataclass
    class ParamData:
        name: str
        type: NodeParameterType
        uvalue: Any
        expr: Optional[str]

    @dataclass
    class NodeData:
        tmpid: int
        type: str
        name: str
        parameters: Dict[str, "NodeSnippetData.ParamData"]
        pos: Tuple[float, float]

    @dataclass
    class ConnData:
        tmpout: int
        out_name: str
        tmpin: int
        in_name: str

    @classmethod
    def from_nodes(cls, nodes: Iterable[Node]):
        raise NotImplementedError()

    def __init__(self, nodes_data: Iterable[NodeData], connections_data: Iterable[ConnData], pos: Tuple[float, float]):
        self.nodes_data = list(nodes_data)
        self.connections_data = list(connections_data)
        self.pos = pos


class LongOperation:
    _nextid = 0

    def __init__(self, progress_callback: Callable[["LongOperation"], Generator]):
        """

        :param progress_callback: this is supposed to be a generator able to take this operation object and opdata as yield arguments.
        if it returns True - we consider long operation done
        """
        self.__id = self._nextid
        LongOperation._nextid += 1
        self.__progress_callback_factory = progress_callback
        self.__progress_callback: Optional[Generator] = None

    def _start(self):
        """
        just to make sure it starts when we need.
        if it starts in constructor - we might potentially have race condition when longop is not yet registered where it's being managed
        and receive _progress too soon
        """
        self.__progress_callback: Generator = self.__progress_callback_factory(self)
        next(self.__progress_callback)

    def _progress(self, opdata):
        try:
            self.__progress_callback.send(opdata)
        except StopIteration:
            return True
        return False

    def opid(self):
        return self.__id


class LongOperationData:
    def __init__(self, op: LongOperation, data: Any):
        self.op = op
        self.data = data


class QGraphicsImguiScene(QGraphicsScene):
    # these are private signals to invoke shit on worker in another thread. QMetaObject's invokemethod is broken in pyside2
    _signal_log_has_been_requested = Signal(int, int, int)
    _signal_log_meta_has_been_requested = Signal(int)
    _signal_node_ui_has_been_requested = Signal(int)
    _signal_task_ui_attributes_has_been_requested = Signal(int)
    _signal_task_invocation_job_requested = Signal(int)
    _signal_node_has_parameter_requested = Signal(int, str, object)
    _signal_node_parameter_change_requested = Signal(int, object, object)
    _signal_node_parameter_expression_change_requested = Signal(int, object, object)
    _signal_nodetypes_update_requested = Signal()
    _signal_set_node_name_requested = Signal(int, str)
    _signal_create_node_requested = Signal(str, str, QPointF, object)
    _signal_remove_node_requested = Signal(int)
    _signal_wipe_node_requested = Signal(int)
    _signal_change_node_connection_requested = Signal(int, object, object, object, object)
    _signal_remove_node_connection_requested = Signal(int)
    _signal_add_node_connection_requested = Signal(int, str, int, str)
    _signal_set_task_group_filter = Signal(set)
    _signal_set_task_state = Signal(list, TaskState)
    _signal_set_tasks_paused = Signal(object, bool)  # object is Union[List[int], str]
    _signal_set_task_node_requested = Signal(int, int)
    _signal_cancel_task_requested = Signal(int)
    _signal_add_task_requested = Signal(NewTask)
    _signal_duplicate_nodes_requested = Signal(dict, QPointF)

    nodetypes_updated = Signal(dict)  # TODO: separate worker-oriented "private" signals for readability
    task_groups_updated = Signal(set)
    task_invocation_job_fetched = Signal(int, InvocationJob)

    long_operation_progressed = Signal(object)  # do we need this signal?

    def __init__(self, db_path: str = None, worker: Optional["SchedulerConnectionWorker"] = None, parent=None):
        super(QGraphicsImguiScene, self).__init__(parent=parent)
        # to debug fuching bsp # self.setItemIndexMethod(QGraphicsScene.NoIndex)
        self.__task_dict: Dict[int, Task] = {}
        self.__node_dict: Dict[int, Node] = {}
        self.__db_path = db_path
        self.__cached_nodetypes: Dict[str, NodeTypeMetadata] = {}
        self.__all_task_groups = set()
        self.__task_group_filter = None

        if worker is None:
            self.__ui_connection_thread = QThread(self)  # SchedulerConnectionThread(self)
            self.__ui_connection_worker = SchedulerConnectionWorker()
            self.__ui_connection_worker.moveToThread(self.__ui_connection_thread)

            self.__ui_connection_thread.started.connect(self.__ui_connection_worker.start)
            self.__ui_connection_thread.finished.connect(self.__ui_connection_worker.finish)
        else:
            self.__ui_connection_thread = None
            self.__ui_connection_worker = worker

        self.__long_operations: Dict[int, LongOperation] = {}

        self.__ui_connection_worker.full_update.connect(self.full_update)
        self.__ui_connection_worker.log_fetched.connect(self.log_fetched)
        self.__ui_connection_worker.nodeui_fetched.connect(self.nodeui_fetched)
        self.__ui_connection_worker.task_attribs_fetched.connect(self._task_attribs_fetched)
        self.__ui_connection_worker.task_invocation_job_fetched.connect(self._task_invocation_job_fetched)
        self.__ui_connection_worker.nodetypes_fetched.connect(self._nodetypes_fetched)
        self.__ui_connection_worker.node_has_parameter.connect(self._node_has_parameter)
        self.__ui_connection_worker.node_parameter_changed.connect(self._node_parameter_changed)
        self.__ui_connection_worker.node_parameter_expression_changed.connect(self._node_parameter_expression_changed)
        self.__ui_connection_worker.node_created.connect(self._node_created)
        self.__ui_connection_worker.nodes_copied.connect(self._nodes_duplicated)

        self._signal_log_has_been_requested.connect(self.__ui_connection_worker.get_log)
        self._signal_log_meta_has_been_requested.connect(self.__ui_connection_worker.get_log_metadata)
        self._signal_node_ui_has_been_requested.connect(self.__ui_connection_worker.get_nodeui)
        self._signal_task_ui_attributes_has_been_requested.connect(self.__ui_connection_worker.get_task_attribs)
        self._signal_node_has_parameter_requested.connect(self.__ui_connection_worker.send_node_has_parameter)
        self._signal_node_parameter_change_requested.connect(self.__ui_connection_worker.send_node_parameter_change)
        self._signal_node_parameter_expression_change_requested.connect(self.__ui_connection_worker.send_node_parameter_expression_change)
        self._signal_nodetypes_update_requested.connect(self.__ui_connection_worker.get_nodetypes)
        self._signal_set_node_name_requested.connect(self.__ui_connection_worker.set_node_name)
        self._signal_create_node_requested.connect(self.__ui_connection_worker.create_node)
        self._signal_remove_node_requested.connect(self.__ui_connection_worker.remove_node)
        self._signal_wipe_node_requested.connect(self.__ui_connection_worker.wipe_node)
        self._signal_duplicate_nodes_requested.connect(self.__ui_connection_worker.duplicate_nodes)
        self._signal_change_node_connection_requested.connect(self.__ui_connection_worker.change_node_connection)
        self._signal_remove_node_connection_requested.connect(self.__ui_connection_worker.remove_node_connection)
        self._signal_add_node_connection_requested.connect(self.__ui_connection_worker.add_node_connection)
        self._signal_set_task_state.connect(self.__ui_connection_worker.set_task_state)
        self._signal_set_tasks_paused.connect(self.__ui_connection_worker.set_tasks_paused)
        self._signal_set_task_group_filter.connect(self.__ui_connection_worker.set_task_group_filter)
        self._signal_set_task_node_requested.connect(self.__ui_connection_worker.set_task_node)
        self._signal_cancel_task_requested.connect(self.__ui_connection_worker.cancel_task)
        self._signal_add_task_requested.connect(self.__ui_connection_worker.add_task)
        self._signal_task_invocation_job_requested.connect(self.__ui_connection_worker.get_task_invocation_job)
        # self.__ui_connection_thread.full_update.connect(self.full_update)

    def request_log(self, task_id: int, node_id: int, invocation_id: int):
        self._signal_log_has_been_requested.emit(task_id, node_id, invocation_id)

    def request_log_meta(self, task_id: int):
        self._signal_log_meta_has_been_requested.emit(task_id)

    def request_attributes(self, task_id: int):
        self._signal_task_ui_attributes_has_been_requested.emit(task_id)

    def request_invocation_job(self, task_id: int):
        self._signal_task_invocation_job_requested.emit(task_id)

    def request_node_ui(self, node_id: int):
        self._signal_node_ui_has_been_requested.emit(node_id)

    def query_node_has_parameter(self, node_id: int, param_name: str, operation_data: Optional["LongOperationData"] = None):
        self._signal_node_has_parameter_requested.emit(node_id, param_name, operation_data)

    def send_node_parameter_change(self, node_id: int, param: Parameter, operation_data: Optional["LongOperationData"] = None):
        self._signal_node_parameter_change_requested.emit(node_id, param, operation_data)

    def send_node_parameter_expression_change(self, node_id: int, param: Parameter, operation_data: Optional["LongOperationData"] = None):
        self._signal_node_parameter_expression_change_requested.emit(node_id, param, operation_data)

    def request_node_types_update(self):
        self._signal_nodetypes_update_requested.emit()

    def request_set_node_name(self, node_id: int, name: str):
        self._signal_set_node_name_requested.emit(node_id, name)

    def request_node_connection_change(self,  connection_id: int, outnode_id: Optional[int] = None, outname: Optional[str] = None, innode_id: Optional[int] = None, inname: Optional[str] = None):
        self._signal_change_node_connection_requested.emit(connection_id, outnode_id, outname, innode_id, inname)

    def request_node_connection_remove(self, connection_id: int):
        self._signal_remove_node_connection_requested.emit(connection_id)

    def request_node_connection_add(self, outnode_id:int , outname: str, innode_id: int, inname: str):
        self._signal_add_node_connection_requested.emit(outnode_id, outname, innode_id, inname)

    def request_create_node(self, typename: str, nodename: str, pos: QPointF, operation_data: Optional["LongOperationData"] = None):
        self._signal_create_node_requested.emit(typename, nodename, pos, operation_data)

    def request_remove_node(self, node_id: int):
        self._signal_remove_node_requested.emit(node_id)

    def request_wipe_node(self, node_id: int):
        self._signal_wipe_node_requested.emit(node_id)

    def request_duplicate_nodes(self, node_ids: List[int], shift: QPointF):
        self._signal_duplicate_nodes_requested.emit(node_ids, shift)

    def set_task_group_filter(self, groups):
        self._signal_set_task_group_filter.emit(groups)

    def set_task_state(self, task_ids: List[int], state: TaskState):
        self._signal_set_task_state.emit(task_ids, state)

    def set_tasks_paused(self, task_ids_or_group: Union[List[int], str], paused: bool):
        self._signal_set_tasks_paused.emit(task_ids_or_group, paused)

    def request_task_cancel(self, task_id: int):
        self._signal_cancel_task_requested.emit(task_id)

    def request_set_task_node(self, task_id: int, node_id:int):
        self._signal_set_task_node_requested.emit(task_id, node_id)

    def request_add_task(self, new_task: NewTask):
        self._signal_add_task_requested.emit(new_task)

    def node_position(self, node_id: int):
        if self.__db_path is not None:
            with sqlite3.connect(self.__db_path) as con:
                con.row_factory = sqlite3.Row
                cur = con.execute('SELECT * FROM "nodes" WHERE "id" = ?', (node_id,))
                row = cur.fetchone()
                if row is not None:
                    return row['posx'], row['posy']

        raise ValueError(f'node id {node_id} has no stored position')

    def set_node_position(self, node_id: int, pos: Union[Tuple[float, float], QPointF]):
        if isinstance(pos, QPointF):
            pos = pos.toTuple()
        if self.__db_path is not None:
            with sqlite3.connect(self.__db_path) as con:
                con.row_factory = sqlite3.Row
                cur = con.execute('INSERT INTO "nodes" ("id", "posx", "posy") VALUES (?, ?, ?) ON CONFLICT("id") DO UPDATE SET posx = ?, posy = ?', (node_id, *pos, *pos))
                row = cur.fetchone()
                if row is not None:
                    return row['posx'], row['posy']

    def node_types(self) -> MappingProxyType:
        return MappingProxyType(self.__cached_nodetypes)

    @Slot(object)
    def full_update(self, uidata: UiData):
        # logger.debug('full_update')

        to_del = []
        to_del_tasks = {}
        existing_node_ids: Dict[int, Node] = {}
        existing_conn_ids: Dict[int, NodeConnection] = {}
        existing_task_ids: Dict[int, Task] = {}
        for item in self.items():
            if isinstance(item, Node):  # TODO: unify this repeating code and move the setting attribs to after all elements are created
                if item.get_id() not in uidata.nodes() or item.node_type() != uidata.nodes()[item.get_id()]['type']:
                    to_del.append(item)
                    continue
                existing_node_ids[item.get_id()] = item
                # TODO: update all kind of attribs here, for now we just don't have any
            elif isinstance(item, NodeConnection):
                if item.get_id() not in uidata.connections():
                    to_del.append(item)
                    continue
                existing_conn_ids[item.get_id()] = item
                # TODO: update all kind of attribs here, for now we just don't have any
            elif isinstance(item, Task):
                if item.get_id() not in uidata.tasks():
                    to_del.append(item)
                    if item.node() is not None:
                        if not item.node() in to_del_tasks:
                            to_del_tasks[item.node()] = []
                        to_del_tasks[item.node()].append(item)
                    continue
                existing_task_ids[item.get_id()] = item

        # before we delete everything - we'll remove tasks from nodes to avoid deleting tasks one by one triggering tonns of animation
        for node, tasks in to_del_tasks.items():
            node.remove_tasks(tasks)
        for item in to_del:
            self.removeItem(item)
        # removing items might cascade things, like removing node will remove connections to that node
        # so now we need to recheck existing items validity
        # though not consistent scene states should not come in uidata at all
        for existings in (existing_node_ids, existing_task_ids, existing_conn_ids):
            for item_id, item in tuple(existings.items()):
                if item.scene() != self:
                    del existings[item_id]

        nodes_to_layout = []
        for id, newdata in uidata.nodes().items():
            if id in existing_node_ids:
                existing_node_ids[id].set_name(newdata['name'])
                continue
            new_node = Node(id, newdata['type'], newdata['name'] or f'node #{id}')
            try:
                new_node.setPos(*self.node_position(id))
            except ValueError:
                nodes_to_layout.append(new_node)
            existing_node_ids[id] = new_node
            self.addItem(new_node)

        for id, newdata in uidata.connections().items():
            if id in existing_conn_ids:
                # ensure connections
                innode, inname = existing_conn_ids[id].input()
                outnode, outname = existing_conn_ids[id].output()
                if innode.get_id() != newdata['node_id_in'] or inname != newdata['in_name']:
                    existing_conn_ids[id].set_input(existing_node_ids[newdata['node_id_in']], newdata['in_name'])
                    existing_conn_ids[id].update()
                if outnode.get_id() != newdata['node_id_out'] or outname != newdata['out_name']:
                    existing_conn_ids[id].set_output(existing_node_ids[newdata['node_id_out']], newdata['out_name'])
                    existing_conn_ids[id].update()
                continue
            new_conn = NodeConnection(id, existing_node_ids[newdata['node_id_out']],
                                      existing_node_ids[newdata['node_id_in']],
                                      newdata['out_name'], newdata['in_name'])
            existing_conn_ids[id] = new_conn
            self.addItem(new_conn)

        for id, newdata in uidata.tasks().items():
            if id not in existing_task_ids:
                new_task = Task(id, newdata['name'] or '<noname>', newdata['groups'])
                existing_task_ids[id] = new_task
                if newdata['origin_task_id'] is not None and newdata['origin_task_id'] in existing_task_ids:  # TODO: bug: this and below will only work if parent/original tasks were created during previous updates
                    origin_task = existing_task_ids[newdata['origin_task_id']]
                    new_task.setPos(origin_task.scenePos())
                elif newdata['parent_id'] is not None and newdata['parent_id'] in existing_task_ids:
                    origin_task = existing_task_ids[newdata['parent_id']]
                    new_task.setPos(origin_task.scenePos())
                self.addItem(new_task)
            task = existing_task_ids[id]
            #print(f'setting {task.get_id()} to {newdata["node_id"]}')
            existing_node_ids[newdata['node_id']].add_task(task)
            task.set_state(TaskState(newdata['state']), bool(newdata['paused']))
            task.set_state_details(newdata['state_details'])  # TODO: maybe instead of 3 calls do it with one, so task parses it's own raw data?
            task.set_raw_data(newdata)
            if newdata['progress'] is not None:
                task.set_progress(newdata['progress'])
            task.set_groups(newdata['groups'])
            # new_task_groups.update(task.groups())

        # now layout nodes that need it
        if nodes_to_layout:
            self.layout_nodes(nodes_to_layout)

        if self.__all_task_groups != uidata.task_groups():
            self.__all_task_groups = uidata.task_groups()
            self.task_groups_updated.emit(uidata.task_groups())

    @Slot(object, object)
    def log_fetched(self, task_id: int, log: dict):
        task = self.get_task(task_id)
        if task is None:
            logger.warning('log fetched, but task not found!')
            return
        task.update_log(log)

    @Slot(object, object)
    def nodeui_fetched(self, node_id: int, nodeui: NodeUi):
        node = self.get_node(node_id)
        if node is None:
            logger.warning('node ui fetched for non existant node')
            return
        node.update_nodeui(nodeui)

    @Slot(object, object)
    def _task_attribs_fetched(self, task_id: int, attribs: dict):
        task = self.get_task(task_id)
        if task is None:
            logger.warning('attribs fetched, but task not found!')
            return
        task.update_attributes(attribs)

    @Slot(object, object)
    def _task_invocation_job_fetched(self, task_id: int, invjob: InvocationJob):
        self.task_invocation_job_fetched.emit(task_id, invjob)

    @Slot(int, str, bool, object)
    def _node_has_parameter(self, node_id, param_name, exists, data):
        if data is not None:
            data.data = (node_id, param_name, exists)
            self.process_operation(data)
            self.long_operation_progressed.emit(data)

    @Slot(int, object, object, object)
    def _node_parameter_changed(self, node_id, param, newval, data):
        if data is not None:
            data.data = (node_id, param.name(), newval)
            self.process_operation(data)
            self.long_operation_progressed.emit(data)

    @Slot(int, object, object)
    def _node_parameter_expression_changed(self, node_id, param, data):
        if data is not None:
            data.data = (node_id, param.name())
            self.process_operation(data)
            self.long_operation_progressed.emit(data)

    @Slot(int, str, str, object, object)
    def _node_created(self, node_id, node_type, node_name, pos, data: "LongOperationData"):
        node = Node(node_id, node_type, node_name)
        node.setPos(pos)
        self.addItem(node)
        if data is not None:
            data.data = (node_id, node_type, node_name)
            self.process_operation(data)
            self.long_operation_progressed.emit(data)

    @Slot(object, object)
    def _nodes_duplicated(self, old_to_new: Dict[int, int], shift: QPointF):
        for old_id, new_id in old_to_new.items():
            old_pos = QPointF()
            old_node = self.get_node(old_id)
            if old_node is not None:
                old_pos = old_node.pos()
            self.set_node_position(new_id, old_pos + shift)


    @Slot(object)
    def _nodetypes_fetched(self, nodetypes):
        self.__cached_nodetypes = nodetypes
        self.nodetypes_updated.emit(nodetypes)

    def addItem(self, item):
        logger.debug('adding item %s', item)
        super(QGraphicsImguiScene, self).addItem(item)
        if isinstance(item, Task):
            self.__task_dict[item.get_id()] = item
        elif isinstance(item, Node):
            self.__node_dict[item.get_id()] = item
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
        logger.debug('item removed')

    def clear(self):
        super(QGraphicsImguiScene, self).clear()
        self.__task_dict = {}
        self.__node_dict = {}

    @Slot(NodeSnippetData, QPointF)
    def paste_copied_nodes(self, snippet: NodeSnippetData, pos: QPointF):
        def pasteop(longop):

            tmp_to_new: Dict[int, int] = {}
            for nodedata in snippet.nodes_data:
                self.request_create_node(nodedata.type, f'{nodedata.name} copy', QPointF(*nodedata.pos) + pos - QPointF(*snippet.pos), LongOperationData(longop, None))
                # NOTE: there is currently no mechanism to ensure order of results when more than one things are requested
                #  from the same operation. So we request and wait things one by one
                node_id, _, _ = yield
                tmp_to_new[nodedata.tmpid] = node_id

                # now setting parameters.
                cyclestart = None
                done_smth_this_cycle = False
                parm_pairs: List[Tuple[str, NodeSnippetData.ParamData]] = list(nodedata.parameters.items())
                for param_name, param_data in parm_pairs:
                    self.query_node_has_parameter(node_id, param_name, LongOperationData(longop, None))
                    _, _, has_param = yield
                    if not has_param:
                        if cyclestart is None or cyclestart == param_name and done_smth_this_cycle:
                            parm_pairs.append((param_name, param_data))
                            cyclestart = param_name
                            done_smth_this_cycle = False
                            continue
                        else:
                            logger.warning(f'could not set parameter {param_name} value')  # and all potential other params in the cycle
                            break
                    done_smth_this_cycle = True
                    proxy_parm = Parameter(param_name, None, param_data.type, param_data.uvalue)
                    if param_data.expr is not None:
                        proxy_parm.set_expression(param_data.expr)
                    self.send_node_parameter_change(node_id, proxy_parm, LongOperationData(longop, None))
                    yield
                    if proxy_parm.has_expression():
                        self.send_node_parameter_expression_change(node_id, proxy_parm, LongOperationData(longop, None))
                        yield

            for conndata in snippet.connections_data:
                self.request_node_connection_add(tmp_to_new[conndata.tmpout], conndata.out_name,
                                                 tmp_to_new[conndata.tmpin], conndata.in_name)

        self.add_long_operation(pasteop)

    @Slot(LongOperationData)
    def process_operation(self, op: LongOperationData):
        assert op.op.opid() in self.__long_operations
        done = op.op._progress(op.data)
        if done:
            del self.__long_operations[op.op.opid()]

    def add_long_operation(self, generator_to_call):
        newop = LongOperation(generator_to_call)
        self.__long_operations[newop.opid()] = newop
        newop._start()

    def get_task(self, task_id) -> Optional[Task]:
        return self.__task_dict.get(task_id, None)

    def get_node(self, node_id) -> Optional[Node]:
        return self.__node_dict.get(node_id, None)

    def nodes(self) -> Tuple[Node]:
        return tuple(self.__node_dict.values())

    def tasks(self) -> Tuple[Task]:
        return tuple(self.__task_dict.values())

    def start(self):
        if self.__ui_connection_thread is None:
            return
        self.__ui_connection_thread.start()

    def stop(self):
        if self.__ui_connection_thread is None:
            for meth in dir(self):  # disconnect all signals from worker slots
                if not meth.startswith('_signal_'):
                    continue
                getattr(self, meth).disconnect()

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
        with sqlite3.connect(self.__db_path) as con:
            con.row_factory = sqlite3.Row
            for item in self.items():
                if not isinstance(item, Node):
                    continue
                con.execute('INSERT OR REPLACE INTO "nodes" ("id", "posx", "posy") '
                            'VALUES (?, ?, ?)', (item.get_id(), *item.pos().toTuple()))
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
        print('press mg=', self.mouseGrabberItem())
        if not event.isAccepted() and len(event.wire_candidates) > 0:
            print([x[0] for x in event.wire_candidates])
            closest = min(event.wire_candidates, key=lambda x: x[0])
            closest[1].post_mousePressEvent(event)  # this seem a bit unsafe, at least not typed statically enough

    def mouseReleaseEvent(self, event: QGraphicsSceneMouseEvent) -> None:
        super(QGraphicsImguiScene, self).mouseReleaseEvent(event)
        print('release mg=', self.mouseGrabberItem())

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


class NodeEditor(QGraphicsView):
    def __init__(self, db_path: str = None, worker=None, parent=None):
        super(NodeEditor, self).__init__(parent=parent)

        self.__oglwidget = QOpenGLWidgetWithSomeShit()
        self.setViewport(self.__oglwidget)
        self.setRenderHints(QPainter.Antialiasing | QPainter.SmoothPixmapTransform)
        self.setMouseTracking(True)
        self.setDragMode(self.RubberBandDrag)

        self.setViewportUpdateMode(QGraphicsView.FullViewportUpdate)
        self.setCacheMode(QGraphicsView.CacheBackground)
        self.__view_scale = 0.0

        self.__ui_panning_lastpos = None

        self.__ui_focused_item = None

        self.__scene = QGraphicsImguiScene(db_path, worker)
        self.setScene(self.__scene)
        #self.__update_timer = PySide2.QtCore.QTimer(self)
        #self.__update_timer.timeout.connect(lambda: self.__scene.invalidate(layers=QGraphicsScene.ForegroundLayer))
        #self.__update_timer.setInterval(50)
        #self.__update_timer.start()
        self.__editor_clipboard = Clipboard()

        self.__shortcut_layout = QShortcut(QKeySequence('ctrl+l'), self)
        self.__shortcut_layout.activated.connect(self.layout_selected_nodes)

        self.__create_menu_popup_toopen = False
        self.__node_type_input = ''
        self.__menu_popup_selection_id = 0
        self.__menu_popup_selection_name = ''
        self.__menu_popup_arrow_down = False
        self.__node_types: Dict[str, NodeTypeMetadata] = {}

        self.__scene.nodetypes_updated.connect(self._nodetypes_updated)
        self.__scene.task_invocation_job_fetched.connect(self._popup_show_invocation_info)

        self.__scene.request_node_types_update()

        self.__imgui_input_blocked = False

        self.__imgui_init = False
        self.__imgui_config_path = get_config('viewer').get_option_noasync('imgui.ini_file', str(paths.config_path('imgui.ini', 'viewer'))).encode('UTF-8')
        self.update()

    #
    # Actions
    #
    @Slot()
    def layout_selected_nodes(self):
        nodes = [n for n in self.__scene.selectedItems() if isinstance(n, Node)]
        if not nodes:
            return
        self.__scene.layout_nodes(nodes, center=self.sceneRect().center())

    @Slot()
    def copy_selected_nodes(self):
        """
        we save a structure that remembers all selected nodes' names, types and all parameters' values
        and all connections
        later on "paste" event these will be used to create all new nodes

        :return:
        """
        clipnodes = []
        clipconns = []
        old_to_tmp: Dict[int, int] = {}
        all_clip_nodes = set()
        avgpos = QPointF()
        tmpid = 0
        for node in self.__scene.selectedItems():
            if not isinstance(node, Node):
                continue
            all_clip_nodes.add(node)
            params: Dict[str, "NodeSnippetData.ParamData"] = {}
            old_to_tmp[node.get_id()] = tmpid
            nodedata = NodeSnippetData.NodeData(tmpid,
                                                node.node_type(),
                                                node.node_name(),
                                                params,
                                                node.pos().toTuple())
            # nodedata = {'tmpid': tmpid,
            #             'type': node.node_type(),
            #             'name': node.node_name(),
            #             'parameters': params,
            #             'pos': node.pos()}
            tmpid += 1
            avgpos += node.pos()

            nodeui = node.get_nodeui()
            if nodeui is not None:
                for param in nodeui.parameters():
                    param_data = NodeSnippetData.ParamData(param.name(),
                                                           param.type(),
                                                           param.unexpanded_value(),
                                                           param.expression())
                    params[param.name()] = param_data
                    # param_data['uvalue'] = param.unexpanded_value()
                    # param_data['type'] = param.type()
                    # param_data['expr'] = param.expression()

            clipnodes.append(nodedata)

        if len(all_clip_nodes) == 0:
            return
        avgpos /= len(all_clip_nodes)

        # now connections
        for node in all_clip_nodes:
            for out_name in node.output_names():
                for conn in node.output_connections(out_name):
                    other_node, other_name = conn.input()
                    if other_node not in all_clip_nodes:
                        continue
                    clipconns.append(NodeSnippetData.ConnData(old_to_tmp[conn.output()[0].get_id()], conn.output()[1],
                                                              old_to_tmp[conn.input()[0].get_id()], conn.input()[1]))

        self.__editor_clipboard.set_contents(Clipboard.ClipboardContentsType.NODES, NodeSnippetData(clipnodes, clipconns, avgpos.toTuple()))

    @Slot(QPointF)
    def paste_copied_nodes(self, pos: QPointF):
        clipdata = self.__editor_clipboard.contents(self.__editor_clipboard.ClipboardContentsType.NODES)
        if clipdata is None:
            return
        self.__scene.paste_copied_nodes(clipdata[1], pos)

    @Slot(QPointF)
    def duplicate_selected_nodes(self, pos: QPointF):
        contents = self.__scene.selectedItems()
        if not contents:
            return
        node_ids = []
        avg_old_pos = QPointF()
        for item in contents:
            if not isinstance(item, Node):
                continue
            node_ids.append(item.get_id())
            avg_old_pos += item.pos()
        if len(node_ids) == 0:
            return
        avg_old_pos /= len(node_ids)
        print(node_ids, pos, avg_old_pos)
        self.__scene.request_duplicate_nodes(node_ids, pos - avg_old_pos)

    def show_task_menu(self, task):
        menu = QMenu(self)
        menu.addAction(f'task {task.get_id()}').setEnabled(False)
        menu.addSeparator()
        menu.addAction(f'{task.state().name}').setEnabled(False)
        if task.state_details() is None:
            menu.addAction('no state message').setEnabled(False)
        else:
            menu.addAction('state message').triggered.connect(lambda _=False, x=task: self.show_task_details(x))
        menu.addAction('-paused-' if task.paused() else 'active').setEnabled(False)

        menu.addAction('show invocation info').triggered.connect(lambda ckeched=False, x=task.get_id(): self.__scene.request_invocation_job(x))

        menu.addSeparator()

        if task.paused():
            menu.addAction('resume').triggered.connect(lambda checked=False, x=task.get_id(): self.__scene.set_tasks_paused([x], False))
        else:
            menu.addAction('pause').triggered.connect(lambda checked=False, x=task.get_id(): self.__scene.set_tasks_paused([x], True))

        if task.state() == TaskState.IN_PROGRESS:
            menu.addAction('cancel').triggered.connect(lambda checked=False, x=task.get_id(): self.__scene.request_task_cancel(x))
        state_submenu = menu.addMenu('force state')
        for state in TaskState:
            if state in (TaskState.GENERATING, TaskState.INVOKING, TaskState.IN_PROGRESS, TaskState.POST_GENERATING):
                continue
            state_submenu.addAction(state.name).triggered.connect(lambda checked=False, x=task.get_id(), state=state: self.__scene.set_task_state([x], state))

        pos = self.mapToGlobal(self.mapFromScene(task.scenePos()))
        menu.aboutToHide.connect(menu.deleteLater)
        menu.popup(pos)

    def show_task_details(self, task: Task):
        details = task.state_details()
        if details is None:
            return
        dialog = MessageWithSelectableText(details.get('message', ''), parent=self)
        dialog.show()

    def show_node_menu(self, node: Node, pos=None):
        menu = QMenu(self)
        menu.addAction(f'node {node.node_name()}').setEnabled(False)
        menu.addSeparator()
        menu.addAction('rename').triggered.connect(lambda checked=False, x=node: self._popup_node_rename_widget(x))
        menu.addSeparator()
        menu.addAction('pause all tasks').triggered.connect(node.pause_all_tasks)
        menu.addAction('resume all tasks').triggered.connect(node.resume_all_tasks)
        menu.addSeparator()

        if len(self.__scene.selectedItems()) > 0:
            menu.addAction(f'layout selected nodes ({self.__shortcut_layout.key().toString()})').triggered.connect(self.layout_selected_nodes)
            menu.addSeparator()

        menu.addAction('create new task').triggered.connect(lambda checked=False, x=node: self._popup_create_task(x))

        menu.addSeparator()
        del_submenu = menu.addMenu('extra')

        def _action(checked=False, nid=node.get_id()):
            self.__scene.request_wipe_node(nid)
            node = self.__scene.get_node(nid)
            if node is not None:
                node.setSelected(False)

        del_submenu.addAction('reset node to default state').triggered.connect(_action)

        if pos is None:
            pos = self.mapToGlobal(self.mapFromScene(node.mapToScene(node.boundingRect().topRight())))
        menu.aboutToHide.connect(menu.deleteLater)
        menu.popup(pos)

    def show_general_menu(self, pos):
        menu = QMenu(self)
        menu.addAction(f'layout selected nodes ({self.__shortcut_layout.key().toString()})').triggered.connect(self.layout_selected_nodes)
        menu.addAction('duplicate selected nodes here').triggered.connect(lambda c=False, p=self.mapToScene(self.mapFromGlobal(pos)): self.duplicate_selected_nodes(p))
        menu.addSeparator()
        menu.addAction('copy selected').triggered.connect(self.copy_selected_nodes)
        menu.addAction('paste').triggered.connect(lambda c=False, p=self.mapToScene(self.mapFromGlobal(pos)): self.paste_copied_nodes(p))
        menu.aboutToHide.connect(menu.deleteLater)
        menu.popup(pos)

    def _popup_node_rename_widget(self, node: Node):
        assert node.scene() == self.__scene
        lpos = self.mapFromScene(node.mapToScene(node.boundingRect().topLeft()))
        wgt = QLineEdit(self)
        wgt.setMinimumWidth(256)  # TODO: user-befriend this shit
        wgt.move(lpos)
        self.__imgui_input_blocked = True
        wgt.editingFinished.connect(lambda i=node.get_id(), w=wgt: self.__scene.request_set_node_name(i, w.text()))
        wgt.editingFinished.connect(wgt.deleteLater)
        wgt.editingFinished.connect(lambda: PySide2.QtCore.QTimer.singleShot(0, self.__unblock_imgui_input))  # polish trick to make this be called after current events are processed, events where keypress might be that we need to skip

        wgt.textChanged.connect(lambda x: print('sh', self.sizeHint()))
        wgt.setText(node.node_name())
        wgt.show()
        wgt.setFocus()

    def _popup_create_task_callback(self, node_id: int, wgt: CreateTaskDialog):
        print(wgt.get_task_name())
        print(wgt.get_task_groups())
        print(wgt.get_task_attributes())
        new_task = NewTask(wgt.get_task_name(), node_id, task_attributes=wgt.get_task_attributes())
        new_task.add_extra_group_names(wgt.get_task_groups())
        self.__scene.request_add_task(new_task)

    def _popup_create_task(self, node: Node):
        wgt = CreateTaskDialog(self)
        wgt.accepted.connect(lambda i=node.get_id(), w=wgt: self._popup_create_task_callback(i, w))
        wgt.finished.connect(wgt.deleteLater)
        wgt.show()

    @Slot(object, object)
    def _popup_show_invocation_info(self, task_id: int, invjob: InvocationJob):
        popup = QDialog(parent=self)
        layout = QVBoxLayout(popup)
        edit = QTextEdit()
        edit.setReadOnly(True)
        layout.addWidget(edit)
        #popup = QMessageBox(QMessageBox.Information, f'invocation job information for task #{task_id}', 'see details', parent=self)
        popup.finished.connect(popup.deleteLater)
        popup.setModal(False)
        popup.setSizeGripEnabled(True)
        popup.setWindowTitle(f'invocation job information for task #{task_id}')

        env = 'Extra environment:\n' + '\n'.join(f'\t{k}={v}' for k, v in invjob.env().resolve().items()) if invjob.env() is not None else 'none'
        argv = f'Command line:\n\t{repr(invjob.args())}'
        extra_files = 'Extra Files:\n' + '\n'.join(f'\t{name}: {len(data):,d}B' for name, data in invjob.extra_files().items())

        #popup.setDetailedText('\n\n'.join((argv, env, extra_files)))
        edit.setPlainText('\n\n'.join((argv, env, extra_files)))

        popup.show()

    @Slot()
    def __unblock_imgui_input(self):
        self.__imgui_input_blocked = False

    @Slot()
    def _nodetypes_updated(self, nodetypes):
        self.__node_types = nodetypes

    def _set_clipboard(self, text: str):
        QApplication.clipboard().setText(text)

    def _get_clipboard(self) -> str:
        return QApplication.clipboard().text()

    def drawForeground(self, painter: PySide2.QtGui.QPainter, rect: QRectF) -> None:
        painter.beginNativePainting()
        if not self.__imgui_init:
            logger.debug('initializing imgui')
            self.__imgui_init = True
            imgui.create_context()
            self.__imimpl = ProgrammablePipelineRenderer()
            imguio = imgui.get_io()
            # note that as of imgui 1.3.0 ini_file_name seem to have a bug of not increasing refcount,
            # so there HAS to be some other python variable, like self.__imgui_config_path, to ensure
            # that path is not garbage collected
            imguio.ini_file_name = self.__imgui_config_path
            imguio.display_size = 400, 400
            imguio.set_clipboard_text_fn = self._set_clipboard
            imguio.get_clipboard_text_fn = self._get_clipboard
            self._map_keys()

        imgui.get_io().display_size = self.rect().size().toTuple()  # rect.size().toTuple()
        # start new frame context
        imgui.new_frame()

        if imgui.begin_main_menu_bar():
            if imgui.begin_menu("File", True):

                clicked_quit, selected_quit = imgui.menu_item(
                    "Quit", 'Cmd+Q', False, True
                )

                if clicked_quit:
                    self.close()

                imgui.end_menu()
            imgui.end_main_menu_bar()

        imgui.core.show_metrics_window()

        # open new window context
        imgui.set_next_window_size(561, 697, imgui.FIRST_USE_EVER)
        imgui.set_next_window_position(1065, 32, imgui.FIRST_USE_EVER)
        imgui.begin("Parameters")

        # draw text label inside of current window
        sel = self.__scene.selectedItems()
        if len(sel) > 0 and isinstance(sel[0], NetworkItemWithUI):
            sel[0].draw_imgui_elements(self)

        # close current window context
        imgui.end()

        # tab menu
        if self.__create_menu_popup_toopen:
            imgui.open_popup('create node')
            self.__node_type_input = ''
            self.__menu_popup_selection_id = 0
            self.__menu_popup_selection_name = ''
            self.__menu_popup_arrow_down = False

        if imgui.begin_popup('create node'):
            changed, self.__node_type_input = imgui.input_text('', self.__node_type_input, 256)
            if not imgui.is_item_active() and not imgui.is_mouse_down():
                # if text input is always focused - selectable items do not work
                imgui.set_keyboard_focus_here(-1)
            if changed:
                self.__menu_popup_selection_id = 0
            item_number = 0
            for type_name, type_meta in self.__node_types.items():
                inparts = [x.strip() for x in self.__node_type_input.split(' ')]
                if all(x in type_name
                       or any(t.startswith(x) for t in type_meta.tags)
                       or x in type_meta.label for x in inparts):  # TODO: this can be cached
                    selected = self.__menu_popup_selection_id == item_number
                    _, selected = imgui.selectable(f'{type_meta.label}##popup_selectable',  selected=selected, flags=imgui.SELECTABLE_DONT_CLOSE_POPUPS)
                    if selected:
                        self.__menu_popup_selection_id = item_number
                        self.__menu_popup_selection_name = type_name
                    item_number += 1

            imguio: imgui.core._IO = imgui.get_io()
            if imguio.keys_down[imgui.KEY_DOWN_ARROW]:  # TODO: pauses until key up
                if not self.__menu_popup_arrow_down:
                    self.__menu_popup_selection_id += 1
                    self.__menu_popup_arrow_down = True
            elif imguio.keys_down[imgui.KEY_UP_ARROW]:
                if not self.__menu_popup_arrow_down:
                    self.__menu_popup_selection_id -= 1
                    self.__menu_popup_arrow_down = True
            if imguio.keys_down[imgui.KEY_ENTER] or imgui.is_mouse_double_clicked():
                imgui.close_current_popup()
                # for type_name, type_meta in self.__node_types.items():
                #     if self.__node_type_input in type_name \
                #             or self.__node_type_input in type_meta.tags \
                #             or self.__node_type_input in type_meta.label:
                #         self.__node_type_input = type_name
                #         break
                # else:
                #     self.__node_type_input = ''
                if self.__menu_popup_selection_name:
                    self.__scene.request_create_node(self.__menu_popup_selection_name, f'{self.__menu_popup_selection_name} {generate_name(5, 7)}', self.mapToScene(imguio.mouse_pos.x, imguio.mouse_pos.y))
            elif self.__menu_popup_arrow_down:
                self.__menu_popup_arrow_down = False


            elif imguio.keys_down[imgui.KEY_ESCAPE]:
                imgui.close_current_popup()
                self.__node_type_input = ''
                self.__menu_popup_selection_id = 0
            imgui.end_popup()

        self.__create_menu_popup_toopen = False
        # pass all drawing comands to the rendering pipeline
        # and close frame context
        imgui.render()
        # imgui.end_frame()
        self.__imimpl.render(imgui.get_draw_data())
        painter.endNativePainting()

    def imguiProcessEvents(self, event: PySide2.QtGui.QInputEvent, do_recache=True):
        if self.__imgui_input_blocked:
            return
        if not self.__imgui_init:
            return
        io = imgui.get_io()
        if isinstance(event, PySide2.QtGui.QMouseEvent):
            io.mouse_pos = event.pos().toTuple()
        elif isinstance(event, PySide2.QtGui.QWheelEvent):
            io.mouse_wheel = event.angleDelta().y() / 100
        elif isinstance(event, PySide2.QtGui.QKeyEvent):
            #print('pressed', event.key(), event.nativeScanCode(), event.nativeVirtualKey(), event.text(), imgui.KEY_A)
            if event.key() in imgui_key_map:
                if event.type() == QEvent.KeyPress:
                    io.keys_down[imgui_key_map[event.key()]] = True  # TODO: figure this out
                    #io.keys_down[event.key()] = True
                elif event.type() == QEvent.KeyRelease:
                    io.keys_down[imgui_key_map[event.key()]] = False
            elif event.key() == Qt.Key_Control:
                io.key_ctrl = event.type() == QEvent.KeyPress

            if event.type() == QEvent.KeyPress and len(event.text()) > 0:
                io.add_input_character(ord(event.text()))

        if isinstance(event, (PySide2.QtGui.QMouseEvent, PySide2.QtGui.QWheelEvent)):
            io.mouse_down[0] = event.buttons() & Qt.LeftButton
            io.mouse_down[1] = event.buttons() & Qt.MiddleButton
            io.mouse_down[2] = event.buttons() & Qt.RightButton
        if do_recache:
            self.resetCachedContent()

    def focusInEvent(self, event):
        # just in case we will drop all imgui extra keys
        event.accept()
        if self.__imgui_input_blocked:
            return
        if not self.__imgui_init:
            return
        io = imgui.get_io()
        for key in imgui_key_map.values():
            io.keys_down[key] = False
        io.key_ctrl = False

    # def _map_keys(self):
    #     key_map = imgui.get_io().key_map
    #
    #     key_map[imgui.KEY_TAB] = Qt.Key_Tab
    #     key_map[imgui.KEY_LEFT_ARROW] = Qt.Key_Left
    #     key_map[imgui.KEY_RIGHT_ARROW] = Qt.Key_Right
    #     key_map[imgui.KEY_UP_ARROW] = Qt.Key_Up
    #     key_map[imgui.KEY_DOWN_ARROW] = Qt.Key_Down
    #     key_map[imgui.KEY_PAGE_UP] = Qt.Key_PageUp
    #     key_map[imgui.KEY_PAGE_DOWN] = Qt.Key_PageDown
    #     key_map[imgui.KEY_HOME] = Qt.Key_Home
    #     key_map[imgui.KEY_END] = Qt.Key_End
    #     key_map[imgui.KEY_DELETE] = Qt.Key_Delete
    #     key_map[imgui.KEY_BACKSPACE] = Qt.Key_Backspace
    #     key_map[imgui.KEY_ENTER] = Qt.Key_Enter
    #     key_map[imgui.KEY_ESCAPE] = Qt.Key_Escape
    #     key_map[imgui.KEY_A] = Qt.Key_A
    #     key_map[imgui.KEY_C] = Qt.Key_C
    #     key_map[imgui.KEY_V] = Qt.Key_V
    #     key_map[imgui.KEY_X] = Qt.Key_X
    #     key_map[imgui.KEY_Y] = Qt.Key_Y
    #     key_map[imgui.KEY_Z] = Qt.Key_Z

    def _map_keys(self):
        key_map = imgui.get_io().key_map

        key_map[imgui.KEY_TAB] = imgui.KEY_TAB
        key_map[imgui.KEY_LEFT_ARROW] = imgui.KEY_LEFT_ARROW
        key_map[imgui.KEY_RIGHT_ARROW] = imgui.KEY_RIGHT_ARROW
        key_map[imgui.KEY_UP_ARROW] = imgui.KEY_UP_ARROW
        key_map[imgui.KEY_DOWN_ARROW] = imgui.KEY_DOWN_ARROW
        key_map[imgui.KEY_PAGE_UP] = imgui.KEY_PAGE_UP
        key_map[imgui.KEY_PAGE_DOWN] = imgui.KEY_PAGE_DOWN
        key_map[imgui.KEY_HOME] = imgui.KEY_HOME
        key_map[imgui.KEY_END] = imgui.KEY_END
        key_map[imgui.KEY_DELETE] = imgui.KEY_DELETE
        key_map[imgui.KEY_BACKSPACE] = imgui.KEY_BACKSPACE
        key_map[imgui.KEY_ENTER] = imgui.KEY_ENTER
        key_map[imgui.KEY_ESCAPE] = imgui.KEY_ESCAPE
        key_map[imgui.KEY_A] = imgui.KEY_A
        key_map[imgui.KEY_C] = imgui.KEY_C
        key_map[imgui.KEY_V] = imgui.KEY_V
        key_map[imgui.KEY_X] = imgui.KEY_X
        key_map[imgui.KEY_Y] = imgui.KEY_Y
        key_map[imgui.KEY_Z] = imgui.KEY_Z

    def request_ui_focus(self, item: NetworkItem):
        if self.__ui_focused_item is not None and self.__ui_focused_item.scene() != self.__scene:
            self.__ui_focused_item = None

        if self.__ui_focused_item is not None:
            return False
        self.__ui_focused_item = item
        return True

    def release_ui_focus(self, item: NetworkItem):
        assert item == self.__ui_focused_item, "ui focus was released by not the item that got focus"
        self.__ui_focused_item = None
        return True

    def mouseDoubleClickEvent(self, event: PySide2.QtGui.QMouseEvent):
        self.imguiProcessEvents(event)
        if imgui.get_io().want_capture_mouse:
            event.accept()
        else:
            super(NodeEditor, self).mouseDoubleClickEvent(event)

    def mouseMoveEvent(self, event: PySide2.QtGui.QMouseEvent):
        self.imguiProcessEvents(event)
        if imgui.get_io().want_capture_mouse:
            event.accept()
        else:
            if self.__ui_panning_lastpos is not None:
                rect = self.sceneRect()
                rect.setSize(QSize(1, 1))
                self.setSceneRect(rect.translated(*((self.__ui_panning_lastpos - event.screenPos()) * (2 ** self.__view_scale)).toTuple()))
                #self.translate(*(event.screenPos() - self.__ui_panning_lastpos).toTuple())
                self.__ui_panning_lastpos = event.screenPos()
            else:
                super(NodeEditor, self).mouseMoveEvent(event)

    def mousePressEvent(self, event: PySide2.QtGui.QMouseEvent):
        self.imguiProcessEvents(event)
        if imgui.get_io().want_capture_mouse:
            event.accept()
        else:
            if event.buttons() & Qt.MiddleButton:
                self.__ui_panning_lastpos = event.screenPos()
            elif event.buttons() & Qt.RightButton and self.itemAt(event.pos()) is None:
                event.accept()
                self.show_general_menu(event.globalPos())
            else:
                super(NodeEditor, self).mousePressEvent(event)

    def mouseReleaseEvent(self, event: PySide2.QtGui.QMouseEvent):
        self.imguiProcessEvents(event)
        if imgui.get_io().want_capture_mouse:
            event.accept()
        else:
            super(NodeEditor, self).mouseReleaseEvent(event)
            if not (event.buttons() & Qt.MiddleButton):
                self.__ui_panning_lastpos = None
        PySide2.QtCore.QTimer.singleShot(50, self.resetCachedContent)

    def wheelEvent(self, event: PySide2.QtGui.QWheelEvent):
        self.imguiProcessEvents(event)
        if imgui.get_io().want_capture_mouse:
            event.accept()
        else:
            event.accept()
            self.__view_scale = max(0, self.__view_scale - event.angleDelta().y()*0.001)

            iz = 2**(-self.__view_scale)
            self.setTransform(QTransform.fromScale(iz, iz))
            super(NodeEditor, self).wheelEvent(event)

    def keyPressEvent(self, event: PySide2.QtGui.QKeyEvent):
        self.imguiProcessEvents(event)
        if imgui.get_io().want_capture_keyboard:
            event.accept()
        else:
            if event.key() == Qt.Key_Tab:
                # in case enter or escape is pressed at this time - force unpress it
                self.imguiProcessEvents(PySide2.QtGui.QKeyEvent(QEvent.KeyRelease, Qt.Key_Return, Qt.NoModifier))
                self.imguiProcessEvents(PySide2.QtGui.QKeyEvent(QEvent.KeyRelease, Qt.Key_Escape, Qt.NoModifier))

                self.__create_menu_popup_toopen = True
                self.__scene.request_node_types_update()
                PySide2.QtCore.QTimer.singleShot(0, self.resetCachedContent)
            super(NodeEditor, self).keyPressEvent(event)

    def keyReleaseEvent(self, event: PySide2.QtGui.QKeyEvent):
        self.imguiProcessEvents(event)
        if imgui.get_io().want_capture_keyboard:
            event.accept()
        else:
            super(NodeEditor, self).keyReleaseEvent(event)

    def closeEvent(self, event: PySide2.QtGui.QCloseEvent) -> None:
        self.stop()
        super(NodeEditor, self).closeEvent(event)

    def start(self):
        self.__scene.start()

    def stop(self):
        self.__scene.stop()
        self.__scene.save_node_layout()


imgui_key_map = {
    Qt.Key_Tab: imgui.KEY_TAB,
    Qt.Key_Left: imgui.KEY_LEFT_ARROW,
    Qt.Key_Right: imgui.KEY_RIGHT_ARROW,
    Qt.Key_Up: imgui.KEY_UP_ARROW,
    Qt.Key_Down: imgui.KEY_DOWN_ARROW,
    Qt.Key_PageUp: imgui.KEY_PAGE_UP,
    Qt.Key_PageDown: imgui.KEY_PAGE_DOWN,
    Qt.Key_Home: imgui.KEY_HOME,
    Qt.Key_End: imgui.KEY_END,
    Qt.Key_Delete: imgui.KEY_DELETE,
    Qt.Key_Backspace: imgui.KEY_BACKSPACE,
    Qt.Key_Return: imgui.KEY_ENTER,
    Qt.Key_Escape: imgui.KEY_ESCAPE,
    Qt.Key_A: imgui.KEY_A,
    Qt.Key_C: imgui.KEY_C,
    Qt.Key_V: imgui.KEY_V,
    Qt.Key_X: imgui.KEY_X,
    Qt.Key_Y: imgui.KEY_Y,
    Qt.Key_Z: imgui.KEY_Z,
}
