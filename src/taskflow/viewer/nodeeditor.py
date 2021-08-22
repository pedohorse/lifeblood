import socket
import struct
import time
import json
import pickle
import sqlite3
import asyncio
from types import MappingProxyType
from .graphics_items import Task, Node, NodeConnection, NetworkItem, NetworkItemWithUI
from ..uidata import UiData, NodeUi, Parameter
from ..enums import TaskState
from ..broadcasting import await_broadcast
from ..nethelpers import recv_exactly, get_default_addr, address_to_ip_port
from ..config import get_config
from .. import logging
from .. import paths
from ..net_classes import NodeTypeMetadata
from ..taskspawn import NewTask
from ..invocationjob import InvocationJob

from ..enums import NodeParameterType

from ..misc import generate_name

import PySide2.QtCore
import PySide2.QtGui
from PySide2.QtWidgets import *
from PySide2.QtCore import Qt, Slot, Signal, QThread, QRectF, QPointF, QEvent
from PySide2.QtGui import QKeyEvent, QSurfaceFormat, QPainter, QTransform

from .dialogs import MessageWithSelectableText
from .create_task_dialog import CreateTaskDialog

import imgui
from imgui.integrations.opengl import ProgrammablePipelineRenderer

from typing import Optional, List, Tuple, Dict, Set, Callable, Iterable, Union

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


class QGraphicsImguiScene(QGraphicsScene):
    # these are private signals to invoke shit on worker in another thread. QMetaObject's invokemethod is broken in pyside2
    _signal_log_has_been_requested = Signal(int, int, int)
    _signal_log_meta_has_been_requested = Signal(int)
    _signal_node_ui_has_been_requested = Signal(int)
    _signal_task_ui_attributes_has_been_requested = Signal(int)
    _signal_task_invocation_job_requested = Signal(int)
    _signal_node_parameter_change_requested = Signal(int, object)
    _signal_nodetypes_update_requested = Signal()
    _signal_set_node_name_requested = Signal(int, str)
    _signal_create_node_requested = Signal(str, str, QPointF)
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

    nodetypes_updated = Signal(dict)  # TODO: separate worker-oriented "private" signals for readability
    task_groups_updated = Signal(set)
    task_invocation_job_fetched = Signal(int, InvocationJob)

    def __init__(self, db_path: str = None, parent=None):
        super(QGraphicsImguiScene, self).__init__(parent=parent)
        # to debug fuching bsp # self.setItemIndexMethod(QGraphicsScene.NoIndex)
        self.__task_dict: Dict[int, Task] = {}
        self.__node_dict: Dict[int, Node] = {}
        self.__db_path = db_path
        self.__cached_nodetypes: Dict[str, NodeTypeMetadata] = {}
        self.__all_task_groups = set()
        self.__task_group_filter = None

        self.__ui_connection_thread = QThread(self)  # SchedulerConnectionThread(self)
        self.__ui_connection_worker = SchedulerConnectionWorker()
        self.__ui_connection_worker.moveToThread(self.__ui_connection_thread)

        self.__ui_connection_thread.started.connect(self.__ui_connection_worker.start)
        self.__ui_connection_thread.finished.connect(self.__ui_connection_worker.finish)

        self.__ui_connection_worker.full_update.connect(self.full_update)
        self.__ui_connection_worker.log_fetched.connect(self.log_fetched)
        self.__ui_connection_worker.nodeui_fetched.connect(self.nodeui_fetched)
        self.__ui_connection_worker.task_attribs_fetched.connect(self._task_attribs_fetched)
        self.__ui_connection_worker.task_invocation_job_fetched.connect(self._task_invocation_job_fetched)
        self.__ui_connection_worker.nodetypes_fetched.connect(self._nodetypes_fetched)
        self.__ui_connection_worker.node_created.connect(self._node_created)

        self._signal_log_has_been_requested.connect(self.__ui_connection_worker.get_log)
        self._signal_log_meta_has_been_requested.connect(self.__ui_connection_worker.get_log_metadata)
        self._signal_node_ui_has_been_requested.connect(self.__ui_connection_worker.get_nodeui)
        self._signal_task_ui_attributes_has_been_requested.connect(self.__ui_connection_worker.get_task_attribs)
        self._signal_node_parameter_change_requested.connect(self.__ui_connection_worker.send_node_parameter_change)
        self._signal_nodetypes_update_requested.connect(self.__ui_connection_worker.get_nodetypes)
        self._signal_set_node_name_requested.connect(self.__ui_connection_worker.set_node_name)
        self._signal_create_node_requested.connect(self.__ui_connection_worker.create_node)
        self._signal_remove_node_requested.connect(self.__ui_connection_worker.remove_node)
        self._signal_wipe_node_requested.connect(self.__ui_connection_worker.wipe_node)
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

    def request_node_ui(self, node_id: int):
        self._signal_node_ui_has_been_requested.emit(node_id)

    def send_node_parameter_change(self, node_id: int, param: Parameter):
        self._signal_node_parameter_change_requested.emit(node_id, param)

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

    def request_create_node(self, typename: str, nodename: str, pos: QPointF):
        self._signal_create_node_requested.emit(typename, nodename, pos)

    def request_remove_node(self, node_id: int):
        self._signal_remove_node_requested.emit(node_id)

    def request_wipe_node(self, node_id: int):
        self._signal_wipe_node_requested.emit(node_id)

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

        return node_id * 125.79 % 400, node_id * 357.17 % 400  # TODO: do something better!

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

        for id, newdata in uidata.nodes().items():
            if id in existing_node_ids:
                existing_node_ids[id].set_name(newdata['name'])
                continue
            new_node = Node(id, newdata['type'], newdata['name'] or f'node #{id}')
            new_node.setPos(*self.node_position(id))
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

    @Slot(int)
    def _node_created(self, node_id, node_type, node_name, pos):
        node = Node(node_id, node_type, node_name)
        node.setPos(pos)
        self.addItem(node)

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

    def get_task(self, task_id) -> Optional[Task]:
        return self.__task_dict.get(task_id, None)

    def get_node(self, node_id) -> Optional[Node]:
        return self.__node_dict.get(node_id, None)

    def nodes(self) -> Tuple[Node]:
        return tuple(self.__node_dict.values())

    def tasks(self) -> Tuple[Task]:
        return tuple(self.__task_dict.values())

    def start(self):
        self.__ui_connection_thread.start()

    def stop(self):
        # self.__ui_connection_thread.requestInterruption()
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


class SchedulerConnectionWorker(PySide2.QtCore.QObject):
    full_update = Signal(UiData)
    log_fetched = Signal(int, dict)
    nodeui_fetched = Signal(int, NodeUi)
    task_attribs_fetched = Signal(int, dict)
    task_invocation_job_fetched = Signal(int, InvocationJob)
    nodetypes_fetched = Signal(dict)
    node_created = Signal(int, str, str, QPointF)

    def __init__(self, parent=None):
        super(SchedulerConnectionWorker, self).__init__(parent)
        self.__started = False
        self.__timer = None
        self.__to_stop = False
        self.__task_group_filter: Optional[Set[str]] = None
        self.__conn: Optional[socket.socket] = None

    def request_interruption(self):
        self.__to_stop = True  # assume it's atomic, which it should be

    def interruption_requested(self):
        return self.__to_stop

    @Slot()
    def start(self):
        """
        supposed to be called from the thread timer lives in
        starts checking on full state
        :return:
        """
        assert self.thread() == QThread.currentThread()
        self.__started = True
        self.__timer = PySide2.QtCore.QTimer(self)
        self.__timer.setInterval(1000)
        self.__timer.timeout.connect(self.check_scheduler)
        self.__timer.start()

    @Slot()
    def finish(self):
        """
        note that interruption mush have been requested before
        after this out thread will probably never enter the event loop again
        :return:
        """
        self.__timer.stop()

    @Slot(set)
    def set_task_group_filter(self, groups: Set[str]):
        self.__task_group_filter = groups
        # TODO: force update

    def ensure_connected(self) -> bool:
        if self.__conn is not None:
            return True

        async def _interrupt_waiter():
            while True:
                if self.interruption_requested():
                    return None
                await asyncio.sleep(0.5)

        config = get_config('viewer')
        if config.get_option_noasync('viewer.listen_to_broadcast', True):
            logger.info('waiting for scheduler broadcast...')
            tasks = asyncio.run(asyncio.wait((
                await_broadcast('taskflow_scheduler'),
                _interrupt_waiter()), return_when=asyncio.FIRST_COMPLETED))

            logger.debug(tasks)
            message = list(tasks[0])[0].result()

            logger.debug(message)
            if message is None:
                return False
            logger.debug('received broadcast: %s', message)
            schedata = json.loads(message)

            sche_addr, sche_port = address_to_ip_port(schedata['ui'])  #schedata['ui'].split(':')
            #sche_port = int(sche_port)
        else:
            sche_addr = config.get_option_noasync('viewer.scheduler_ip', get_default_addr())
            sche_port = config.get_option_noasync('viewer.scheduler_port', 7989)  # TODO: promote all defaults like this somewhere
        logger.debug(f'connecting to scheduler on {sche_addr}:{sche_port} ...')

        while not self.interruption_requested():
            try:
                self.__conn = socket.create_connection((sche_addr, sche_port), timeout=30)
            except ConnectionError:
                logger.debug('ui connection refused, retrying...')

                # now sleep, but listening to interrupt requests
                for i in range(25):
                    time.sleep(0.2)
                    if self.interruption_requested():
                        return False
            else:
                break

        assert self.__conn is not None
        self.__conn.sendall(b'\0\0\0\0')
        return True

    def _send_string(self, text: str):
        bts = text.encode('UTF-8')
        self.__conn.sendall(struct.pack('>Q', len(bts)))
        self.__conn.sendall(bts)

    def _recv_string(self):
        btlen = struct.unpack('>Q', recv_exactly(self.__conn, 8))[0]
        return recv_exactly(self.__conn, btlen).decode('UTF-8')

    @Slot()
    def check_scheduler(self):
        if self.interruption_requested():
            self.__timer.stop()
            if self.__conn is not None:
                self.__conn.close()
            self.__conn = None
            return

        if not self.ensure_connected():
            return

        assert self.__conn is not None

        try:
            self.__conn.sendall(b'getfullstate\n')
            if not self.__task_group_filter:
                self.__conn.sendall(struct.pack('>I', 0))
            else:
                self.__conn.sendall(struct.pack('>I', len(self.__task_group_filter)))
                for group in self.__task_group_filter:
                    self._send_string(group)
            recvdata = recv_exactly(self.__conn, 8)
        except ConnectionError as e:
            logger.error(f'connection reset {e}')
            logger.error('scheduler connection lost')
            self.__conn = None
            return
        except:
            logger.exception('problems in network operations')
            self.__conn = None
            return
        if len(recvdata) != 8:  # means connection was closed
            logger.error('scheduler connection lost')
            self.__conn = None
            return
        uidatasize = struct.unpack('>Q', recvdata)[0]
        logger.debug(f'fullstate: {uidatasize}B')
        uidatabytes = recv_exactly(self.__conn, uidatasize)
        if len(uidatabytes) != uidatasize:
            logger.error('scheduler connection lost')
            return
        uidata = UiData.deserialize_noasync(uidatabytes)
        self.full_update.emit(uidata)

    @Slot(int)
    def get_log_metadata(self, task_id: int):
        if not self.ensure_connected():
            return

        assert self.__conn is not None
        try:
            self.__conn.sendall(b'getlogmeta\n')
            self.__conn.sendall(struct.pack('>Q', task_id))
            rcvsize = struct.unpack('>I', recv_exactly(self.__conn, 4))[0]
            logmeta = pickle.loads(recv_exactly(self.__conn, rcvsize))
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except:
            logger.exception('problems in network operations')
        else:
            self.log_fetched.emit(task_id, logmeta)

    @Slot(int)
    def get_task_attribs(self, task_id: int):
        if not self.ensure_connected():
            return
        assert self.__conn is not None

        try:
            self.__conn.sendall(b'gettaskattribs\n')
            self.__conn.sendall(struct.pack('>Q', task_id))
            rcvsize = struct.unpack('>Q', recv_exactly(self.__conn, 8))[0]
            attribs = pickle.loads(recv_exactly(self.__conn, rcvsize))
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except:
            logger.exception('problems in network operations')
        else:
            self.task_attribs_fetched.emit(task_id, attribs)

    @Slot(int)
    def get_task_invocation_job(self, task_id: int):
        if not self.ensure_connected():
            return
        assert self.__conn is not None

        try:
            self.__conn.sendall(b'gettaskinvoc\n')
            self.__conn.sendall(struct.pack('>Q', task_id))
            rcvsize = struct.unpack('>Q', recv_exactly(self.__conn, 8))[0]
            invoc = InvocationJob.deserialize(recv_exactly(self.__conn, rcvsize))
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except:
            logger.exception('problems in network operations')
        else:
            self.task_invocation_job_fetched.emit(task_id, invoc)

    @Slot(int, int, int)
    def get_log(self, task_id: int, node_id: int, invocation_id: int):
        if not self.ensure_connected():
            return

        assert self.__conn is not None
        try:
            self.__conn.sendall(b'getlog\n')
            self.__conn.sendall(struct.pack('>QQQ', task_id, node_id, invocation_id))
            rcvsize = struct.unpack('>I', recv_exactly(self.__conn, 4))[0]
            alllogs = pickle.loads(recv_exactly(self.__conn, rcvsize))
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except:
            logger.exception('problems in network operations')
        else:
            self.log_fetched.emit(task_id, alllogs)

    @Slot()
    def get_nodeui(self, node_id: int):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'getnodeinterface\n')
            self.__conn.sendall(struct.pack('>Q', node_id))
            rcvsize = struct.unpack('>I', recv_exactly(self.__conn, 4))[0]
            nodeui: NodeUi = pickle.loads(recv_exactly(self.__conn, rcvsize))
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except:
            logger.exception('problems in network operations')
        else:
            self.nodeui_fetched.emit(node_id, nodeui)

    @Slot()
    def send_node_parameter_change(self, node_id: int, param: Parameter):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            param_type = param.type()
            param_value = param.unexpanded_value()
            self.__conn.sendall(b'setnodeparam\n')
            param_name_data = param.name().encode('UTF-8')
            self.__conn.sendall(struct.pack('>QII', node_id, param_type.value, len(param_name_data)))
            self.__conn.sendall(param_name_data)
            if param_type == NodeParameterType.FLOAT:
                self.__conn.sendall(struct.pack('>d', param_value))
            elif param_type == NodeParameterType.INT:
                self.__conn.sendall(struct.pack('>q', param_value))
            elif param_type == NodeParameterType.BOOL:
                self.__conn.sendall(struct.pack('>?', param_value))
            elif param_type == NodeParameterType.STRING:
                param_str_data = param_value.encode('UTF-8')
                self.__conn.sendall(struct.pack('>Q', len(param_str_data)))
                self.__conn.sendall(param_str_data)
            else:
                raise NotImplementedError()
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except:
            logger.exception('problems in network operations')

    @Slot()
    def get_nodetypes(self):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        nodetypes: Dict[str, NodeTypeMetadata] = {}
        try:
            metas: List[NodeTypeMetadata] = []
            self.__conn.sendall(b'listnodetypes\n')
            elemcount = struct.unpack('>Q', recv_exactly(self.__conn, 8))[0]
            for i in range(elemcount):
                btlen = struct.unpack('>Q', recv_exactly(self.__conn, 8))[0]
                metas.append(pickle.loads(recv_exactly(self.__conn, btlen)))
            nodetypes = {n.type_name: n for n in metas}
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except:
            logger.exception('problems in network operations')
        else:
            self.nodetypes_fetched.emit(nodetypes)

    @Slot()
    def create_node(self, node_type, node_name, pos):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'addnode\n')
            self._send_string(node_type)
            self._send_string(node_name)
            node_id = struct.unpack('>Q', recv_exactly(self.__conn, 8))[0]
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except:
            logger.exception('problems in network operations')
        else:
            self.node_created.emit(node_id, node_type, node_name, pos)

    @Slot()
    def remove_node(self, node_id: int):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'removenode\n')
            self.__conn.sendall(struct.pack('>Q', node_id))
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except:
            logger.exception('problems in network operations')

    @Slot()
    def wipe_node(self, node_id: int):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'wipenode\n')
            self.__conn.sendall(struct.pack('>Q', node_id))
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except:
            logger.exception('problems in network operations')

    @Slot()
    def set_node_name(self, node_id: int, node_name: str):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'renamenode\n')
            self.__conn.sendall(struct.pack('>Q', node_id))
            self._send_string(node_name)
            _ = self._recv_string()
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except:
            logger.exception('problems in network operations')

    @Slot()
    def change_node_connection(self, connection_id: int, outnode_id: Optional[int] = None, outname: Optional[str] = None, innode_id: Optional[int] = None, inname: Optional[str] = None):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            logger.debug(f'{connection_id}, {outnode_id}, {outname}, {innode_id}, {inname}')
            self.__conn.sendall(b'changeconnection\n')
            self.__conn.sendall(struct.pack('>Q??QQ', connection_id, outnode_id is not None, innode_id is not None, outnode_id or 0, innode_id or 0))
            if outnode_id is not None:
                self._send_string(outname)
            if innode_id is not None:
                self._send_string(inname)
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except:
            logger.exception('problems in network operations')

    @Slot()
    def add_node_connection(self, outnode_id: int, outname: str, innode_id: int, inname: str):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'addconnection\n')
            self.__conn.sendall(struct.pack('>QQ', outnode_id, innode_id))
            self._send_string(outname)
            self._send_string(inname)
            new_id = struct.unpack('>Q', recv_exactly(self.__conn, 8))[0]
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except:
            logger.exception('problems in network operations')

    @Slot()
    def remove_node_connection(self, connection_id: int):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'removeconnection\n')
            self.__conn.sendall(struct.pack('>Q', connection_id))
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except:
            logger.exception('problems in network operations')

    # task control things
    @Slot()
    def set_tasks_paused(self, task_ids_or_group: Union[List[int], str], paused: bool):
        if len(task_ids_or_group) == 0:
            return
        if not self.ensure_connected():
            return
        assert self.__conn is not None

        try:
            if isinstance(task_ids_or_group, str):
                self.__conn.sendall(b'tpausegrp\n')
                self.__conn.sendall(struct.pack('>?', paused))
                self._send_string(task_ids_or_group)
            else:
                numtasks = len(task_ids_or_group)
                if numtasks == 0:
                    return
                self.__conn.sendall(b'tpauselst\n')
                self.__conn.sendall(struct.pack('>Q?Q', numtasks, paused, task_ids_or_group[0]))
                if numtasks > 1:
                    self.__conn.sendall(struct.pack('>' + 'Q' * (numtasks-1), *task_ids_or_group[1:]))
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except:
            logger.exception('problems in network operations')

    @Slot()
    def set_task_node(self, task_id: int, node_id: int):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'tsetnode\n')
            self.__conn.sendall(struct.pack('>QQ', task_id, node_id))
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except:
            logger.exception('problems in network operations')

    @Slot()
    def set_task_state(self, task_ids: List[int], state: TaskState):
        numtasks = len(task_ids)
        if numtasks == 0:
            return
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'tcstate\n')
            self.__conn.sendall(struct.pack('>QIQ', numtasks, state.value, task_ids[0]))
            if numtasks > 1:
                self.__conn.sendall(struct.pack('>' + 'Q' * (numtasks - 1), *task_ids[1:]))
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except:
            logger.exception('problems in network operations')

    @Slot()
    def cancel_task(self, task_id: int):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'tcancel\n')
            self.__conn.sendall(struct.pack('>Q', task_id))
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except:
            logger.exception('problems in network operations')

    @Slot()
    def add_task(self, new_task: NewTask):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        data = new_task.serialize()
        try:
            self.__conn.sendall(b'addtask\n')
            self.__conn.sendall(struct.pack('>Q', len(data)))
            self.__conn.sendall(data)
            _ = recv_exactly(self.__conn, 4)  # reply that we don't care about for now
        except ConnectionError as e:
            logger.error(f'failed {e}')
        except:
            logger.exception('problem in network operations')


class NodeEditor(QGraphicsView):
    def __init__(self, db_path: str = None, parent=None):
        super(NodeEditor, self).__init__(parent=parent)

        self.__oglwidget = QOpenGLWidgetWithSomeShit()
        self.setViewport(self.__oglwidget)
        self.setRenderHints(QPainter.Antialiasing | QPainter.SmoothPixmapTransform)
        self.setMouseTracking(True)
        self.setDragMode(self.RubberBandDrag)

        self.setViewportUpdateMode(QGraphicsView.FullViewportUpdate)
        self.setCacheMode(QGraphicsView.CacheBackground)
        self.__view_scale = 1.0

        self.__ui_panning_lastpos = None

        self.__ui_focused_item = None

        self.__scene = QGraphicsImguiScene(db_path)
        self.setScene(self.__scene)
        #self.__update_timer = PySide2.QtCore.QTimer(self)
        #self.__update_timer.timeout.connect(lambda: self.__scene.invalidate(layers=QGraphicsScene.ForegroundLayer))
        #self.__update_timer.setInterval(50)
        #self.__update_timer.start()

        self.__create_menu_popup_toopen = False
        self.__node_type_input = ''
        self.__menu_popup_selection_id = 0
        self.__menu_popup_selection_name = ''
        self.__menu_popup_arrow_down = False
        self.__node_types: Dict[str, NodeTypeMetadata] = {}

        self.__scene.nodetypes_updated.connect(self._nodetypes_updated)

        self.__scene.request_node_types_update()

        self.__imgui_input_blocked = False

        self.__imgui_init = False
        self.__imgui_config_path = get_config('viewer').get_option_noasync('imgui.ini_file', str(paths.config_path('imgui.ini', 'viewer'))).encode('UTF-8')
        self.update()

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
        new_task = NewTask(wgt.get_task_name(), node_id, **wgt.get_task_attributes())
        new_task.add_extra_group_names(wgt.get_task_groups())
        self.__scene.request_add_task(new_task)

    def _popup_create_task(self, node: Node):
        wgt = CreateTaskDialog(self)
        wgt.accepted.connect(lambda i=node.get_id(), w=wgt: self._popup_create_task_callback(i, w))
        wgt.finished.connect(wgt.deleteLater)
        wgt.show()

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
                self.setSceneRect(rect.translated(*((self.__ui_panning_lastpos - event.screenPos()) * self.__view_scale).toTuple()))
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
            self.__view_scale = max(0.1, self.__view_scale - event.angleDelta().y()*0.001)

            iz = 1.0/self.__view_scale
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
