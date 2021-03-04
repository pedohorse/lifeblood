import sys
import os
import socket
import struct
import time
import json
import pickle
import sqlite3
import asyncio
from math import sqrt
from .uidata import UiData, NodeUi
from .scheduler import TaskState, InvocationState
from .broadcasting import await_broadcast
from .nethelpers import recv_exactly

from .enums import NodeParameterType

import PySide2.QtCore
import PySide2.QtGui
from PySide2.QtWidgets import *
from PySide2.QtCore import Qt, Slot, Signal, QThread, QRectF, QSizeF, QPointF, QAbstractAnimation, QSequentialAnimationGroup
from PySide2.QtGui import QPen, QBrush, QColor, QPainterPath


import imgui
from imgui.integrations.opengl import ProgrammablePipelineRenderer

from typing import Optional, List


class NetworkItem(QGraphicsItem):
    def __init__(self, id):
        super(NetworkItem, self).__init__()
        self.__id = id

    def get_id(self):
        return self.__id


class NetworkItemWithUI(NetworkItem):
    def update_ui(self):
        self.update()  # currently contents and UI are drawn always together, so this will do
        # but in future TODO: invalidate only UI layer

    def draw_imgui_elements(self):
        """
        this should only be called from active opengl context!
        :return:
        """
        pass


class TaskAnimation(QAbstractAnimation):
    def __init__(self, task: "Task",  node2: "Node", pos2: "QPointF", duration: int, parent):
        super(TaskAnimation, self).__init__(parent)
        self.__task = task

        self.__node1, self.__pos1 = task.final_location()
        print(self.__node1, self.__pos1)
        self.__node2 = node2
        self.__pos2 = pos2
        self.__duration = max(duration, 1)
        self.__started = False

    def duration(self) -> int:
        return self.__duration

    def updateCurrentTime(self, currentTime: int) -> None:
        if not self.__started:
            self.__started = True

        pos1 = self.__pos1
        if self.__node1:
            pos1 = self.__node1.mapToScene(pos1)

        pos2 = self.__pos2
        if self.__node2:
            pos2 = self.__node2.mapToScene(pos2)

        t = currentTime / self.duration()
        self.__task.setPos(pos1*(1-t) + pos2*t)


class Node(NetworkItemWithUI):
    def __init__(self, id, name):
        super(Node, self).__init__(id)
        self.setFlags(QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemIsSelectable)
        self.__height = 75
        self.__width = 150
        self.__line_width = 1
        self.__name = name
        self.__tasks: List["Task"] = []

        self.__some_value = 50

        # prepare drawing tools
        self.__borderpen= QPen(QColor(96, 96, 96, 255))
        self.__borderpen_selected = QPen(QColor(144, 144, 144, 255))
        self.__caption_pen = QPen(QColor(192, 192, 192, 255))
        self.__borderpen.setWidthF(self.__line_width)
        self.__header_brush = QBrush(QColor(48, 64, 48, 192))
        self.__body_brush = QBrush(QColor(48, 48, 48, 128))

        self.__nodeui: Optional[NodeUi] = None

    def update_nodeui(self, nodeui: NodeUi):
        self.__nodeui = nodeui
        self.update_ui()

    def boundingRect(self) -> QRectF:
        lw = self.__width + self.__line_width
        lh = self.__height + self.__line_width
        return QRectF(-0.5 * lw, -0.5 * lh, lw, lh)

    def paint(self, painter: PySide2.QtGui.QPainter, option: QStyleOptionGraphicsItem, widget: Optional[QWidget] = None) -> None:
        painter.pen().setWidthF(self.__line_width)
        lw = self.__width + self.__line_width
        lh = self.__height + self.__line_width
        nodeshape = QPainterPath()
        bodymask = QPainterPath()
        bodymask.addRect(-0.5 * lw, -0.5 * lh + 16, lw, lh - 16)
        nodeshape.addRoundedRect(QRectF(-0.5 * lw, -0.5 * lh, lw, lh), 5, 5)
        headershape = nodeshape - bodymask
        bodyshape = nodeshape & bodymask

        if self.isSelected():
            painter.setPen(self.__borderpen_selected)
        else:
            painter.setPen(self.__borderpen)
        painter.fillPath(headershape, self.__header_brush)
        painter.fillPath(bodyshape, self.__body_brush)
        painter.drawPath(nodeshape)
        painter.setPen(self.__caption_pen)
        painter.drawText(self.boundingRect(), Qt.AlignHCenter | Qt.AlignTop, self.__name)

    def get_input_position(self, idx=0) -> QPointF:
        return self.mapToScene(0, -0.5 * self.__height)

    def get_output_position(self, idx=0) -> QPointF:
        return self.mapToScene(0, 0.5 * self.__height)

    # def sceneEvent(self, event: PySide2.QtCore.QEvent) -> bool:
    #     print('qqq', event)
    #     #super(Node, self).sceneEvent(event)
    #     return False

    # def mouseMoveEvent(self, event: QGraphicsSceneMouseEvent) -> None:
    #     print('aqwe')
    #     #self.moveBy(*(event.screenPos() - event.lastScreenPos()).toTuple())

    def add_task(self, task: "Task"):
        if task in self.__tasks:
            return
        print(f"adding task {task.get_id()} to node {self.get_id()}")
        pos_id = len(self.__tasks)
        task.set_node_animated(self, self.get_task_pos(task, pos_id))
        if task.node():
            task.node().remove_task(task)
        self.__tasks.append(task)
        task._Task__node = self

    def remove_task(self, task_to_remove: "Task"):
        print(f"removeing task {task_to_remove.get_id()} from node {self.get_id()}")
        task_pid = self.__tasks.index(task_to_remove)
        task_to_remove._Task__node = None
        for i in range(task_pid, len(self.__tasks) - 1):
            self.__tasks[i] = self.__tasks[i + 1]
            self.__tasks[i].set_node_animated(self, self.get_task_pos(self.__tasks[i], i))
        self.__tasks = self.__tasks[:-1]

    def get_task_pos(self, task: "Task", pos_id: int) -> QPointF:
        #assert task in self.__tasks
        x, y = self.boundingRect().topLeft().toTuple()
        w, h = self.boundingRect().size().toTuple()
        d = task.draw_size()  # TODO: this assumes size is same, so dont make it an instance method
        r = d * 0.5
        y += 16  # hardcoded header
        h -= 16
        w *= 0.5
        x += r
        y += r
        h -= d
        w -= d
        x += (d * pos_id % w)
        y += (d * int(d * pos_id / w) % h)
        return QPointF(x, y)

    #
    # interface
    def draw_imgui_elements(self):
        imgui.text(f'Node {self.get_id()}, name {self.__name}')
        _, self.__some_value = imgui.slider_float('foo', self.__some_value, 0, 100, format='%.3f')
        if self.__nodeui is not None:
            for param_name, param_dict in self.__nodeui.parameters_items():
                param_type = param_dict['type']

                if param_type == NodeParameterType.BOOL:
                    changed, param_dict['value'] = imgui.checkbox(param_name, param_dict['value'])
                elif param_type == NodeParameterType.INT:
                    changed, param_dict['value'] = imgui.slider_int(param_name, param_dict['value'], 0, 10)
                elif param_type == NodeParameterType.FLOAT:
                    changed, param_dict['value'] = imgui.slider_float(param_name, param_dict['value'], 0, 10)
                elif param_type == NodeParameterType.STRING:
                    changed, param_dict['value'] = imgui.input_text(param_name, param_dict['value'], 256)
                else:
                    raise NotImplementedError()
                if changed:
                    self.scene().send_node_parameter_change(self.get_id(), param_name, param_dict)

    def itemChange(self, change, value):
        if change == QGraphicsItem.ItemSelectedHasChanged:
            if value:   # item was just selected
                self.scene().request_node_ui(self.get_id())
                
        return super(Node, self).itemChange(change, value)


class NodeConnection(NetworkItem):
    def __init__(self, id: int, nodeout: Node, nodein: Node):
        super(NodeConnection, self).__init__(id)
        self.__nodeout = nodeout
        self.__nodein = nodein
        self.setZValue(-1)
        self.__line_width = 4

        self.__pen = QPen(QColor(64, 64, 64, 192))
        self.__pen.setWidthF(3)

    def boundingRect(self) -> QRectF:
        hlw = self.__line_width * 0.5
        return QRectF(self.__nodeout.get_output_position(), self.__nodein.get_input_position()).adjusted(-hlw, -hlw, hlw, hlw)

    def paint(self, painter: PySide2.QtGui.QPainter, option: QStyleOptionGraphicsItem, widget: Optional[QWidget] = None) -> None:
        line = QPainterPath()
        p0 = self.__nodeout.get_output_position()
        p1 = self.__nodein.get_input_position()
        line.moveTo(p0)
        line.cubicTo(p0 + QPointF(0, 150), p1 - QPointF(0, 150), p1)

        painter.setPen(self.__pen)
        painter.drawPath(line)

    def out_node(self):
        return self.__nodeout

    def in_node(self):
        return self.__nodein


class Task(NetworkItemWithUI):
    def __init__(self, id, name):
        super(Task, self).__init__(id)
        self.setFlags(QGraphicsItem.ItemIsSelectable)
        self.__name = name
        self.__state = TaskState.WAITING
        self.__log: dict = {}
        self.__ui_attributes: dict = {}
        self.__requested_invocs_while_selected = set()

        self.__size = 16
        self.__line_width = 4
        self.__node: Optional[Node] = None

        self.__animation_group: Optional[QSequentialAnimationGroup] = None
        self.__final_pos = None

        self.__borderpen = [QPen(QColor(96, 96, 96, 255)),
                            QPen(QColor(192, 192, 192, 255))]
        self.__brushes = {TaskState.WAITING: QBrush(QColor(64, 64, 64, 192)),
                          TaskState.GENERATING: QBrush(QColor(32, 128, 128, 192)),
                          TaskState.READY:  QBrush(QColor(32, 64, 32, 192)),
                          TaskState.IN_PROGRESS: QBrush(QColor(128, 128, 32, 192)),
                          TaskState.POST_WAITING: QBrush(QColor(96, 96, 96, 192)),
                          TaskState.POST_GENERATING: QBrush(QColor(128, 32, 128, 192)),
                          TaskState.DONE: QBrush(QColor(32, 192, 32, 192)),
                          TaskState.ERROR: QBrush(QColor(192, 32, 32, 192)),
                          TaskState.SPAWNED: QBrush(QColor(32, 32, 32, 192)),
                          TaskState.DEAD: QBrush(QColor(16, 19, 22, 192))}

    def boundingRect(self) -> QRectF:
        lw = self.__line_width
        return QRectF(QPointF(-0.5 * (self.__size + lw), -0.5 * (self.__size + lw)),
                      QSizeF(self.__size + lw, self.__size + lw))

    def paint(self, painter: PySide2.QtGui.QPainter, option: QStyleOptionGraphicsItem, widget: Optional[QWidget] = None) -> None:
        path = QPainterPath()
        path.addEllipse(-0.5 * self.__size, -0.5 * self.__size,
                        self.__size, self.__size)
        brush = self.__brushes[self.__state]
        painter.fillPath(path, brush)
        painter.setPen(self.__borderpen[int(self.isSelected())])
        painter.drawPath(path)

    def name(self):
        return self.__name

    def state(self):
        return self.__state

    def set_node(self, node: Optional[Node], _drop_animation=True):
        """
        TODO: describe difference from set_node_animated below. this one curently is not used at all btw.
        :param node:
        :param _drop_animation:
        :return:
        """
        if _drop_animation and self.__animation_group is not None:
            self.__animation_group.stop()
            assert self.__animation_group is None
        if node is not None:
            node.add_task(self)
        else:
            if self.__node:
                self.__node.remove_task(self)

    def node(self):
        return self.__node

    def draw_size(self):
        return self.__size

    def set_state(self, state: TaskState):
        if state == self.__state:
            return
        self.__state = state
        self.update()

    def update_log(self, alllog):
        #self.__log = alllog
        print('log updated with', alllog)
        # Note that we assume log deletion is not possible
        for node_id, invocs in alllog.items():
            if self.__log.get(node_id, None) is None:
                self.__log[node_id] = invocs
                continue
            for inv_id, logs in invocs.items():
                if logs is None and inv_id in self.__log[node_id]:
                    continue
                self.__log[node_id][inv_id] = logs

        self.update_ui()

    def update_attributes(self, attributes: dict):
        print('attrs updated with', attributes)
        self.__ui_attributes = attributes
        self.update_ui()

    def set_node_animated(self, node: Optional[Node], pos: QPointF):
        print('set animated called!', node, pos)
        dist = ((pos if node is None else node.mapToScene(pos)) - self.final_scene_position())
        ldist = sqrt(QPointF.dotProduct(dist, dist))
        new_animation = TaskAnimation(self, node, pos, duration=int(ldist / 0.5), parent=self.scene())
        if self.__animation_group is None:
            self.__animation_group = QSequentialAnimationGroup(self.scene())
            self.__animation_group.finished.connect(self._clear_animation_group)
            self.setParentItem(None)
        self.__final_pos = pos
        self.__animation_group.addAnimation(new_animation)
        if self.__animation_group.state() != QAbstractAnimation.Running:
            self.__animation_group.start()

    def final_location(self) -> (Node, QPointF):
        if self.__animation_group is not None:
            assert self.__final_pos is not None
            return self.__node, self.__final_pos
        else:
            return self.__node, self.pos()

    def final_scene_position(self) -> QPointF:
        fnode, fpos = self.final_location()
        if fnode is not None:
            fpos = fnode.mapToScene(fpos)
        return fpos

    @Slot()
    def _clear_animation_group(self):
        if self.__animation_group is not None:
            print('anim finished!')
            ag, self.__animation_group = self.__animation_group, None
            ag.stop()  # just in case some recursion occures
            self.setParentItem(self.__node)
            self.setPos(self.__final_pos)
            self.__final_pos = None

    def setParentItem(self, item):
        """
        use set_node if you want to set node
        :param item:
        :return:
        """
        super(Task, self).setParentItem(item)

    def itemChange(self, change, value):
        if change == QGraphicsItem.ItemSelectedHasChanged:
            if value and self.__node is not None:   # item was just selected
                self.scene().request_log_meta(self.get_id())  # update all task metadata: which nodes it ran on and invocation numbers only
                self.scene().request_attributes(self.get_id())

                # if task is in progress - we find that invocation of it that is not finished and null it to force update
                if self.__state == TaskState.IN_PROGRESS \
                        and self.__node.get_id() in self.__log \
                        and self.__log[self.__node.get_id()] is not None:
                    for invoc_id, invoc in self.__log[self.__node.get_id()].items():
                        if (invoc is None or
                                invoc['state'] != InvocationState.FINISHED.value) \
                                and invoc_id in self.__requested_invocs_while_selected:
                            self.__requested_invocs_while_selected.remove(invoc_id)
            elif not value:
                pass
                #self.__log = None
        return super(Task, self).itemChange(change, value)

    #
    # interface
    def draw_imgui_elements(self):
        imgui.text(f'Task {self.get_id()}')
        # first draw attributes
        for key, val in self.__ui_attributes.items():
            imgui.columns(2, 'node_attributes')
            imgui.text(key)
            imgui.next_column()
            imgui.text(repr(val))
            imgui.columns(1)

        # now draw log
        if self.__log is None:
            return
        for node_id, invocs in self.__log.items():
            node_expanded, _ = imgui.collapsing_header(f'node {node_id}')
            if not node_expanded:  # or invocs is None:
                continue
            for invoc_id, invoc in invocs.items():
                # TODO: pyimgui is not covering a bunch of fancy functions... watch when it's done
                invoc_expanded, _ = imgui.collapsing_header(f'invocation {invoc_id}')
                if not invoc_expanded:
                    continue
                if invoc_id not in self.__requested_invocs_while_selected:
                    self.__requested_invocs_while_selected.add(invoc_id)
                    self.scene().request_log(self.get_id(), node_id, invoc_id)
                if invoc is None:
                    imgui.text('...fetching...')
                else:
                    imgui.text_unformatted(invoc.get('stdout', 'error') or '...nothing here...')
                    if invoc['state'] == InvocationState.IN_PROGRESS.value:
                        if imgui.button('update'):
                            print('clicked')
                            if invoc_id in self.__requested_invocs_while_selected:
                                self.__requested_invocs_while_selected.remove(invoc_id)


class QOpenGLWidgetWithSomeShit(QOpenGLWidget):
    def initializeGL(self) -> None:
        super(QOpenGLWidgetWithSomeShit, self).initializeGL()
        print('init')


class QGraphicsImguiScene(QGraphicsScene):
    # these are private signals to invoke shit on worker in another thread. QMetaObject's invokemethod is broken in pyside2
    log_has_been_requested = Signal(int, int, int)
    log_meta_has_been_requested = Signal(int)
    node_ui_has_been_requested = Signal(int)
    task_ui_attributes_has_been_requested = Signal(int)
    node_parameter_change_requested = Signal(int, str, dict)

    def __init__(self, db_path: str = None, parent=None):
        super(QGraphicsImguiScene, self).__init__(parent=parent)
        self.__task_dict = {}
        self.__node_dict = {}
        self.__db_path = db_path

        self.__ui_connection_thread = QThread(self)  # SchedulerConnectionThread(self)
        self.__ui_connection_worker = SchedulerConnectionWorker()
        self.__ui_connection_worker.moveToThread(self.__ui_connection_thread)

        self.__ui_connection_thread.started.connect(self.__ui_connection_worker.start)
        self.__ui_connection_thread.finished.connect(self.__ui_connection_worker.finish)

        self.__ui_connection_worker.full_update.connect(self.full_update)
        self.__ui_connection_worker.log_fetched.connect(self.log_fetched)
        self.__ui_connection_worker.nodeui_fetched.connect(self.nodeui_fetched)
        self.__ui_connection_worker.task_attribs_fetched.connect(self.task_attribs_fetched)

        self.log_has_been_requested.connect(self.__ui_connection_worker.get_log)
        self.log_meta_has_been_requested.connect(self.__ui_connection_worker.get_log_metadata)
        self.node_ui_has_been_requested.connect(self.__ui_connection_worker.get_nodeui)
        self.task_ui_attributes_has_been_requested.connect(self.__ui_connection_worker.get_task_attribs)
        self.node_parameter_change_requested.connect(self.__ui_connection_worker.send_node_parameter_change)
        # self.__ui_connection_thread.full_update.connect(self.full_update)

        self.__ui_connection_thread.start()

    def request_log(self, task_id, node_id, invocation_id):
        self.log_has_been_requested.emit(task_id, node_id, invocation_id)

    def request_log_meta(self, task_id):
        self.log_meta_has_been_requested.emit(task_id)

    def request_attributes(self, task_id):
        self.task_ui_attributes_has_been_requested.emit(task_id)

    def request_node_ui(self, node_id):
        self.node_ui_has_been_requested.emit(node_id)

    def send_node_parameter_change(self, node_id: int, param_name: str, param: dict):
        self.node_parameter_change_requested.emit(node_id, param_name, param)

    def node_position(self, node_id: int):
        if self.__db_path is not None:
            with sqlite3.connect(self.__db_path) as con:
                con.row_factory = sqlite3.Row
                cur = con.execute('SELECT * FROM "nodes" WHERE "id" = ?', (node_id,))
                row = cur.fetchone()
                if row is not None:
                    return row['posx'], row['posy']

        return node_id * 125.79 % 400, node_id * 357.17 % 400  # TODO: do something better!

    @Slot(object)
    def full_update(self, uidata):
        print(uidata)

        to_del = []
        existing_node_ids = {}
        existing_conn_ids = {}
        existing_task_ids = {}
        for item in self.items():
            if isinstance(item,
                          Node):  # TODO: unify this repeating code and move the setting attribs to after all elements are created
                if item.get_id() not in uidata.nodes():
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
                    continue
                existing_task_ids[item.get_id()] = item
        for item in to_del:
            self.removeItem(item)

        for id, newdata in uidata.nodes().items():
            if id in existing_node_ids:
                continue
            new_node = Node(id, f'node #{id}')
            new_node.setPos(*self.node_position(id))
            existing_node_ids[id] = new_node
            self.addItem(new_node)

        for id, newdata in uidata.connections().items():
            if id in existing_conn_ids:
                continue
            new_conn = NodeConnection(id, existing_node_ids[newdata['node_id_out']],
                                      existing_node_ids[newdata['node_id_in']])
            existing_conn_ids[id] = new_conn
            self.addItem(new_conn)

        for id, newdata in uidata.tasks().items():
            if id not in existing_task_ids:
                new_task = Task(id, newdata['name'] or '<noname>')
                existing_task_ids[id] = new_task
                if newdata['origin_task_id'] is not None and newdata['origin_task_id'] in existing_task_ids:
                    origin_task = existing_task_ids[newdata['origin_task_id']]
                    new_task.setPos(origin_task.pos())
                self.addItem(new_task)
            task = existing_task_ids[id]
            #print(f'setting {task.get_id()} to {newdata["node_id"]}')
            existing_node_ids[newdata['node_id']].add_task(task)
            task.set_state(TaskState(newdata['state']))

    @Slot(object, object)
    def log_fetched(self, task_id: int, log: dict):
        try:
            task = self.get_task(task_id)
        except KeyError:
            print('log fetched, but task not found!')
            return
        task.update_log(log)

    @Slot(object, object)
    def nodeui_fetched(self, node_id: int, nodeui: NodeUi):
        try:
            node = self.get_node(node_id)
            node.update_nodeui(nodeui)
        except KeyError:
            print('node ui fetched for non existant node')

    @Slot(object, object)
    def task_attribs_fetched(self, task_id: int, attribs: dict):
        try:
            task = self.get_task(task_id)
        except KeyError:
            print('log fetched, but task not found!')
            return
        task.update_attributes(attribs)

    def addItem(self, item):
        super(QGraphicsImguiScene, self).addItem(item)
        if isinstance(item, Task):
            self.__task_dict[item.get_id()] = item
        elif isinstance(item, Node):
            self.__node_dict[item.get_id()] = item

    def removeItem(self, item):
        super(QGraphicsImguiScene, self).removeItem(item)
        if isinstance(item, Task):
            del self.__task_dict[item.get_id()]
        elif isinstance(item, Node):
            del self.__node_dict[item.get_id()]

    def clear(self):
        super(QGraphicsImguiScene, self).clear()
        self.__task_dict = {}
        self.__node_dict = {}

    def get_task(self, task_id):
        return self.__task_dict[task_id]

    def get_node(self, node_id):
        return self.__node_dict[node_id]

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


class SchedulerConnectionWorker(PySide2.QtCore.QObject):
    full_update = Signal(UiData)
    log_fetched = Signal(int, dict)
    nodeui_fetched = Signal(int, NodeUi)
    task_attribs_fetched = Signal(int, dict)

    def __init__(self, parent=None):
        super(SchedulerConnectionWorker, self).__init__(parent)
        self.__started = False
        self.__timer = None
        self.__to_stop = False
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

    def ensure_connected(self) -> bool:
        if self.__conn is not None:
            return True

        async def _interrupt_waiter():
            while True:
                if self.interruption_requested():
                    return None
                await asyncio.sleep(0.5)

        print('waiting for scheduler broadcast...')
        tasks = asyncio.run(asyncio.wait((
            await_broadcast('taskflow_scheduler'),
            _interrupt_waiter()), return_when=asyncio.FIRST_COMPLETED))

        print(tasks)
        message = list(tasks[0])[0].result()

        print(message)
        if message is None:
            return False
        print('received broadcast:', message)
        schedata = json.loads(message)
        sche_addr, sche_port = schedata['ui'].split(':')
        sche_port = int(sche_port)
        print('connecting to scheduler...')

        while not self.interruption_requested():
            try:
                self.__conn = socket.create_connection((sche_addr, sche_port), timeout=30)
            except ConnectionError:
                print('ui connection refused, retrying...')

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

    @Slot()
    def check_scheduler(self):
        print('schee')
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
            recvdata = recv_exactly(self.__conn, 4)
        except ConnectionError as e:
            print('connection reset', e)
            print('scheduler connection lost')
            self.__conn = None
            return
        if len(recvdata) != 4:  # means connection was closed
            print('scheduler connection lost')
            self.__conn = None
            return
        uidatasize = struct.unpack('>I', recvdata)[0]
        uidatabytes = recv_exactly(self.__conn, uidatasize)
        if len(uidatabytes) != uidatasize:
            print('scheduler connection lost')
            return
        uidata = UiData.deserialize(uidatabytes)
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
            print('failed ', e)
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
            rcvsize = struct.unpack('>I', recv_exactly(self.__conn, 4))[0]
            attribs = pickle.loads(recv_exactly(self.__conn, rcvsize))
        except ConnectionError as e:
            print('failed ', e)
        else:
            self.task_attribs_fetched.emit(task_id, attribs)

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
            print('failed ', e)
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
            print('failed ', e)
        else:
            self.nodeui_fetched.emit(node_id, nodeui)

    @Slot()
    def send_node_parameter_change(self, node_id: int, param_name: str, param: dict):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            param_type = param['type']
            param_value = param['value']
            self.__conn.sendall(b'setnodeparam\n')
            param_name_data = param_name.encode('UTF-8')
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
            print('failed', e)

# class SchedulerConnectionThread(QThread):
#     full_update = Signal(UiData)
#
#     def __init__(self, parent=None):
#         super(SchedulerConnectionThread, self).__init__(parent=parent)
#
#     def run(self) -> None:
#         while not self.isInterruptionRequested():
#             conn = None
#
#             async def _interrupt_waiter():
#                 while True:
#                     if self.isInterruptionRequested():
#                         return None
#                     await asyncio.sleep(0.5)
#
#             print('waiting for scheduler broadcast...')
#             tasks = asyncio.run(asyncio.wait((
#                 await_broadcast('taskflow_scheduler'),
#                 _interrupt_waiter()), return_when=asyncio.FIRST_COMPLETED))
#
#             print(tasks)
#             message = list(tasks[0])[0].result()
#
#             print(message)
#             if message is None:
#                 return
#             print('received broadcast:', message)
#             schedata = json.loads(message)
#             sche_addr, sche_port = schedata['ui'].split(':')
#             sche_port = int(sche_port)
#             print('connecting to scheduler...')
#
#             while not self.isInterruptionRequested():
#                 try:
#                     conn = socket.create_connection((sche_addr, sche_port), timeout=30)
#                 except ConnectionError:
#                     print('ui connection refused, retrying...')
#
#                     # now sleep, but listening to interrupt requests
#                     for i in range(25):
#                         time.sleep(0.2)
#                         if self.isInterruptionRequested():
#                             return
#                 else:
#                     break
#             if self.isInterruptionRequested():
#                 return
#             # at this point conn cannot be None
#             assert conn is not None
#             conn.sendall(b'\0\0\0\0')
#             while not self.isInterruptionRequested():
#                 try:
#                     conn.sendall(b'getfullstate\n')
#                     recvdata = conn.recv(4)
#                 except ConnectionResetError as e:
#                     print('connection reset', e)
#                     break
#                 if len(recvdata) != 4:  # means connection was closed
#                     break
#                 uidatasize = struct.unpack('>I', recvdata)[0]
#                 uidatabytes = conn.recv(uidatasize)
#                 if len(uidatabytes) != uidatasize:
#                     break
#                 uidata = UiData.deserialize(uidatabytes)
#                 self.full_update.emit(uidata)
#
#                 # now sleep, but listening to interrupt requests
#                 for i in range(5):
#                     time.sleep(0.2)
#                     if self.isInterruptionRequested():
#                         break
#             if not self.isInterruptionRequested():
#                 print('scheduler connection lost')
#             conn.close()


class NodeEditor(QGraphicsView):
    def __init__(self, db_path: str = None, parent=None):
        super(NodeEditor, self).__init__(parent=parent)

        self.__oglwidget = QOpenGLWidgetWithSomeShit()
        self.setViewport(self.__oglwidget)
        self.setMouseTracking(True)
        self.setViewportUpdateMode(QGraphicsView.FullViewportUpdate)
        self.setCacheMode(QGraphicsView.CacheBackground)

        self.__ui_panning_lastpos = None

        self.__scene = QGraphicsImguiScene(db_path)
        #node1 = Node("first node", 0)
        #node2 = Node("second node", 1)
        #con = NodeConnection(node1, node2)
        #self.__scene.addItem(node1)
        #self.__scene.addItem(node2)
        #self.__scene.addItem(con)
        #node2.setPos(100, 150)
        self.setScene(self.__scene)
        #self.__update_timer = PySide2.QtCore.QTimer(self)
        #self.__update_timer.timeout.connect(lambda: self.__scene.invalidate(layers=QGraphicsScene.ForegroundLayer))
        #self.__update_timer.setInterval(50)
        #self.__update_timer.start()

        self.__imgui_init = False
        self.update()

    def drawForeground(self, painter: PySide2.QtGui.QPainter, rect: QRectF) -> None:
        #print('asd')
        painter.beginNativePainting()
        if not self.__imgui_init:
            print('initdf')
            self.__imgui_init = True
            imgui.create_context()
            self.__imimpl = ProgrammablePipelineRenderer()
            imgui.get_io().display_size = 400, 400

        imgui.get_io().display_size = rect.size().toTuple()
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
        imgui.begin("Your first window!", True)

        # draw text label inside of current window
        imgui.text("Hello world!")
        sel = self.__scene.selectedItems()
        if len(sel) > 0 and isinstance(sel[0], NetworkItemWithUI):
            sel[0].draw_imgui_elements()

        # close current window context
        imgui.end()

        # pass all drawing comands to the rendering pipeline
        # and close frame context
        imgui.render()
        # imgui.end_frame()
        self.__imimpl.render(imgui.get_draw_data())
        painter.endNativePainting()

    def imguiProcessEvents(self, event: PySide2.QtGui.QInputEvent, do_recache=True):
        io = imgui.get_io()
        if isinstance(event, PySide2.QtGui.QMouseEvent):
            io.mouse_pos = event.windowPos().toTuple()
        elif isinstance(event, PySide2.QtGui.QWheelEvent):
            io.mouse_wheel = event.angleDelta().y() / 100
        io.mouse_down[0] = event.buttons() & Qt.LeftButton
        io.mouse_down[1] = event.buttons() & Qt.MiddleButton
        io.mouse_down[2] = event.buttons() & Qt.RightButton
        if do_recache:
            self.resetCachedContent()

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
                self.setSceneRect(rect.translated(*(self.__ui_panning_lastpos - event.screenPos()).toTuple()))
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
            super(NodeEditor, self).wheelEvent(event)

    def closeEvent(self, event: PySide2.QtGui.QCloseEvent) -> None:
        self.__scene.stop()
        self.__scene.save_node_layout()
        super(NodeEditor, self).closeEvent(event)


def _main():
    qapp = QApplication(sys.argv)

    db_path = os.path.join(os.getcwd(), 'node_viewer.db')
    hgt, wgt = None, None
    posx, posy = None, None
    with sqlite3.connect(db_path) as con:
        con.row_factory = sqlite3.Row
        cur = con.execute('SELECT * FROM widgets WHERE "name" = ?', ('main',))
        row = cur.fetchone()
        if row is not None:
            hgt = row['height']
            wgt = row['width']
            posx = row['posx']
            posy = row['posy']
            if row['scene_x'] is not None:
                scene_rect = QRectF(row['scene_x'], row['scene_y'], row['scene_w'], row['scene_h'])
            else:
                scene_rect = None

    widget = NodeEditor(db_path)
    if hgt is not None:
        widget.resize(wgt, hgt)
    if posx is not None:
        widget.move(posx, posy)
    if scene_rect is not None:
        widget.setSceneRect(scene_rect)
    widget.show()

    qapp.exec_()
    with sqlite3.connect(db_path) as con:
        scene_rect = widget.sceneRect()
        con.execute('INSERT OR REPLACE INTO widgets ("name", "width", "height", "posx", "posy", '
                    '"scene_x", "scene_y", "scene_w", "scene_h") '
                    'VALUES (?, ?, ?, ?, ?, '
                    '?, ?, ?, ?)',
                    ('main', *widget.size().toTuple(), *widget.pos().toTuple(),
                     *scene_rect.topLeft().toTuple(), *scene_rect.size().toTuple()))
        con.commit()



if __name__ == '__main__':
    _main()
