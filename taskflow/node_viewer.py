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
from PySide2.QtCore import Qt, Slot, Signal, QThread, QRectF, QSizeF, QPointF, QAbstractAnimation, QSequentialAnimationGroup, QEvent
from PySide2.QtGui import QPen, QBrush, QColor, QPainterPath, QKeyEvent


import imgui
from imgui.integrations.opengl import ProgrammablePipelineRenderer

from typing import Optional, List, Tuple, Dict, Set, Callable


def call_later(callable, *args, **kwargs):
    if len(args) == 0 and len(kwargs) == 0:
        PySide2.QtCore.QTimer.singleShot(0, callable)
    else:
        PySide2.QtCore.QTimer.singleShot(0, lambda: callable(*args, **kwargs))


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
    # cache node type-2-inputs/outputs names, not to ask a million times for every node
    _node_inputs_outputs_cached: Dict[str, Tuple[List[str], List[str]]] = {}

    def __init__(self, id: int, type: str, name: str):
        super(Node, self).__init__(id)
        self.setFlags(QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemIsSelectable | QGraphicsItem.ItemSendsGeometryChanges)
        self.__height = 75
        self.__width = 150
        self.__input_radius = 7
        self.__line_width = 1
        self.__name = name
        self.__tasks: List["Task"] = []
        self.__node_type = type

        self.__some_value = 50

        self.__ui_interactor = None
        self.__ui_widget: Optional[NodeEditor] = None
        self.__ui_grabbed_conn = None

        # prepare drawing tools
        self.__borderpen= QPen(QColor(96, 96, 96, 255))
        self.__borderpen_selected = QPen(QColor(144, 144, 144, 255))
        self.__caption_pen = QPen(QColor(192, 192, 192, 255))
        self.__borderpen.setWidthF(self.__line_width)
        self.__header_brush = QBrush(QColor(48, 64, 48, 192))
        self.__body_brush = QBrush(QColor(48, 48, 48, 128))

        self.__nodeui: Optional[NodeUi] = None
        self.__connections: Set[NodeConnection] = set()
        self.__expanded = False

        self.__inputs, self.__outputs = None, None
        self.__node_ui_for_io_requested = False
        if self.__node_type in Node._node_inputs_outputs_cached:
            self.__inputs, self.__outputs = Node._node_inputs_outputs_cached[self.__node_type]

    def node_type(self) -> str:
        return self.__node_type

    def update_nodeui(self, nodeui: NodeUi):
        self.__nodeui = nodeui
        Node._node_inputs_outputs_cached[self.__node_type] = (list(nodeui.inputs_names()), list(nodeui.outputs_names()))
        self.__inputs, self.__outputs = Node._node_inputs_outputs_cached[self.__node_type]
        self.update_ui()

    def input_snap_points(self):
        # TODO: cache snap points, don't recalc them every time
        if self.__nodeui is None:
            return []
        inputs = []
        for input_name in self.__nodeui.inputs_names():
            inputs.append(NodeConnSnapPoint(self, input_name, True))
        return inputs

    def output_snap_points(self):
        # TODO: cache snap points, don't recalc them every time
        if self.__nodeui is None:
            return []
        outputs = []
        for output_name in self.__nodeui.outputs_names():
            outputs.append(NodeConnSnapPoint(self, output_name, False))
        return outputs

    def boundingRect(self) -> QRectF:
        lw = self.__width + self.__line_width
        lh = self.__height + self.__line_width
        return QRectF(-0.5 * lw, -0.5 * lh - self.__input_radius, lw, lh + 2 * self.__input_radius)

    def paint(self, painter: PySide2.QtGui.QPainter, option: QStyleOptionGraphicsItem, widget: Optional[QWidget] = None) -> None:
        painter.pen().setWidthF(self.__line_width)
        lw = self.__width + self.__line_width
        lh = self.__height + self.__line_width

        nodeshape = QPainterPath()
        nodeshape.addRoundedRect(QRectF(-0.5 * lw, -0.5 * lh, lw, lh), 5, 5)

        if not self.__node_ui_for_io_requested:
            assert self.scene() is not None
            self.__node_ui_for_io_requested = True
            self.scene().request_node_ui(self.get_id())
        if self.__inputs is not None and self.__outputs is not None:
            ninputs = len(self.__inputs)
            noutputs = len(self.__outputs)
            for i in range(len(self.__inputs)):
                path = QPainterPath()
                path.addEllipse(QPointF(-0.5 * self.__width + (i + 1) * self.__width/(ninputs + 1), -0.5 * self.__height), self.__input_radius, self.__input_radius)
                path -= nodeshape
                painter.setPen(self.__borderpen)
                painter.drawPath(path)
            for i in range(len(self.__outputs)):
                path = QPainterPath()
                path.addEllipse(QPointF(-0.5 * self.__width + (i + 1) * self.__width/(noutputs + 1), 0.5 * self.__height), self.__input_radius, self.__input_radius)
                path -= nodeshape
                painter.setPen(self.__borderpen)
                painter.drawPath(path)


        bodymask = QPainterPath()
        bodymask.addRect(-0.5 * lw, -0.5 * lh + 16, lw, lh - 16)
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
        painter.drawText(headershape.boundingRect(), Qt.AlignHCenter | Qt.AlignTop, self.__name)

    def get_input_position(self, name: str = 'main') -> QPointF:
        if self.__inputs is None:
            idx = 0
            cnt = 1
        elif name not in self.__inputs:
            raise RuntimeError(f'unexpected input name {name}')
        else:
            idx = self.__inputs.index(name)
            cnt = len(self.__inputs)
        assert cnt > 0
        return self.mapToScene(-0.5 * self.__width + (idx + 1) * self.__width/(cnt + 1), -0.5 * self.__height)

    def get_output_position(self, name: str = 'main') -> QPointF:
        if self.__outputs is None:
            idx = 0
            cnt = 1
        elif name not in self.__outputs:
            raise RuntimeError(f'unexpected output name {name} , {self.__outputs}')
        else:
            idx = self.__outputs.index(name)
            cnt = len(self.__outputs)
        assert cnt > 0
        return self.mapToScene(-0.5 * self.__width + (idx + 1) * self.__width/(cnt + 1), 0.5 * self.__height)

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

    def add_connection(self, new_connection: "NodeConnection"):
        self.__connections.add(new_connection)

    def remove_connection(self, connection: "NodeConnection"):
        self.__connections.remove(connection)

    def itemChange(self, change, value):
        if change == QGraphicsItem.ItemSelectedHasChanged:
            if value:   # item was just selected
                self.scene().request_node_ui(self.get_id())
        elif change == QGraphicsItem.ItemSceneChange:
            conns = self.__connections.copy()
            for connection in conns:
                if self.scene() is not None and value != self.scene():
                    print('removing connections...')
                    assert connection.scene() is not None
                    connection.scene().removeItem(connection)
            assert len(self.__connections) == 0
        elif change == QGraphicsItem.ItemPositionChange:
            for connection in self.__connections:
                connection.prepareGeometryChange()
        #print(change, value)

        return super(Node, self).itemChange(change, value)

    def mousePressEvent(self, event: QGraphicsSceneMouseEvent):
        if self.__ui_interactor is None:
            pos = event.scenePos()
            r2 = self.__input_radius**2
            node_viewer = event.widget().parent()
            assert isinstance(node_viewer, NodeEditor)

            # check expand button


            for input in self.__inputs:
                inpos = self.get_input_position(input)
                if QPointF.dotProduct(inpos - pos, inpos - pos) <= r2 and node_viewer.request_ui_focus(self):
                    snap_points = [y for x in self.scene().nodes() if x != self for y in x.output_snap_points()]
                    displayer = NodeConnectionCreatePreview(None, self, '', input, snap_points, 15, self._ui_interactor_finished)
                    self.scene().addItem(displayer)
                    self.__ui_interactor = displayer
                    self.__ui_grabbed_conn = input
                    self.__ui_widget = node_viewer
                    event.accept()
                    self.__ui_interactor.mousePressEvent(event)
                    return

            for output in self.__outputs:
                outpos = self.get_output_position(output)
                if QPointF.dotProduct(outpos - pos, outpos - pos) <= r2 and node_viewer.request_ui_focus(self):
                    snap_points = [y for x in self.scene().nodes() if x != self for y in x.input_snap_points()]
                    displayer = NodeConnectionCreatePreview(self, None, output, '', snap_points, 15, self._ui_interactor_finished)
                    self.scene().addItem(displayer)
                    self.__ui_interactor = displayer
                    self.__ui_grabbed_conn = output
                    self.__ui_widget = node_viewer
                    event.accept()
                    self.__ui_interactor.mousePressEvent(event)
                    return
        super(Node, self).mousePressEvent(event)

    def mouseMoveEvent(self, event: QGraphicsSceneMouseEvent):
        if self.__ui_interactor is not None:
            event.accept()
            self.__ui_interactor.mouseMoveEvent(event)
            return
        super(Node, self).mouseMoveEvent(event)

    def mouseReleaseEvent(self, event: QGraphicsSceneMouseEvent):
        if self.__ui_interactor is not None:
            event.accept()
            self.__ui_interactor.mouseReleaseEvent(event)
            return
        super(Node, self).mouseReleaseEvent(event)

    @Slot(object)
    def _ui_interactor_finished(self, snap_point: Optional["NodeConnSnapPoint"]):
        assert self.__ui_interactor is not None
        call_later(lambda x: print('bloop', x) or self.scene().removeItem(x), self.__ui_interactor)
        # NodeConnection._dbg_shitlist.append(self.__ui_interactor)
        grabbed_conn = self.__ui_grabbed_conn
        self.__ui_widget.release_ui_focus(self)
        self.__ui_widget = None
        self.__ui_interactor = None
        self.__ui_grabbed_conn = None

        # actual node reconection
        if snap_point is None:
            print('no change')
            return
        scene: QGraphicsImguiScene = self.scene()
        setting_out = not snap_point.connection_is_input()
        scene.request_node_connection_add(snap_point.node().get_id() if setting_out else self.get_id(),
                                          snap_point.connection_name() if setting_out else grabbed_conn,
                                          snap_point.node().get_id() if not setting_out else self.get_id(),
                                          snap_point.connection_name() if not setting_out else grabbed_conn)

    def keyPressEvent(self, event: QKeyEvent):
        if event.key() == Qt.Key_Delete:
            self.scene().request_remove_node(self.get_id())
        event.accept()


class NodeConnection(NetworkItem):
    def __init__(self, id: int, nodeout: Node, nodein: Node, outname: str, inname: str):
        super(NodeConnection, self).__init__(id)
        self.setFlags(QGraphicsItem.ItemIsSelectable)
        self.__nodeout = nodeout
        self.__nodein = nodein
        self.__outname = outname
        self.__inname = inname
        self.setZValue(-1)
        self.__line_width = 6  # TODO: rename it to match what it represents
        self.__pick_radius2 = 50**2

        self.__ui_interactor: Optional[NodeConnectionCreatePreview] = None
        self.__ui_widget: Optional[NodeEditor] = None
        self.__ui_last_pos = QPointF()
        self.__ui_grabbed_beginning: bool = True

        self.__pen = QPen(QColor(64, 64, 64, 192))
        self.__pen.setWidthF(3)
        self.__thick_pen = QPen(QColor(144, 144, 144, 128))
        self.__thick_pen.setWidthF(4)
        self.__last_drawn_path: Optional[QPainterPath] = None

    def boundingRect(self) -> QRectF:
        hlw = self.__line_width
        line = self.get_painter_path()
        return line.boundingRect().adjusted(-hlw, -hlw, hlw, hlw)
        # inputpos = self.__nodeout.get_output_position(self.__outname)
        # outputpos = self.__nodein.get_input_position(self.__inname)
        # return QRectF(QPointF(min(inputpos.x(), outputpos.x()) - hlw, min(inputpos.y(), outputpos.y()) - hlw),
        #               QPointF(max(inputpos.x(), outputpos.x()) + hlw, max(inputpos.y(), outputpos.y()) + hlw))

    def get_painter_path(self, close_path=False):
        line = QPainterPath()

        p0 = self.__nodeout.get_output_position(self.__outname)
        p1 = self.__nodein.get_input_position(self.__inname)
        line.moveTo(p0)
        line.cubicTo(p0 + QPointF(0, 150), p1 - QPointF(0, 150), p1)
        if close_path:
            line.cubicTo(p1 - QPointF(0, 150), p0 + QPointF(0, 150), p0)
        return line

    def paint(self, painter: PySide2.QtGui.QPainter, option: QStyleOptionGraphicsItem, widget: Optional[QWidget] = None) -> None:
        line = self.get_painter_path()

        if self.isSelected():
            painter.setPen(self.__thick_pen)
            painter.drawPath(line)
        painter.setPen(self.__pen)
        painter.drawPath(line)
        # painter.drawRect(self.boundingRect())
        self.__last_drawn_path = line

    def output(self) -> (Node, str):
        return self.__nodeout, self.__outname

    def input(self) -> (Node, str):
        return self.__nodein, self.__inname

    def set_output(self, node: Node, output_name: str = 'main'):
        print(f'reassigning NodeConnection output to {node.get_id()}, {output_name}')
        assert node is not None
        self.prepareGeometryChange()
        if node != self.__nodeout:
            self.__nodeout.remove_connection(self)
            self.__nodeout = node
            self.__nodeout.add_connection(self)
        self.__outname = output_name

    def set_input(self, node: Node, input_name: str = 'main'):
        print(f'reassigning NodeConnection input to {node.get_id()}, {input_name}')
        assert node is not None
        self.prepareGeometryChange()
        if node != self.__nodein:
            self.__nodein.remove_connection(self)
            self.__nodein = node
            self.__nodein.add_connection(self)
        self.__inname = input_name

    def mousePressEvent(self, event: QGraphicsSceneMouseEvent):
        print('proper beep', self)
        line = self.get_painter_path(close_path=True)
        pick_radius = 10
        circle = QPainterPath()
        circle.addEllipse(event.scenePos(), pick_radius, pick_radius)
        if self.__ui_interactor is None and line.intersects(circle):
            print('---GOT A PEAK AT MY DICK---')
            wgt = event.widget()
            if wgt is None:
                return

            p = event.scenePos()
            p0 = self.__nodeout.get_output_position(self.__outname)
            p1 = self.__nodein.get_input_position(self.__inname)
            d02 = QPointF.dotProduct(p0 - p, p0 - p)
            d12 = QPointF.dotProduct(p1 - p, p1 - p)
            if d02 > self.__pick_radius2 and d12 > self.__pick_radius2:  # if picked too far from ends - just select
                super(NodeConnection, self).mousePressEvent(event)
                event.accept()
                return

            node_viewer = wgt.parent()
            assert isinstance(node_viewer, NodeEditor)
            if node_viewer.request_ui_focus(self):
                event.accept()

                output_picked = d02 < d12
                if output_picked:
                    snap_points = [y for x in self.scene().nodes() if x != self.__nodein for y in x.output_snap_points() ]
                else:
                    snap_points = [y for x in self.scene().nodes() if x != self.__nodeout for y in x.input_snap_points()]
                self.__ui_interactor = NodeConnectionCreatePreview(None if output_picked else self.__nodeout,
                                                                   self.__nodein if output_picked else None,
                                                                   self.__outname, self.__inname,
                                                                   snap_points, 15, self._ui_interactor_finished)
                self.__ui_widget = node_viewer
                self.scene().addItem(self.__ui_interactor)
                self.__ui_interactor.mouseMoveEvent(event)

    def mouseMoveEvent(self, event: QGraphicsSceneMouseEvent) -> None:
        if self.__ui_interactor is not None:  # redirect input, cuz scene will direct all events to this item. would be better to change focus, but so far scene.setFocusItem did not work as expected
            self.__ui_interactor.mouseMoveEvent(event)
            event.accept()

    def mouseReleaseEvent(self, event: QGraphicsSceneMouseEvent) -> None:
        if self.__ui_interactor is not None:  # redirect input, cuz scene will direct all events to this item. would be better to change focus, but so far scene.setFocusItem did not work as expected
            self.__ui_interactor.mouseReleaseEvent(event)
            event.accept()

    def keyPressEvent(self, event: QKeyEvent):
        if event.key() == Qt.Key_Delete:
            self.scene().request_node_connection_remove(self.get_id())
        event.accept()

    # _dbg_shitlist = []
    @Slot(object)
    def _ui_interactor_finished(self, snap_point: Optional["NodeConnSnapPoint"]):
        assert self.__ui_interactor is not None
        call_later(lambda x: print('bloop', x) or self.scene().removeItem(x), self.__ui_interactor)
        # NodeConnection._dbg_shitlist.append(self.__ui_interactor)
        self.__ui_widget.release_ui_focus(self)
        self.__ui_widget = None
        self.__ui_interactor = None

        # actual node reconection
        if snap_point is None:
            print('no change')
            return
        scene: QGraphicsImguiScene = self.scene()
        changing_out = not snap_point.connection_is_input()
        scene.request_node_connection_change(self.get_id(),
                                             snap_point.node().get_id() if changing_out else None,
                                             snap_point.connection_name() if changing_out else None,
                                             None if changing_out else snap_point.node().get_id(),
                                             None if changing_out else snap_point.connection_name())

    def itemChange(self, change: QGraphicsItem.GraphicsItemChange, value):
        if change == QGraphicsItem.ItemSceneChange:
            if value == self.__nodein.scene():
                self.__nodein.add_connection(self)
            else:
                self.__nodein.remove_connection(self)
            if value == self.__nodeout.scene():
                self.__nodeout.add_connection(self)
            else:
                self.__nodeout.remove_connection(self)
        return super(NodeConnection, self).itemChange(change, value)


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
        elif change == QGraphicsItem.ItemSceneChange:
            if value is None:  # removing item from scene
                if self.__node is not None:
                    self.__node.remove_task(self)
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


class SnapPoint:
    def pos(self) -> QPointF:
        raise NotImplementedError()


class NodeConnSnapPoint(SnapPoint):
    def __init__(self, node: Node, connection_name: str, connection_is_input: bool):
        super(NodeConnSnapPoint, self).__init__()
        self.__node = node
        self.__conn_name = connection_name
        self.__isinput = connection_is_input

    def node(self) -> Node:
        return self.__node

    def connection_name(self) -> str:
        return self.__conn_name

    def connection_is_input(self) -> bool:
        return self.__isinput

    def pos(self) -> QPointF:
        if self.__isinput:
            return self.__node.get_input_position(self.__conn_name)
        return self.__node.get_output_position(self.__conn_name)


class NodeConnectionCreatePreview(QGraphicsItem):
    def __init__(self, nodeout: Optional[Node], nodein: Optional[Node], outname: str, inname: str, snap_points: List[NodeConnSnapPoint], snap_radius: float, report_done_here: Callable):
        super(NodeConnectionCreatePreview, self).__init__()
        assert nodeout is None and nodein is not None or \
               nodeout is not None and nodein is None
        self.setFlags(QGraphicsItem.ItemSendsGeometryChanges)
        self.__nodeout = nodeout
        self.__nodein = nodein
        self.__outname = outname
        self.__inname = inname
        self.__snappoints = snap_points
        self.__snap_radius2 = snap_radius * snap_radius
        self.setZValue(-1)
        self.__line_width = 4

        self.__ui_last_pos = QPointF()
        self.__finished_callback = report_done_here

        self.__pen = QPen(QColor(64, 64, 64, 192))
        self.__pen.setWidthF(3)

    def get_painter_path(self):
        if self.__nodein is not None:
            p0 = self.__ui_last_pos
            p1 = self.__nodein.get_input_position(self.__inname)
        else:
            p0 = self.__nodeout.get_output_position(self.__outname)
            p1 = self.__ui_last_pos

        line = QPainterPath()
        line.moveTo(p0)
        line.cubicTo(p0 + QPointF(0, 150), p1 - QPointF(0, 150), p1)
        return line

    def boundingRect(self) -> QRectF:
        hlw = self.__line_width

        if self.__nodein is not None:
            inputpos = self.__ui_last_pos
            outputpos = self.__nodein.get_input_position(self.__inname)
        else:
            inputpos = self.__nodeout.get_output_position(self.__outname)
            outputpos = self.__ui_last_pos

        return QRectF(QPointF(min(inputpos.x(), outputpos.x()) - hlw, min(inputpos.y(), outputpos.y()) - hlw),
                      QPointF(max(inputpos.x(), outputpos.x()) + hlw, max(inputpos.y(), outputpos.y()) + hlw))

    def paint(self, painter: PySide2.QtGui.QPainter, option: QStyleOptionGraphicsItem, widget: Optional[QWidget] = None) -> None:
        line = self.get_painter_path()
        painter.setPen(self.__pen)
        painter.drawPath(line)
        # painter.drawRect(self.boundingRect())

    def mousePressEvent(self, event: QGraphicsSceneMouseEvent):
        pos = event.scenePos()
        closest_snap = self.get_closest_snappoint(pos)
        if closest_snap is not None:
            pos = closest_snap.pos()
        self.prepareGeometryChange()
        self.__ui_last_pos = pos
        event.accept()

    def mouseMoveEvent(self, event):
        pos = event.scenePos()
        closest_snap = self.get_closest_snappoint(pos)
        if closest_snap is not None:
            pos = closest_snap.pos()
        self.prepareGeometryChange()
        self.__ui_last_pos = pos
        event.accept()

    def get_closest_snappoint(self, pos: QPointF) -> Optional[NodeConnSnapPoint]:
        def qpflength2(p: QPointF):
            return QPointF.dotProduct(p, p)

        snappoints = [x for x in self.__snappoints if qpflength2(x.pos() - pos) < self.__snap_radius2]

        if len(snappoints) == 0:
            return None

        return min(snappoints, key=lambda x: qpflength2(x.pos() - pos))

    def mouseReleaseEvent(self, event: QGraphicsSceneMouseEvent):
        if self.__finished_callback is not None:
            self.__finished_callback(self.get_closest_snappoint(event.scenePos()))
        event.accept()


class QOpenGLWidgetWithSomeShit(QOpenGLWidget):
    def initializeGL(self) -> None:
        super(QOpenGLWidgetWithSomeShit, self).initializeGL()
        print('init')


class QGraphicsImguiScene(QGraphicsScene):
    # these are private signals to invoke shit on worker in another thread. QMetaObject's invokemethod is broken in pyside2
    _signal_log_has_been_requested = Signal(int, int, int)
    _signal_log_meta_has_been_requested = Signal(int)
    _signal_node_ui_has_been_requested = Signal(int)
    _signal_task_ui_attributes_has_been_requested = Signal(int)
    _signal_node_parameter_change_requested = Signal(int, str, dict)
    _signal_nodetypes_update_requested = Signal()
    _signal_create_node_requested = Signal(str, str, QPointF)
    _signal_remove_node_requested = Signal(int)
    _signal_change_node_connection_requested = Signal(int, object, object, object, object)
    _signal_remove_node_connection_requested = Signal(int)
    _signal_add_node_connection_requested = Signal(int, str, int, str)

    nodetypes_updated = Signal(dict)  # TODO: separate worker-oriented "private" signals for readability

    def __init__(self, db_path: str = None, parent=None):
        super(QGraphicsImguiScene, self).__init__(parent=parent)
        # to debug fuching bsp # self.setItemIndexMethod(QGraphicsScene.NoIndex)
        self.__task_dict: Dict[int, Task] = {}
        self.__node_dict: Dict[int, Node] = {}
        self.__db_path = db_path
        self.__cached_nodetypes = None

        self.__ui_connection_thread = QThread(self)  # SchedulerConnectionThread(self)
        self.__ui_connection_worker = SchedulerConnectionWorker()
        self.__ui_connection_worker.moveToThread(self.__ui_connection_thread)

        self.__ui_connection_thread.started.connect(self.__ui_connection_worker.start)
        self.__ui_connection_thread.finished.connect(self.__ui_connection_worker.finish)

        self.__ui_connection_worker.full_update.connect(self.full_update)
        self.__ui_connection_worker.log_fetched.connect(self.log_fetched)
        self.__ui_connection_worker.nodeui_fetched.connect(self.nodeui_fetched)
        self.__ui_connection_worker.task_attribs_fetched.connect(self.task_attribs_fetched)
        self.__ui_connection_worker.nodetypes_fetched.connect(self._nodetypes_fetched)
        self.__ui_connection_worker.node_created.connect(self._node_created)

        self._signal_log_has_been_requested.connect(self.__ui_connection_worker.get_log)
        self._signal_log_meta_has_been_requested.connect(self.__ui_connection_worker.get_log_metadata)
        self._signal_node_ui_has_been_requested.connect(self.__ui_connection_worker.get_nodeui)
        self._signal_task_ui_attributes_has_been_requested.connect(self.__ui_connection_worker.get_task_attribs)
        self._signal_node_parameter_change_requested.connect(self.__ui_connection_worker.send_node_parameter_change)
        self._signal_nodetypes_update_requested.connect(self.__ui_connection_worker.get_nodetypes)
        self._signal_create_node_requested.connect(self.__ui_connection_worker.create_node)
        self._signal_remove_node_requested.connect(self.__ui_connection_worker.remove_node)
        self._signal_change_node_connection_requested.connect(self.__ui_connection_worker.change_node_connection)
        self._signal_remove_node_connection_requested.connect(self.__ui_connection_worker.remove_node_connection)
        self._signal_add_node_connection_requested.connect(self.__ui_connection_worker.add_node_connection)
        # self.__ui_connection_thread.full_update.connect(self.full_update)

        self.__ui_connection_thread.start()

    def request_log(self, task_id, node_id, invocation_id):
        self._signal_log_has_been_requested.emit(task_id, node_id, invocation_id)

    def request_log_meta(self, task_id):
        self._signal_log_meta_has_been_requested.emit(task_id)

    def request_attributes(self, task_id):
        self._signal_task_ui_attributes_has_been_requested.emit(task_id)

    def request_node_ui(self, node_id):
        self._signal_node_ui_has_been_requested.emit(node_id)

    def send_node_parameter_change(self, node_id: int, param_name: str, param: dict):
        self._signal_node_parameter_change_requested.emit(node_id, param_name, param)

    def request_node_types_update(self):
        self._signal_nodetypes_update_requested.emit()

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
    def full_update(self, uidata: UiData):
        print(uidata)

        to_del = []
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
                    continue
                existing_task_ids[item.get_id()] = item

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
                continue
            new_node = Node(id, newdata['type'], f'{newdata["type"]}: ' + (newdata['name'] or f'node #{id}'))
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
                new_task = Task(id, newdata['name'] or '<noname>')
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
        print('adding item', item)
        super(QGraphicsImguiScene, self).addItem(item)
        if isinstance(item, Task):
            self.__task_dict[item.get_id()] = item
        elif isinstance(item, Node):
            self.__node_dict[item.get_id()] = item
        print('added item')

    def removeItem(self, item):
        print('removing item', item)
        if item.scene() != self:
            print('item was already removed, just removing ids from internal caches')
        else:
            super(QGraphicsImguiScene, self).removeItem(item)
        if isinstance(item, Task):
            assert item.get_id() in self.__task_dict, 'inconsistency in internal caches. maybe item was doubleremoved?'
            del self.__task_dict[item.get_id()]
        elif isinstance(item, Node):
            assert item.get_id() in self.__node_dict, 'inconsistency in internal caches. maybe item was doubleremoved?'
            del self.__node_dict[item.get_id()]
        print('item removed')

    def clear(self):
        super(QGraphicsImguiScene, self).clear()
        self.__task_dict = {}
        self.__node_dict = {}

    def get_task(self, task_id) -> Task:
        return self.__task_dict[task_id]

    def get_node(self, node_id) -> Node:
        return self.__node_dict[node_id]

    def nodes(self) -> Tuple[Node]:
        return tuple(self.__node_dict.values())

    def tasks(self) -> Tuple[Task]:
        return tuple(self.__task_dict.values())

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

class SchedulerConnectionWorker(PySide2.QtCore.QObject):
    full_update = Signal(UiData)
    log_fetched = Signal(int, dict)
    nodeui_fetched = Signal(int, NodeUi)
    task_attribs_fetched = Signal(int, dict)
    nodetypes_fetched = Signal(dict)
    node_created = Signal(int, str, str, QPointF)

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

    def _send_string(self, text: str):
        bts = text.encode('UTF-8')
        self.__conn.sendall(struct.pack('>Q', len(bts)))
        self.__conn.sendall(bts)

    def _recv_string(self):
        btlen = struct.unpack('>Q', recv_exactly(self.__conn, 8))[0]
        return recv_exactly(self.__conn, btlen).decode('UTF-8')

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
            recvdata = recv_exactly(self.__conn, 8)
        except ConnectionError as e:
            print('connection reset', e)
            print('scheduler connection lost')
            self.__conn = None
            return
        if len(recvdata) != 8:  # means connection was closed
            print('scheduler connection lost')
            self.__conn = None
            return
        uidatasize = struct.unpack('>Q', recvdata)[0]
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

    @Slot()
    def get_nodetypes(self):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        nodetypes = {}
        try:
            names = []
            self.__conn.sendall(b'listnodetypes\n')
            elemcount = struct.unpack('>Q', recv_exactly(self.__conn, 8))[0]
            for i in range(elemcount):
                names.append(self._recv_string())
            nodetypes = {n: {} for n in names}
        except ConnectionError as e:
            print('failed', e)
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
            print('failed', e)
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
            print('failed', e)

    @Slot()
    def change_node_connection(self, connection_id: int, outnode_id: Optional[int] = None, outname: Optional[str] = None, innode_id: Optional[int] = None, inname: Optional[str] = None):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            print(connection_id, outnode_id, outname, innode_id, inname)
            self.__conn.sendall(b'changeconnection\n')
            self.__conn.sendall(struct.pack('>Q??QQ', connection_id, outnode_id is not None, innode_id is not None, outnode_id or 0, innode_id or 0))
            if outnode_id is not None:
                self._send_string(outname)
            if innode_id is not None:
                self._send_string(inname)
        except ConnectionError as e:
            print('failed', e)

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
            print('failed', e)

    @Slot()
    def remove_node_connection(self, connection_id: int):
        if not self.ensure_connected():
            return
        assert self.__conn is not None
        try:
            self.__conn.sendall(b'removeconnection\n')
            self.__conn.sendall(struct.pack('>Q', connection_id))
        except ConnectionError as e:
            print('failed', e)


class NodeEditor(QGraphicsView):
    def __init__(self, db_path: str = None, parent=None):
        super(NodeEditor, self).__init__(parent=parent)

        self.__oglwidget = QOpenGLWidgetWithSomeShit()
        self.setViewport(self.__oglwidget)
        self.setMouseTracking(True)
        self.setViewportUpdateMode(QGraphicsView.FullViewportUpdate)
        self.setCacheMode(QGraphicsView.CacheBackground)

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
        self.__node_types = {}

        self.__scene.nodetypes_updated.connect(self._nodetypes_updated)

        self.__scene.request_node_types_update()

        self.__imgui_init = False
        self.update()

    @Slot()
    def _nodetypes_updated(self, nodetypes):
        self.__node_types = nodetypes

    def drawForeground(self, painter: PySide2.QtGui.QPainter, rect: QRectF) -> None:
        #print('asd')
        painter.beginNativePainting()
        if not self.__imgui_init:
            print('initdf')
            self.__imgui_init = True
            imgui.create_context()
            self.__imimpl = ProgrammablePipelineRenderer()
            imgui.get_io().display_size = 400, 400
            self._map_keys()

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
        imgui.begin("Parameters", True)

        # draw text label inside of current window
        imgui.text("Hello world!")
        sel = self.__scene.selectedItems()
        if len(sel) > 0 and isinstance(sel[0], NetworkItemWithUI):
            sel[0].draw_imgui_elements()

        # close current window context
        imgui.end()

        if self.__create_menu_popup_toopen:
            imgui.open_popup('create node')
            self.__node_type_input = ''

        if imgui.begin_popup('create node'):
            #if self.__create_menu_popup_toopen:
            imgui.set_keyboard_focus_here()
            _, self.__node_type_input = imgui.input_text('', self.__node_type_input, 256)
            for type_name in self.__node_types:
                if self.__node_type_input in type_name:  # TODO: this can be cached
                    imgui.text(type_name)

            imguio = imgui.get_io()
            if imguio.keys_down[imgui.KEY_ENTER]:
                imgui.close_current_popup()
                for type_name in self.__node_types:
                    if self.__node_type_input in type_name:
                        self.__node_type_input = type_name
                        break
                else:
                    self.__node_type_input = ''
                if self.__node_type_input:
                    self.__scene.request_create_node(self.__node_type_input, 'bark foof', self.mapToScene(imguio.mouse_pos.x, imguio.mouse_pos.y))
                print('saoijjaoioijasfafs', self.__node_type_input)
            elif imguio.keys_down[imgui.KEY_ESCAPE]:
                imgui.close_current_popup()
                self.__node_type_input = ''
            imgui.end_popup()

        self.__create_menu_popup_toopen = False
        # pass all drawing comands to the rendering pipeline
        # and close frame context
        imgui.render()
        # imgui.end_frame()
        self.__imimpl.render(imgui.get_draw_data())
        painter.endNativePainting()

    def imguiProcessEvents(self, event: PySide2.QtGui.QInputEvent, do_recache=True):
        if not self.__imgui_init:
            return
        io = imgui.get_io()
        if isinstance(event, PySide2.QtGui.QMouseEvent):
            io.mouse_pos = event.windowPos().toTuple()
        elif isinstance(event, PySide2.QtGui.QWheelEvent):
            io.mouse_wheel = event.angleDelta().y() / 100
        elif isinstance(event, PySide2.QtGui.QKeyEvent):
            # print('pressed', event.key(), event.nativeScanCode(), event.nativeVirtualKey(), event.text(), imgui.KEY_A)
            if event.key() in imgui_key_map:
                if event.type() == QEvent.KeyPress:
                    io.keys_down[imgui_key_map[event.key()]] = True  # TODO: figure this out
                    #io.keys_down[event.key()] = True
                elif event.type() == QEvent.KeyRelease:
                    io.keys_down[imgui_key_map[event.key()]] = False
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

    def keyPressEvent(self, event: PySide2.QtGui.QKeyEvent):
        self.imguiProcessEvents(event)
        if imgui.get_io().want_capture_keyboard:
            event.accept()
        else:
            if event.key() == Qt.Key_Tab:
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
        self.__scene.stop()
        self.__scene.save_node_layout()
        super(NodeEditor, self).closeEvent(event)


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
