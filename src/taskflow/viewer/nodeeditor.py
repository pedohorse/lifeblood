import socket
import struct
import time
import json
import pickle
import sqlite3
import asyncio
from types import MappingProxyType
from math import sqrt
from ..uidata import UiData, NodeUi, Parameter, ParametersLayoutBase, OneLineParametersLayout, VerticalParametersLayout
from ..enums import TaskState, InvocationState
from ..broadcasting import await_broadcast
from ..nethelpers import recv_exactly, get_default_addr
from ..config import get_config
from .. import logging
from .. import paths
from ..net_classes import NodeTypeMetadata

from ..enums import NodeParameterType

from ..misc import generate_name

import PySide2.QtCore
import PySide2.QtGui
from PySide2.QtWidgets import *
from PySide2.QtCore import Qt, Slot, Signal, QThread, QRectF, QSizeF, QPointF, QAbstractAnimation, QSequentialAnimationGroup, QEvent
from PySide2.QtGui import QPen, QBrush, QColor, QPainterPath, QKeyEvent, QSurfaceFormat, QPainter, QTransform

from .dialogs import MessageWithSelectableText
from .code_editor.editor import StringParameterEditor
from .node_extra_items import ImplicitSplitVisualizer

import imgui
from imgui.integrations.opengl import ProgrammablePipelineRenderer

from typing import Optional, List, Tuple, Dict, Set, Callable, Iterable, Union

logger = logging.getLogger('viewer')


def call_later(callable, *args, **kwargs):
    if len(args) == 0 and len(kwargs) == 0:
        PySide2.QtCore.QTimer.singleShot(0, callable)
    else:
        PySide2.QtCore.QTimer.singleShot(0, lambda: callable(*args, **kwargs))


def length2(v: QPointF):
    return QPointF.dotProduct(v, v)


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

    def draw_imgui_elements(self, drawing_widget):
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
    base_height = 100
    base_width = 150
    # cache node type-2-inputs/outputs names, not to ask a million times for every node
    _node_inputs_outputs_cached: Dict[str, Tuple[List[str], List[str]]] = {}

    def __init__(self, id: int, type: str, name: str):
        super(Node, self).__init__(id)
        self.setFlags(QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemIsSelectable | QGraphicsItem.ItemSendsGeometryChanges)
        self.__height = self.base_height
        self.__width = self.base_width
        self.__input_radius = 8
        self.__line_width = 1
        self.__name = name
        self.__tasks: List["Task"] = []
        self.__node_type = type

        self.__ui_interactor = None
        self.__ui_widget: Optional[NodeEditor] = None
        self.__ui_grabbed_conn = None

        # prepare drawing tools
        self.__borderpen= QPen(QColor(96, 96, 96, 255))
        self.__borderpen_selected = QPen(QColor(144, 144, 144, 255))
        self.__caption_pen = QPen(QColor(192, 192, 192, 255))
        self.__typename_pen = QPen(QColor(128, 128, 128, 192))
        self.__borderpen.setWidthF(self.__line_width)
        self.__header_brush = QBrush(QColor(48, 64, 48, 192))
        self.__body_brush = QBrush(QColor(48, 48, 48, 128))

        self.__nodeui: Optional[NodeUi] = None
        self.__nodeui_menucache = {}
        self.__connections: Set[NodeConnection] = set()
        self.__expanded = False

        self.__inputs, self.__outputs = None, None
        self.__node_ui_for_io_requested = False
        if self.__node_type in Node._node_inputs_outputs_cached:
            self.__inputs, self.__outputs = Node._node_inputs_outputs_cached[self.__node_type]

        # children!
        self.__vismark = ImplicitSplitVisualizer(self)
        self.__vismark.setPos(QPointF(0, self._get_nodeshape().boundingRect().height() * 0.5))
        self.__vismark.setZValue(-2)

    def prepareGeometryChange(self):
        super(Node, self).prepareGeometryChange()
        for conn in self.__connections:
            conn.prepareGeometryChange()

    def node_type(self) -> str:
        return self.__node_type

    def node_name(self) -> str:
        return self.__name

    def set_name(self, new_name: str):
        if new_name == self.__name:
            return
        self.__name = new_name
        self.update()
        self.update_ui()

    def pause_all_tasks(self):
        scene: QGraphicsImguiScene = self.scene()
        scene.set_tasks_paused([x.get_id() for x in self.__tasks], True)

    def resume_all_tasks(self):
        scene: QGraphicsImguiScene = self.scene()
        scene.set_tasks_paused([x.get_id() for x in self.__tasks], False)

    def update_nodeui(self, nodeui: NodeUi):
        self.__nodeui = nodeui
        self.__nodeui_menucache = {}
        Node._node_inputs_outputs_cached[self.__node_type] = (list(nodeui.inputs_names()), list(nodeui.outputs_names()))
        self.__inputs, self.__outputs = Node._node_inputs_outputs_cached[self.__node_type]
        self.update_ui()

    def set_expanded(self, expanded: bool):
        if self.__expanded == expanded:
            return
        self.__expanded = expanded
        self.prepareGeometryChange()
        self.__height = self.base_height
        if expanded:
            self.__height += 225
            self.setPos(self.pos() + QPointF(0, 225*0.5))
        else:
            self.setPos(self.pos() - QPointF(0, 225 * 0.5))  # TODO: modify painterpath getters to avoid moving nodes on expand
        self.__vismark.setPos(QPointF(0, self._get_nodeshape().boundingRect().height() * 0.5))

        for i, task in enumerate(self.__tasks):
            task.set_node_animated(self, *self.get_task_pos(task, i))

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

    def input_connections(self, inname) -> Set["NodeConnection"]:
        if self.__inputs is not None and inname not in self.__inputs:
            raise RuntimeError(f'nodetype {self.__node_type} does not have input {inname}')
        return {x for x in self.__connections if x.input() == (self, inname)}

    def output_connections(self, outname) -> Set["NodeConnection"]:
        if self.__outputs is not None and outname not in self.__outputs:
            raise RuntimeError(f'nodetype {self.__node_type} does not have output {outname}')
        return {x for x in self.__connections if x.output() == (self, outname)}

    def input_names(self) -> Set[str]:
        return self.__inputs or set()

    def output_names(self) -> Set[str]:
        return self.__outputs or set()

    def boundingRect(self) -> QRectF:
        lw = self.__width + self.__line_width
        lh = self.__height + self.__line_width
        return QRectF(-0.5 * lw, -0.5 * lh - self.__input_radius, lw, lh + 2 * self.__input_radius)

    def _get_nodeshape(self):
        lw = self.__width + self.__line_width
        lh = self.__height + self.__line_width
        nodeshape = QPainterPath()
        nodeshape.addRoundedRect(QRectF(-0.5 * lw, -0.5 * lh, lw, lh), 5, 5)
        return nodeshape

    def _get_bodymask(self):
        lw = self.__width + self.__line_width
        lh = self.__height + self.__line_width
        bodymask = QPainterPath()
        bodymask.addRect(-0.5 * lw, -0.5 * lh + 32, lw, lh - 32)
        return bodymask

    def _get_headershape(self):
        return self._get_nodeshape() - self._get_bodymask()

    def _get_bodyshape(self):
        return self._get_nodeshape() & self._get_bodymask()

    def _get_expandbutton_shape(self):
        bodyshape = self._get_bodyshape()
        mask = QPainterPath()
        body_bound = bodyshape.boundingRect()
        corner = body_bound.bottomRight() + QPointF(15, 15)
        top = corner + QPointF(0, -60)
        left = corner + QPointF(-60, 0)
        mask.moveTo(corner)
        mask.lineTo(top)
        mask.lineTo(left)
        mask.lineTo(corner)
        return bodyshape & mask

    def paint(self, painter: PySide2.QtGui.QPainter, option: QStyleOptionGraphicsItem, widget: Optional[QWidget] = None) -> None:
        painter.pen().setWidthF(self.__line_width)
        nodeshape = self._get_nodeshape()

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

        headershape = self._get_headershape()
        bodyshape = self._get_bodyshape()

        if self.isSelected():
            painter.setPen(self.__borderpen_selected)
        else:
            painter.setPen(self.__borderpen)
        painter.fillPath(headershape, self.__header_brush)
        painter.fillPath(bodyshape, self.__body_brush)
        painter.fillPath(self._get_expandbutton_shape(), self.__header_brush)
        painter.drawPath(nodeshape)
        painter.setPen(self.__caption_pen)
        painter.drawText(headershape.boundingRect(), Qt.AlignHCenter | Qt.AlignTop, self.__name)
        painter.setPen(self.__typename_pen)
        painter.drawText(headershape.boundingRect(), Qt.AlignRight | Qt.AlignBottom, self.__node_type)

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
        logger.debug(f"adding task {task.get_id()} to node {self.get_id()}")
        pos_id = len(self.__tasks)
        if task.node() is None:
            task.set_node(self, *self.get_task_pos(task, pos_id))
        else:
            task.set_node_animated(self, *self.get_task_pos(task, pos_id))

        self.__tasks.append(task)
        task._Task__node = self

    def remove_task(self, task_to_remove: "Task"):
        logger.debug(f"removeing task {task_to_remove.get_id()} from node {self.get_id()}")
        task_pid = self.__tasks.index(task_to_remove)
        task_to_remove._Task__node = None
        for i in range(task_pid, len(self.__tasks) - 1):
            self.__tasks[i] = self.__tasks[i + 1]
            self.__tasks[i].set_node_animated(self, *self.get_task_pos(self.__tasks[i], i))
        self.__tasks = self.__tasks[:-1]

    def get_task_pos(self, task: "Task", pos_id: int) -> (QPointF, int):
        #assert task in self.__tasks
        rect = self._get_bodyshape().boundingRect()
        x, y = rect.topLeft().toTuple()
        w, h = rect.size().toTuple()
        d = task.draw_size()  # TODO: this assumes size is same, so dont make it an instance method
        r = d * 0.5

        #w *= 0.5
        x += r
        y += r
        h -= d
        w -= d
        x += (d * pos_id % w)
        y_shift = d * int(d * pos_id / w)
        y += (y_shift % h)
        return QPointF(x, y), int(y_shift / h)

    def task_state_changed(self, task):
        """
        here node might decide to highlight the task that changed state one way or another
        """
        if task.state() not in (TaskState.IN_PROGRESS, TaskState.GENERATING, TaskState.POST_GENERATING):
            return
        idx = self.__tasks.index(task)
        if idx == 0:
            return
        the_one = self.__tasks[idx]
        for i in reversed(range(1, idx+1)):
            self.__tasks[i] = self.__tasks[i-1]
            self.__tasks[i].set_node_animated(self, *self.get_task_pos(task, i))
        self.__tasks[0] = the_one
        self.__tasks[0].set_node_animated(self, *self.get_task_pos(task, 0))

    #
    # interface

    # helper
    def __draw_single_item(self, item, size=(1.0, 1.0), drawing_widget=None):
        if isinstance(item, Parameter):
            if not item.visible():
                return
            param_name = item.name()
            param_label = item.label() or ''
            parent_layout = item.parent()
            assert isinstance(parent_layout, ParametersLayoutBase)
            imgui.push_item_width(imgui.get_window_width() * parent_layout.relative_size_for_child(item)[0] * 2 / 3)
            try:
                if item.has_menu():
                    menu_order, menu_items = item.get_menu_items()

                    if param_name not in self.__nodeui_menucache:
                        self.__nodeui_menucache[param_name] = {'menu_items_inv': {v: k for k, v in menu_items.items()},
                                                               'menu_order_inv': {v: i for i, v in enumerate(menu_order)}}

                    menu_items_inv = self.__nodeui_menucache[param_name]['menu_items_inv']
                    menu_order_inv = self.__nodeui_menucache[param_name]['menu_order_inv']
                    if item.is_readonly():
                        imgui.text(menu_items_inv[item.value()])
                        return
                    else:
                        changed, val = imgui.combo('##'.join((param_label, param_name)), menu_order_inv[menu_items_inv[item.value()]], menu_order)
                        if changed:
                            item.set_value(menu_items[menu_order[val]])
                else:
                    if item.is_readonly():
                        imgui.text(f'{item.value()}')
                        return
                    param_type = item.type()
                    if param_type == NodeParameterType.BOOL:
                        changed, newval = imgui.checkbox('##'.join((param_label, param_name)), item.value())
                    elif param_type == NodeParameterType.INT:
                        #changed, newval = imgui.slider_int('##'.join((param_label, param_name)), item.value(), 0, 10)
                        slider_limits = item.display_value_limits()
                        if slider_limits[0] is not None:
                            changed, newval = imgui.slider_int('##'.join((param_label, param_name)), item.value(), *slider_limits)
                        else:
                            changed, newval = imgui.input_int('##'.join((param_label, param_name)), item.value())
                        if imgui.begin_popup_context_item(f'item context menu##{param_name}', 2):
                            imgui.selectable('toggle expression')
                            imgui.end_popup()
                    elif param_type == NodeParameterType.FLOAT:
                        #changed, newval = imgui.slider_float('##'.join((param_label, param_name)), item.value(), 0, 10)
                        slider_limits = item.display_value_limits()
                        if slider_limits[0] is not None:
                            changed, newval = imgui.input_float('##'.join((param_label, param_name)), item.value(), *slider_limits)
                        else:
                            changed, newval = imgui.input_float('##'.join((param_label, param_name)), item.value())
                    elif param_type == NodeParameterType.STRING:
                        if item.is_text_multiline():
                            # TODO: this below is a temporary solution. it only gives 8192 extra symbols for editing, but currently there is no proper way around with current pyimgui version
                            imgui.begin_group()
                            ed_butt_pressed = imgui.small_button(f'open in external window##{param_name}')
                            changed, newval = imgui.input_text_multiline('##'.join((param_label, param_name)), item.unexpanded_value(), len(item.unexpanded_value()) + 1024*8, flags=imgui.INPUT_TEXT_ALLOW_TAB_INPUT)
                            imgui.end_group()
                            if ed_butt_pressed:
                                hl = StringParameterEditor.SyntaxHighlight.NO_HIGHLIGHT
                                if item.syntax_hint() == 'python':
                                    hl = StringParameterEditor.SyntaxHighlight.PYTHON
                                wgt = StringParameterEditor(syntax_highlight=hl, parent=drawing_widget)
                                wgt.set_text(item.unexpanded_value())
                                wgt.edit_done.connect(lambda x, sc=self.scene(), id=self.get_id(), it=item: (item.set_value(x), sc.send_node_parameter_change(id, item)))  # TODO: this ugly multiexpr lambda freaks me out
                                wgt.show()
                        else:
                            changed, newval = imgui.input_text('##'.join((param_label, param_name)), item.unexpanded_value(), 256)
                    else:
                        raise NotImplementedError()
                    if changed:
                        item.set_value(newval)
            finally:
                imgui.pop_item_width()

            if changed:
                self.scene().send_node_parameter_change(self.get_id(), item)
                # see the logic is that whatever callbacks parameters may have - we rely that they will happen the same way
                # here and on scheduler side
                # so we just inform scheduler of the value change
                # anyway it's scheduler side that matters
        elif isinstance(item, OneLineParametersLayout):
            first_time = True
            for child in item.items(recursive=False):
                h, w = item.relative_size_for_child(child)
                if isinstance(child, Parameter):
                    if not child.visible():
                        continue
                if first_time:
                    first_time = False
                else:
                    imgui.same_line()
                self.__draw_single_item(child, (h*size[0], w*size[1]), drawing_widget=drawing_widget)
        elif isinstance(item, ParametersLayoutBase):
            for child in item.items(recursive=False):
                h, w = item.relative_size_for_child(child)
                if isinstance(child, Parameter):
                    if not child.visible():
                        continue
                self.__draw_single_item(child, (h*size[0], w*size[1]), drawing_widget=drawing_widget)
        else:
            raise NotImplementedError(f'unknown parameter hierarchy item to display {type(item)}')

    # main dude
    def draw_imgui_elements(self, drawing_widget):
        imgui.text(f'Node {self.get_id()}, type "{self.__node_type}", name {self.__name}')
        if imgui.collapsing_header(f'description##{self.__node_type}', None)[0]:
            imgui.text(self.scene().node_types().get(self.__node_type, 'error').description)
        if self.__nodeui is not None:
            self.__draw_single_item(self.__nodeui.main_parameter_layout(), drawing_widget=drawing_widget)

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
                    logger.debug('removing connections...')
                    assert connection.scene() is not None
                    connection.scene().removeItem(connection)
            assert len(self.__connections) == 0
        elif change == QGraphicsItem.ItemPositionChange:
            for connection in self.__connections:
                connection.prepareGeometryChange()

        return super(Node, self).itemChange(change, value)

    def mousePressEvent(self, event: QGraphicsSceneMouseEvent):
        if event.button() == Qt.LeftButton and self.__ui_interactor is None:
            pos = event.scenePos()
            r2 = (self.__input_radius + 0.5*self.__line_width)**2
            node_viewer = event.widget().parent()
            assert isinstance(node_viewer, NodeEditor)

            # check expand button
            expand_button_shape = self._get_expandbutton_shape()
            if expand_button_shape.contains(event.pos()):
                self.set_expanded(not self.__expanded)
                event.ignore()
                return

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

            if not self._get_nodeshape().contains(event.pos()):
                event.ignore()
                return

        super(Node, self).mousePressEvent(event)

        if event.button() == Qt.RightButton:
            # context menu time
            view = event.widget().parent()
            assert isinstance(view, NodeEditor)
            view.show_node_menu(self)
            event.accept()

    def mouseMoveEvent(self, event: QGraphicsSceneMouseEvent):
        # if self.__ui_interactor is not None:
        #     event.accept()
        #     self.__ui_interactor.mouseMoveEvent(event)
        #     return
        super(Node, self).mouseMoveEvent(event)

    def mouseReleaseEvent(self, event: QGraphicsSceneMouseEvent):
        # if self.__ui_interactor is not None:
        #     event.accept()
        #     self.__ui_interactor.mouseReleaseEvent(event)
        #     return
        super(Node, self).mouseReleaseEvent(event)

    @Slot(object)
    def _ui_interactor_finished(self, snap_point: Optional["NodeConnSnapPoint"]):
        assert self.__ui_interactor is not None
        call_later(lambda x: print('bloop', x) or x.scene().removeItem(x), self.__ui_interactor)
        if self.scene() is None:  # if scheduler deleted us while interacting
            return
        # NodeConnection._dbg_shitlist.append(self.__ui_interactor)
        grabbed_conn = self.__ui_grabbed_conn
        self.__ui_widget.release_ui_focus(self)
        self.__ui_widget = None
        self.__ui_interactor = None
        self.__ui_grabbed_conn = None

        # actual node reconection
        if snap_point is None:
            logger.debug('no change')
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
        self.setFlags(QGraphicsItem.ItemSendsGeometryChanges)  # QGraphicsItem.ItemIsSelectable |
        self.__nodeout = nodeout
        self.__nodein = nodein
        self.__outname = outname
        self.__inname = inname
        self.setZValue(-1)
        self.__line_width = 6  # TODO: rename it to match what it represents
        self.__pick_radius2 = 100**2
        self.__curv = 150

        self.__ui_interactor: Optional[NodeConnectionCreatePreview] = None
        self.__ui_widget: Optional[NodeEditor] = None
        self.__ui_last_pos = QPointF()
        self.__ui_grabbed_beginning: bool = True

        self.__pen = QPen(QColor(64, 64, 64, 192))
        self.__pen.setWidthF(3)
        self.__thick_pen = QPen(QColor(144, 144, 144, 128))
        self.__thick_pen.setWidthF(4)
        self.__last_drawn_path: Optional[QPainterPath] = None

    def distance_to_point(self, pos: QPointF):
        """
        returns approx distance to a given point
        currently it has the most crude implementation
        :param pos:
        :return:
        """

        line = self.get_painter_path()
        # determine where to start
        p0 = self.__nodeout.get_output_position(self.__outname)
        p1 = self.__nodein.get_input_position(self.__inname)

        if length2(p0-pos) < length2(p1-pos):  # pos closer to p0
            curper = 0
            curstep = 0.1
            lastsqlen = length2(p0 - pos)
        else:
            curper = 1
            curstep = -0.1
            lastsqlen = length2(p1 - pos)

        sqlen = lastsqlen
        while 0 <= curper <= 1:
            curper += curstep
            sqlen = length2(line.pointAtPercent(curper) - pos)
            if sqlen > lastsqlen:
                curstep *= -0.1
                if abs(sqlen - lastsqlen) < 0.001**2 or abs(curstep) < 1e-7:
                    break
            lastsqlen = sqlen

        return sqrt(sqlen)

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
        curv = self.__curv
        curv = min((p0-p1).manhattanLength()*0.5, curv)
        line.moveTo(p0)
        line.cubicTo(p0 + QPointF(0, curv), p1 - QPointF(0, curv), p1)
        if close_path:
            line.cubicTo(p1 - QPointF(0, curv), p0 + QPointF(0, curv), p0)
        return line

    def paint(self, painter: PySide2.QtGui.QPainter, option: QStyleOptionGraphicsItem, widget: Optional[QWidget] = None) -> None:
        if self.__ui_interactor is not None:  # if interactor exists - it does all the drawing
            return
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
        logger.debug(f'reassigning NodeConnection output to {node.get_id()}, {output_name}')
        assert node is not None
        self.prepareGeometryChange()
        if node != self.__nodeout:
            self.__nodeout.remove_connection(self)
            self.__nodeout = node
            self.__nodeout.add_connection(self)
        self.__outname = output_name

    def set_input(self, node: Node, input_name: str = 'main'):
        logger.debug(f'reassigning NodeConnection input to {node.get_id()}, {input_name}')
        assert node is not None
        self.prepareGeometryChange()
        if node != self.__nodein:
            self.__nodein.remove_connection(self)
            self.__nodein = node
            self.__nodein.add_connection(self)
        self.__inname = input_name

    def mousePressEvent(self, event: QGraphicsSceneMouseEvent):
        event.ignore()
        if event.button() != Qt.LeftButton:
            return
        line = self.get_painter_path(close_path=True)
        wire_pick_radius = 15
        circle = QPainterPath()
        circle.addEllipse(event.scenePos(), wire_pick_radius, wire_pick_radius)
        if self.__ui_interactor is None and line.intersects(circle):
            logger.debug('---GOT A PEAK AT MY DICK---')
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

            if hasattr(event, 'wire_candidates'):
                event.wire_candidates.append((self.distance_to_point(p), self))

    def post_mousePressEvent(self, event: QGraphicsSceneMouseEvent):
        """
        this will be called by scene as continuation of mousePressEvent
        IF scene decides so.
        :param event:
        :return:
        """
        wgt = event.widget()
        p = event.scenePos()
        p0 = self.__nodeout.get_output_position(self.__outname)
        p1 = self.__nodein.get_input_position(self.__inname)
        d02 = QPointF.dotProduct(p0 - p, p0 - p)
        d12 = QPointF.dotProduct(p1 - p, p1 - p)
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
                                                               snap_points, 15, self._ui_interactor_finished, True)
            self.update()
            self.__ui_widget = node_viewer
            self.scene().addItem(self.__ui_interactor)
            self.__ui_interactor.mousePressEvent(event)

    def mouseMoveEvent(self, event: QGraphicsSceneMouseEvent) -> None:
        # if self.__ui_interactor is not None:  # redirect input, cuz scene will direct all events to this item. would be better to change focus, but so far scene.setFocusItem did not work as expected
        #     self.__ui_interactor.mouseMoveEvent(event)
        #     event.accept()
        super(NodeConnection, self).mouseMoveEvent(event)

    def mouseReleaseEvent(self, event: QGraphicsSceneMouseEvent) -> None:
        # event.ignore()
        # if event.button() != Qt.LeftButton:
        #     return
        # if self.__ui_interactor is not None:  # redirect input, cuz scene will direct all events to this item. would be better to change focus, but so far scene.setFocusItem did not work as expected
        #     self.__ui_interactor.mouseReleaseEvent(event)
        #     event.accept()
        # self.ungrabMouse()
        print('ungrabbin')
        self.ungrabMouse()
        super(NodeConnection, self).mouseReleaseEvent(event)

    def keyPressEvent(self, event: QKeyEvent):
        if event.key() == Qt.Key_Delete:
            self.scene().request_node_connection_remove(self.get_id())
        event.accept()

    # _dbg_shitlist = []
    @Slot(object)
    def _ui_interactor_finished(self, snap_point: Optional["NodeConnSnapPoint"]):
        assert self.__ui_interactor is not None
        call_later(lambda x: print('bloop', x) or x.scene().removeItem(x), self.__ui_interactor)
        if self.scene() is None:  # if scheduler deleted us while interacting
            return
        # NodeConnection._dbg_shitlist.append(self.__ui_interactor)
        self.__ui_widget.release_ui_focus(self)
        self.__ui_widget = None
        is_cutting = self.__ui_interactor.is_cutting()
        self.__ui_interactor = None
        self.update()

        # are we cutting the wire
        if is_cutting:
            self.scene().request_node_connection_remove(self.get_id())
            return

        # actual node reconection
        if snap_point is None:
            logger.debug('no change')
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
    __brushes = None
    __borderpen = None
    __paused_pen = None

    def __init__(self, id, name: str, groups=None):
        super(Task, self).__init__(id)
        #self.setFlags(QGraphicsItem.ItemIsSelectable)
        self.setZValue(1)
        self.__name = name
        self.__state = TaskState.WAITING
        self.__paused = False
        self.__progress = None
        self.__layer = 0  # draw layer from 0 - main up to inf. kinda like LOD with highres being 0

        self.__state_details_raw = None
        self.__state_details = None
        self.__raw_data = {}

        self.__groups = set() if groups is None else set(groups)
        self.__log: dict = {}
        self.__ui_attributes: dict = {}
        self.__requested_invocs_while_selected = set()

        self.__size = 16
        self.__line_width = 1.5
        self.__node: Optional[Node] = None

        self.__ui_interactor = None
        self.__press_pos = None

        self.__animation_group: Optional[QSequentialAnimationGroup] = None
        self.__final_pos = None
        self.__final_layer = None

        self.__visible_layers_count = 3
        if self.__borderpen is None:
            Task.__borderpen = [QPen(QColor(96, 96, 96, 255), self.__line_width),
                                QPen(QColor(192, 192, 192, 255), self.__line_width)]
        if self.__brushes is None:
            # brushes and paused_pen are precalculated for several layers with different alphas, just not to calc them in paint
            def lerp(a, b, t):
                return a*(1.0-t) + b*t

            def lerpclr(c1, c2, t):
                color = c1
                color.setAlphaF(lerp(color.alphaF(), c2.alphaF(), t))
                color.setRedF(lerp(color.redF(), c2.redF(), t))
                color.setGreenF(lerp(color.greenF(), c2.redF(), t))
                color.setBlueF(lerp(color.blueF(), c2.redF(), t))
                return color

            Task.__brushes = {TaskState.WAITING: QBrush(QColor(64, 64, 64, 192)),
                              TaskState.GENERATING: QBrush(QColor(32, 128, 128, 192)),
                              TaskState.READY:  QBrush(QColor(32, 64, 32, 192)),
                              TaskState.IN_PROGRESS: QBrush(QColor(128, 128, 32, 192)),
                              TaskState.POST_WAITING: QBrush(QColor(96, 96, 96, 192)),
                              TaskState.POST_GENERATING: QBrush(QColor(128, 32, 128, 192)),
                              TaskState.DONE: QBrush(QColor(32, 192, 32, 192)),
                              TaskState.ERROR: QBrush(QColor(192, 32, 32, 192)),
                              TaskState.SPAWNED: QBrush(QColor(32, 32, 32, 192)),
                              TaskState.DEAD: QBrush(QColor(16, 19, 22, 192)),
                              TaskState.SPLITTED: QBrush(QColor(64, 32, 64, 192))}
            for k, v in Task.__brushes.items():
                ocolor = v.color()
                Task.__brushes[k] = []
                for i in range(self.__visible_layers_count):
                    color = lerpclr(ocolor, QColor.fromRgbF(0, 0, 0, 1), i*1.0/self.__visible_layers_count)
                    Task.__brushes[k].append(QColor(color))
        if self.__paused_pen is None:
            ocolor = QColor(64, 64, 128, 192)
            Task.__paused_pen = []
            for i in range(self.__visible_layers_count):
                color = lerpclr(ocolor, QColor.fromRgbF(0, 0, 0, 1), i*1.0/self.__visible_layers_count)
                Task.__paused_pen.append(QPen(color, self.__line_width*3))

    def boundingRect(self) -> QRectF:
        lw = self.__line_width
        return QRectF(QPointF(-0.5 * (self.__size + lw), -0.5 * (self.__size + lw)),
                      QSizeF(self.__size + lw, self.__size + lw))

    def _get_mainpath(self) -> QPainterPath:
        path = QPainterPath()
        path.addEllipse(-0.5 * self.__size, -0.5 * self.__size,
                        self.__size, self.__size)
        return path

    def _get_selectshapepath(self) -> QPainterPath:
        path = QPainterPath()
        lw = self.__line_width
        path.addEllipse(-0.5 * (self.__size + lw), -0.5 * (self.__size + lw),
                        self.__size + lw, self.__size + lw)
        return path

    def _get_pausedpath(self) -> QPainterPath:
        path = QPainterPath()
        lw = self.__line_width
        path.addEllipse(-0.5 * self.__size + 1.5*lw, -0.5 * self.__size + 1.5*lw,
                        self.__size - 3*lw, self.__size - 3*lw)
        return path

    def paint(self, painter: PySide2.QtGui.QPainter, option: QStyleOptionGraphicsItem, widget: Optional[QWidget] = None) -> None:
        if self.__layer >= self.__visible_layers_count:
            return
        path = self._get_mainpath()
        brush = self.__brushes[self.__state][self.__layer]
        painter.fillPath(path, brush)
        if self.__progress:
            arcpath = QPainterPath()
            arcpath.arcTo(QRectF(-0.5*self.__size, -0.5*self.__size, self.__size, self.__size),
                          90, -3.6*self.__progress)
            arcpath.closeSubpath()
            painter.fillPath(arcpath, self.__brushes[TaskState.DONE][self.__layer])
        if self.__paused:
            painter.setPen(self.__paused_pen[self.__layer])
            painter.drawPath(self._get_pausedpath())
        painter.setPen(self.__borderpen[int(self.isSelected())])
        painter.drawPath(path)

    def name(self):
        return self.__name

    def state(self):
        return self.__state

    def state_details(self) -> Optional[dict]:
        return self.__state_details

    def paused(self):
        return self.__paused

    def groups(self):
        return self.__groups

    def in_group(self, group_name):
        return group_name in self.__groups

    def node(self):
        return self.__node

    def draw_size(self):
        return self.__size

    def set_layer(self, layer: int):
        assert layer >= 0
        self.__layer = layer
        self.setZValue(1.0/(1.0 + layer))

    def set_state_details(self, state_details: Optional[str] = None):
        if self.__state_details_raw == state_details:
            return
        self.__state_details_raw = state_details
        if state_details is None:
            self.__state_details = None
            return
        self.__state_details = json.loads(self.__state_details_raw)

    def set_state(self, state: TaskState, paused: bool):
        if state == self.__state and self.__paused == paused:
            return
        self.__state = state
        self.set_state_details(None)
        self.__paused = paused
        if state != TaskState.IN_PROGRESS:
            self.__progress = None
        if self.__node:
            self.__node.task_state_changed(self)
        self.update()
        self.refresh_ui()

    def set_raw_data(self, raw_data: dict):
        self.__raw_data = raw_data

    def set_progress(self, progress: float):
        self.__progress = progress
        logger.debug('progress %d', progress)
        self.update()
        self.update_ui()

    def set_groups(self, groups: Iterable[str]):
        self.__groups = set(groups)

    def update_log(self, alllog: Dict[int, Dict[int, dict]]):
        """
        This function gets called by scene with new shit from worker. Maybe there's more sense to make it "_protected"
        :param alllog: is expected to be a dict of node_id -> (dict of invocation_id -> (invocation dict) )
        :return:
        """
        #self.__log = alllog
        logger.debug('log updated with %s', alllog)
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
        logger.debug('attrs updated with %s', attributes)
        self.__ui_attributes = attributes
        self.update_ui()

    def set_node(self, node: Optional[Node], pos: QPointF, layer: int):
        """
        """
        if self.__node and self.__node != node:
            self.__node.remove_task(self)
        if self.__animation_group is not None:
            self.__animation_group.stop()
            self.__animation_group = None
        self.__node = node
        self.setParentItem(self.__node)
        self.setPos(pos)
        self.set_layer(layer)
        self.refresh_ui()

    def set_node_animated(self, node: Optional[Node], pos: QPointF, layer: int):
        # first try to optimize, if we move on the same node to invisible layer - dont animate
        if node == self.__node and layer >= self.__visible_layers_count:
            return self.set_node(node, pos, layer)
        #
        dist = ((pos if node is None else node.mapToScene(pos)) - self.final_scene_position())
        ldist = sqrt(QPointF.dotProduct(dist, dist))
        new_animation = TaskAnimation(self, node, pos, duration=int(ldist / 0.5), parent=self.scene())
        self.set_layer(0)
        if self.__animation_group is None:
            self.__animation_group = QSequentialAnimationGroup(self.scene())
            self.__animation_group.finished.connect(self._clear_animation_group)
            self.setParentItem(None)
        self.__final_pos = pos
        self.__final_layer = layer
        self.__animation_group.addAnimation(new_animation)
        if self.__animation_group.state() != QAbstractAnimation.Running:
            self.__animation_group.start()
        if self.__node and self.__node != node:
            self.__node.remove_task(self)
        self.__node = node
        self.refresh_ui()

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
            ag, self.__animation_group = self.__animation_group, None
            ag.stop()  # just in case some recursion occures
            self.setParentItem(self.__node)
            self.setPos(self.__final_pos)
            self.set_layer(self.__final_layer)
            self.__final_pos = None
            self.__final_layer = None

    def setParentItem(self, item):
        """
        use set_node if you want to set node
        :param item:
        :return:
        """
        super(Task, self).setParentItem(item)

    def refresh_ui(self):
        """
        unlike update - this method actually queries new task ui status
        if task is not selected - does nothing
        :return:
        """
        if not self.isSelected():
            return
        self.scene().request_log_meta(self.get_id())  # update all task metadata: which nodes it ran on and invocation numbers only
        self.scene().request_attributes(self.get_id())

        for nid, invocs in self.__log.items():
            for invoc_id, invoc_dict in invocs.items():
                if invoc_dict is None:
                    continue
                if invoc_dict['state'] != InvocationState.FINISHED.value and invoc_id in self.__requested_invocs_while_selected:
                    self.__requested_invocs_while_selected.remove(invoc_id)

        # # if task is in progress - we find that invocation of it that is not finished and null it to force update
        # if self.__state == TaskState.IN_PROGRESS \
        #         and self.__node.get_id() in self.__log \
        #         and self.__log[self.__node.get_id()] is not None:
        #     for invoc_id, invoc in self.__log[self.__node.get_id()].items():
        #         if (invoc is None or
        #             invoc['state'] != InvocationState.FINISHED.value) \
        #                 and invoc_id in self.__requested_invocs_while_selected:
        #             self.__requested_invocs_while_selected.remove(invoc_id)

    def itemChange(self, change, value):
        if change == QGraphicsItem.ItemSelectedHasChanged:
            if value and self.__node is not None:   # item was just selected
                self.refresh_ui()
            elif not value:
                self.setFlag(QGraphicsItem.ItemIsSelectable, False)  # we are not selectable any more by band selection until directly clicked
                pass
                #self.__log = None
        elif change == QGraphicsItem.ItemSceneChange:
            if value is None:  # removing item from scene
                if self.__node is not None:
                    self.__node.remove_task(self)
        return super(Task, self).itemChange(change, value)  # TODO: maybe move this to scene's remove item?

    def mousePressEvent(self, event: QGraphicsSceneMouseEvent) -> None:
        if not self._get_selectshapepath().contains(event.pos()):
            event.ignore()
            return
        self.setFlag(QGraphicsItem.ItemIsSelectable, True)  # if we are clicked - we are now selectable until unselected. This is to avoid band selection
        super(Task, self).mousePressEvent(event)
        self.__press_pos = event.scenePos()

        if event.button() == Qt.RightButton:
            # context menu time
            view = event.widget().parent()
            assert isinstance(view, NodeEditor)
            view.show_task_menu(self)
        event.accept()

    def mouseMoveEvent(self, event: QGraphicsSceneMouseEvent) -> None:
        if self.__ui_interactor is None:
            movedist = event.scenePos() - self.__press_pos
            if QPointF.dotProduct(movedist, movedist) > 2500:  # TODO: config this rad squared
                self.__ui_interactor = TaskPreview(self)
                self.scene().addItem(self.__ui_interactor)
        if self.__ui_interactor:
            self.__ui_interactor.mouseMoveEvent(event)
        else:
            super(Task, self).mouseMoveEvent(event)

    def mouseReleaseEvent(self, event: QGraphicsSceneMouseEvent) -> None:
        if self.__ui_interactor:
            self.__ui_interactor.mouseReleaseEvent(event)
            nodes = [x for x in self.scene().items(event.scenePos(), Qt.IntersectsItemBoundingRect) if isinstance(x, Node)]
            if len(nodes) > 0:
                logger.debug(f'moving item {self} to node {nodes[0]}')
                self.scene().request_set_task_node(self.get_id(), nodes[0].get_id())
            call_later(self.__ui_interactor.scene().removeItem, self.__ui_interactor)
            self.__ui_interactor = None

        else:
            super(Task, self).mouseReleaseEvent(event)

    #
    # interface
    def draw_imgui_elements(self, drawing_widget):
        imgui.text(f'Task {self.get_id()} {self.__name}')
        imgui.text(f'groups: {", ".join(self.__groups)}')
        imgui.text(f'parent id: {self.__raw_data.get("parent_id", None)}')
        imgui.text(f'children count: {self.__raw_data.get("children_count", None)}')
        imgui.text(f'split level: {self.__raw_data.get("split_level", None)}')

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
                            logger.debug('clicked')
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
    def __init__(self, nodeout: Optional[Node], nodein: Optional[Node], outname: str, inname: str, snap_points: List[NodeConnSnapPoint], snap_radius: float, report_done_here: Callable, do_cutting: bool = False):
        super(NodeConnectionCreatePreview, self).__init__()
        assert nodeout is None and nodein is not None or \
               nodeout is not None and nodein is None
        self.setFlags(QGraphicsItem.ItemSendsGeometryChanges)
        self.setZValue(10)
        self.__nodeout = nodeout
        self.__nodein = nodein
        self.__outname = outname
        self.__inname = inname
        self.__snappoints = snap_points
        self.__snap_radius2 = snap_radius * snap_radius
        self.setZValue(-1)
        self.__line_width = 4
        self.__curv = 150
        self.__breakdist2 = 200**2

        self.__ui_last_pos = QPointF()
        self.__finished_callback = report_done_here

        self.__pen = QPen(QColor(64, 64, 64, 192))
        self.__pen.setWidthF(3)

        self.__do_cutting = do_cutting
        self.__cutpen = QPen(QColor(96, 32, 32, 192))
        self.__cutpen.setWidthF(3)
        self.__cutpen.setStyle(Qt.DotLine)

        self.__is_snapping = False

        self.__orig_pos: Optional[QPointF] = None

    def get_painter_path(self):
        if self.__nodein is not None:
            p0 = self.__ui_last_pos
            p1 = self.__nodein.get_input_position(self.__inname)
        else:
            p0 = self.__nodeout.get_output_position(self.__outname)
            p1 = self.__ui_last_pos

        curv = self.__curv
        curv = min((p0 - p1).manhattanLength() * 0.5, curv)

        line = QPainterPath()
        line.moveTo(p0)
        line.cubicTo(p0 + QPointF(0, curv), p1 - QPointF(0, curv), p1)
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
        if self.is_cutting():
            painter.setPen(self.__cutpen)
        else:
            painter.setPen(self.__pen)
        painter.drawPath(line)
        # painter.drawRect(self.boundingRect())

    def mousePressEvent(self, event: QGraphicsSceneMouseEvent):
        if event.button() != Qt.LeftButton:
            event.ignore()
            return
        self.grabMouse()
        pos = event.scenePos()
        closest_snap = self.get_closest_snappoint(pos)
        self.__is_snapping = False
        if closest_snap is not None:
            pos = closest_snap.pos()
            self.__is_snapping = True
        self.prepareGeometryChange()
        self.__ui_last_pos = pos
        if self.__orig_pos is None:
            self.__orig_pos = pos
        event.accept()

    def mouseMoveEvent(self, event):
        pos = event.scenePos()
        closest_snap = self.get_closest_snappoint(pos)
        self.__is_snapping = False
        if closest_snap is not None:
            pos = closest_snap.pos()
            self.__is_snapping = True
        self.prepareGeometryChange()
        self.__ui_last_pos = pos
        if self.__orig_pos is None:
            self.__orig_pos = pos
        event.accept()

    def is_cutting(self):
        """
        wether or not interactor is it cutting the wire state
        :return:
        """
        return self.__do_cutting and not self.__is_snapping and self.__orig_pos is not None and length2(self.__orig_pos - self.__ui_last_pos) > self.__breakdist2

    def get_closest_snappoint(self, pos: QPointF) -> Optional[NodeConnSnapPoint]:

        snappoints = [x for x in self.__snappoints if length2(x.pos() - pos) < self.__snap_radius2]

        if len(snappoints) == 0:
            return None

        return min(snappoints, key=lambda x: length2(x.pos() - pos))

    def mouseReleaseEvent(self, event: QGraphicsSceneMouseEvent):
        if event.button() != Qt.LeftButton:
            event.ignore()
            return
        if self.__finished_callback is not None:
            self.__finished_callback(self.get_closest_snappoint(event.scenePos()))
        event.accept()
        self.ungrabMouse()


class TaskPreview(QGraphicsItem):
    def __init__(self, task: Task):
        super(TaskPreview, self).__init__()
        self.setZValue(10)
        self.__size = 16
        self.__line_width = 1.5
        self.__finished_callback = None
        self.setZValue(10)

        self.__borderpen = QPen(QColor(192, 192, 192, 255), self.__line_width)
        self.__brush = QBrush(QColor(64, 64, 64, 128))

    def boundingRect(self) -> QRectF:
        lw = self.__line_width
        return QRectF(QPointF(-0.5 * (self.__size + lw), -0.5 * (self.__size + lw)),
                      QSizeF(self.__size + lw, self.__size + lw))

    def _get_mainpath(self) -> QPainterPath:
        path = QPainterPath()
        path.addEllipse(-0.5 * self.__size, -0.5 * self.__size,
                        self.__size, self.__size)
        return path

    def paint(self, painter: PySide2.QtGui.QPainter, option: QStyleOptionGraphicsItem, widget: Optional[QWidget] = None) -> None:
        path = self._get_mainpath()
        brush = self.__brush
        painter.fillPath(path, brush)
        painter.setPen(self.__borderpen)
        painter.drawPath(path)

    def mouseMoveEvent(self, event: QGraphicsSceneMouseEvent) -> None:
        self.setPos(event.scenePos())

    def mouseReleaseEvent(self, event: QGraphicsSceneMouseEvent):
        if self.__finished_callback is not None:
            self.__finished_callback(event.scenePos())  # not used for now not to overcomplicate
        event.accept()


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
    _signal_cancel_task = Signal(int)

    nodetypes_updated = Signal(dict)  # TODO: separate worker-oriented "private" signals for readability
    task_groups_updated = Signal(set)

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
        self.__ui_connection_worker.task_attribs_fetched.connect(self.task_attribs_fetched)
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
        self._signal_cancel_task.connect(self.__ui_connection_worker.cancel_task)
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
        self._signal_cancel_task.emit(task_id)

    def request_set_task_node(self, task_id: int, node_id:int):
        self._signal_set_task_node_requested.emit(task_id, node_id)

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
        logger.debug('full_update')

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
            logger.error('log fetched, but task not found!')
            return
        task.update_log(log)

    @Slot(object, object)
    def nodeui_fetched(self, node_id: int, nodeui: NodeUi):
        node = self.get_node(node_id)
        if node is None:
            logger.error('node ui fetched for non existant node')
            return
        node.update_nodeui(nodeui)

    @Slot(object, object)
    def task_attribs_fetched(self, task_id: int, attribs: dict):
        task = self.get_task(task_id)
        if task is None:
            logger.error('attribs fetched, but task not found!')
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
            sche_addr, sche_port = schedata['ui'].split(':')
            sche_port = int(sche_port)
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
        uidatabytes = recv_exactly(self.__conn, uidatasize)
        if len(uidatabytes) != uidatasize:
            logger.error('scheduler connection lost')
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
            if state in (TaskState.GENERATING, TaskState.IN_PROGRESS, TaskState.POST_GENERATING):
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
