import json

import itertools
from math import sqrt
from types import MappingProxyType
from datetime import timedelta
from .code_editor.editor import StringParameterEditor
from .node_extra_items import ImplicitSplitVisualizer

from lifeblood.config import get_config
from lifeblood.uidata import NodeUi, Parameter, ParameterExpressionError, ParametersLayoutBase, OneLineParametersLayout, CollapsableVerticalGroup, Separator
from lifeblood.ui_protocol_data import TaskData, TaskDelta, DataNotSet, IncompleteInvocationLogData, InvocationLogData
from lifeblood.basenode import BaseNode
from lifeblood.enums import TaskState, InvocationState
from lifeblood import logging
from lifeblood.environment_resolver import EnvironmentResolverArguments

from lifeblood.enums import NodeParameterType

import PySide2.QtGui
from PySide2.QtWidgets import *
from PySide2.QtCore import Qt, Slot, QRectF, QSizeF, QPointF, QAbstractAnimation, QSequentialAnimationGroup
from PySide2.QtGui import QPen, QBrush, QColor, QPainterPath, QPainterPathStroker, QKeyEvent, QLinearGradient, QDesktopServices

import imgui

from typing import TYPE_CHECKING, Optional, List, Tuple, Dict, Set, Callable, Iterable, Union

from . import nodeeditor
from .editor_scene_integration import fetch_and_open_log_viewer
if TYPE_CHECKING:
    from .graphics_scene import QGraphicsImguiScene

logger = logging.get_logger('viewer')


def call_later(callable, *args, **kwargs):  #TODO: this repeats here and in nodeeditor
    if len(args) == 0 and len(kwargs) == 0:
        PySide2.QtCore.QTimer.singleShot(0, callable)
    else:
        PySide2.QtCore.QTimer.singleShot(0, lambda: callable(*args, **kwargs))


def length2(v: QPointF):
    return QPointF.dotProduct(v, v)


class NetworkItem(QGraphicsItem):
    def __init__(self, id):
        super().__init__()
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
        self.__anim_type = 0 if self.__node1 is self.__node2 else 1

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
        if self.__anim_type == 0:  # linear
            pos = pos1 * (1 - t) + pos2 * t
        else:  # cubic
            curv = min((pos2-pos1).manhattanLength() * 2, 1000)  # 1000 is kinda derivative
            a = QPointF(0, curv) - (pos2-pos1)
            b = QPointF(0, -curv) + (pos2-pos1)
            pos = pos1*(1-t) + pos2*t + t*(1-t)*(a*(1-t) + b*t)
        self.__task.setPos(pos)


class Node(NetworkItemWithUI):
    base_height = 100
    base_width = 150
    # cache node type-2-inputs/outputs names, not to ask a million times for every node
    # actually this can be dynamic, and this cache is not used anyway, so TODO: get rid of it?
    _node_inputs_outputs_cached: Dict[str, Tuple[List[str], List[str]]] = {}

    # cache node shapes
    #_node_shapes

    class PseudoNode(BaseNode):
        def __init__(self, my_node: "Node"):
            super(Node.PseudoNode, self).__init__('_noname_')
            self.__my_node = my_node

        def _ui_changed(self, definition_changed=False):
            if definition_changed:
                self.__my_node.reanalyze_nodeui()

    def __init__(self, id: int, type: str, name: str):
        super(Node, self).__init__(id)
        self.setFlags(QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemIsSelectable | QGraphicsItem.ItemSendsGeometryChanges)
        self.setAcceptHoverEvents(True)
        self.__hoverover_pos: Optional[QPointF] = None
        self.__height = self.base_height
        self.__width = self.base_width
        self.__pivot_x = 0
        self.__pivot_y = 0

        self.__input_radius = 12
        self.__input_visible_radius = 8
        self.__line_width = 1
        self.__name = name
        self.__tasks: List["Task"] = []
        self.__node_type = type

        self.__ui_interactor = None
        self.__ui_widget: Optional[nodeeditor.NodeEditor] = None
        self.__ui_grabbed_conn = None

        self.__ui_selected_tab = 0
        self.__move_start_position = None
        self.__move_start_selection = None

        # prepare default drawing tools
        self.__borderpen= QPen(QColor(96, 96, 96, 255))
        self.__borderpen_selected = QPen(QColor(144, 144, 144, 255))
        self.__caption_pen = QPen(QColor(192, 192, 192, 255))
        self.__typename_pen = QPen(QColor(128, 128, 128, 192))
        self.__borderpen.setWidthF(self.__line_width)
        self.__header_brush = QBrush(QColor(48, 64, 48, 192))
        self.__body_brush = QBrush(QColor(48, 48, 48, 128))
        self.__connector_brush = QBrush(QColor(48, 48, 48, 192))
        self.__connector_brush_hovered = QBrush(QColor(96, 96, 96, 128))

        self.__nodeui: Optional[NodeUi] = None
        self.__nodeui_menucache = {}
        self.__connections: Set[NodeConnection] = set()
        self.__expanded = False

        self.__cached_bounds = None
        self.__cached_nodeshape = None
        self.__cached_bodymask = None
        self.__cached_headershape = None
        self.__cached_bodyshape = None
        self.__cached_expandbutton_shape = None

        self.__inputs, self.__outputs = None, None
        self.__node_ui_for_io_requested = False
        if self.__node_type in Node._node_inputs_outputs_cached:
            self.__inputs, self.__outputs = Node._node_inputs_outputs_cached[self.__node_type]

        # children!
        self.__vismark = ImplicitSplitVisualizer(self)
        self.__vismark.setPos(QPointF(0, self._get_nodeshape().boundingRect().height() * 0.5))
        self.__vismark.setZValue(-2)

        # misc
        self.__manual_url_base = get_config('viewer').get_option_noasync('manual_base_url', 'https://pedohorse.github.io/lifeblood')

    def get_session_id(self):
        """
        session id is local id that should be preserved within a session even after undo/redo operations,
        unlike simple id, that will change on undo/redo
        """
        return self.scene()._session_node_id_from_id(self.get_id())

    def prepareGeometryChange(self):
        super(Node, self).prepareGeometryChange()
        self.__cached_bounds = None
        self.__cached_nodeshape = None
        self.__cached_bodymask = None
        self.__cached_headershape = None
        self.__cached_bodyshape = None
        self.__cached_expandbutton_shape = None
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

    def set_selected(self, selected: bool, *, unselect_others=False):
        scene: QGraphicsImguiScene = self.scene()
        if unselect_others:
            scene.clearSelection()
        self.setSelected(selected)

    def apply_settings(self, settings_name: str):
        scene: QGraphicsImguiScene = self.scene()
        scene.request_apply_node_settings(self.get_id(), settings_name)

    def pause_all_tasks(self):
        scene: QGraphicsImguiScene = self.scene()
        scene.set_tasks_paused([x.get_id() for x in self.__tasks], True)

    def resume_all_tasks(self):
        scene: QGraphicsImguiScene = self.scene()
        scene.set_tasks_paused([x.get_id() for x in self.__tasks], False)

    def regenerate_all_ready_tasks(self):
        """
        all currently displayed tasks that are in states BEFORE processing, will be set to WAITING
        """
        self._change_all_task_states((TaskState.READY, TaskState.WAITING_BLOCKED), TaskState.WAITING)

    def retry_all_error_tasks(self):
        """
        all currently displayed task that are in ERROR state will be reset to WAITING
        """
        self._change_all_task_states((TaskState.ERROR,), TaskState.WAITING)

    def _change_all_task_states(self, from_states: Tuple[TaskState, ...], to_state: TaskState):
        scene: QGraphicsImguiScene = self.scene()
        scene.set_task_state(
            [x.get_id() for x in self.__tasks if x.state() in from_states],
            to_state
        )

    def update_nodeui(self, nodeui: NodeUi):
        self.__nodeui = nodeui
        self.__nodeui_menucache = {}
        self.__nodeui.attach_to_node(Node.PseudoNode(self))
        self.reanalyze_nodeui()

    def reanalyze_nodeui(self):
        self.prepareGeometryChange()  # not calling this seem to be able to break scene's internal index info on our connections
        # bug that appears - on first scene load deleting a node with more than 1 input/output leads to crash
        # on open nodes have 1 output, then they receive interface update and this func is called, and here's where bug may happen

        Node._node_inputs_outputs_cached[self.__node_type] = (list(self.__nodeui.inputs_names()), list(self.__nodeui.outputs_names()))
        self.__inputs, self.__outputs = Node._node_inputs_outputs_cached[self.__node_type]
        css = self.__nodeui.color_scheme()
        if css.secondary_color() is not None:
            gradient = QLinearGradient(-self.__width*0.1, 0, self.__width*0.1, 16)
            gradient.setColorAt(0.0, QColor(*(x * 255 for x in css.main_color()), 192))
            gradient.setColorAt(1.0, QColor(*(x * 255 for x in css.secondary_color()), 192))
            self.__header_brush = QBrush(gradient)
        else:
            self.__header_brush = QBrush(QColor(*(x * 255 for x in css.main_color()), 192))
        self.update()  # cuz input count affects visualization in the graph
        self.update_ui()

    def get_nodeui(self) -> Optional[NodeUi]:
        return self.__nodeui

    def set_expanded(self, expanded: bool):
        if self.__expanded == expanded:
            return
        self.__expanded = expanded
        self.prepareGeometryChange()
        self.__height = self.base_height
        if expanded:
            self.__height += 225
            self.__pivot_y -= 225/2
            # self.setPos(self.pos() + QPointF(0, 225*0.5))
        else:
            self.__pivot_y = 0
            # self.setPos(self.pos() - QPointF(0, 225 * 0.5))  # TODO: modify painterpath getters to avoid moving nodes on expand
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

    def input_nodes(self, inname: Optional[str] = None) -> Set["Node"]:
        """
        get input nodes as viewer (not scheduler) sees them

        :param inname: intput name to follow or None for all intputs
        """
        if inname is None:
            con_names = list(self.input_names())
        else:
            con_names = [inname]

        nodes = set()
        for con_name in con_names:
            nodes.update(con.output()[0] for con in self.input_connections(con_name))
        return nodes

    def output_nodes(self, outname: Optional[str] = None) -> Set["Node"]:
        """
        get output nodes as viewer (not scheduler) sees them

        :param outname: output name to follow or None for all outputs
        """
        if outname is None:
            con_names = list(self.output_names())
        else:
            con_names = [outname]

        nodes = set()
        for con_name in con_names:
            nodes.update(con.input()[0] for con in self.output_connections(con_name))
        return nodes

    def boundingRect(self) -> QRectF:
        if self.__cached_bounds is None:
            lw = self.__width + self.__line_width
            lh = self.__height + self.__line_width
            self.__cached_bounds = QRectF(
                -0.5 * lw - self.__pivot_x,
                -0.5 * lh - (max(self.__input_radius, self.__input_visible_radius) + 0.5 * self.__line_width) - self.__pivot_y,
                lw,
                lh + 2 * (max(self.__input_radius, self.__input_visible_radius) + 0.5 * self.__line_width))
        return self.__cached_bounds

    def _get_nodeshape(self):
        if self.__cached_nodeshape is None:
            lw = self.__width + self.__line_width
            lh = self.__height + self.__line_width
            nodeshape = QPainterPath()
            nodeshape.addRoundedRect(QRectF(-0.5 * lw - self.__pivot_x, -0.5 * lh - self.__pivot_y, lw, lh), 5, 5)
            self.__cached_nodeshape = nodeshape
        return self.__cached_nodeshape

    def _get_bodymask(self):
        if self.__cached_bodymask is None:
            lw = self.__width + self.__line_width
            lh = self.__height + self.__line_width
            bodymask = QPainterPath()
            bodymask.addRect(-0.5 * lw - self.__pivot_x, -0.5 * lh + 32 - self.__pivot_y, lw, lh - 32)
            self.__cached_bodymask = bodymask
        return self.__cached_bodymask

    def _get_headershape(self):
        if self.__cached_headershape is None:
            self.__cached_headershape = self._get_nodeshape() - self._get_bodymask()
        return self.__cached_headershape

    def _get_bodyshape(self):
        if self.__cached_bodyshape is None:
            self.__cached_bodyshape = self._get_nodeshape() & self._get_bodymask()
        return self.__cached_bodyshape

    def _get_expandbutton_shape(self):
        if self.__cached_expandbutton_shape is None:
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
            self.__cached_expandbutton_shape = bodyshape & mask
        return self.__cached_expandbutton_shape

    def paint(self, painter: PySide2.QtGui.QPainter, option: QStyleOptionGraphicsItem, widget: Optional[QWidget] = None) -> None:
        screen_rect = painter.worldTransform().mapRect(self.boundingRect())
        painter.pen().setWidthF(self.__line_width)
        nodeshape = self._get_nodeshape()

        if not self.__node_ui_for_io_requested:
            assert self.scene() is not None
            self.__node_ui_for_io_requested = True
            self.scene().request_node_ui(self.get_id())

        if screen_rect.width() > 40 and self.__inputs is not None and self.__outputs is not None:
            ninputs = len(self.__inputs)
            noutputs = len(self.__outputs)
            r2 = (self.__input_radius + 0.5*self.__line_width)**2
            for fi in range(ninputs + noutputs):
                path = QPainterPath()
                is_inputs = fi < ninputs
                i = fi if is_inputs else fi - ninputs
                input_point = QPointF(-0.5 * self.__width + (i + 1) * self.__width/((ninputs if is_inputs else noutputs) + 1) - self.__pivot_x,
                                      (-0.5 if is_inputs else 0.5) * self.__height - self.__pivot_y)
                path.addEllipse(input_point,
                                self.__input_visible_radius, self.__input_visible_radius)
                path -= nodeshape
                pen = self.__borderpen
                brush = self.__connector_brush
                if self.__hoverover_pos is not None:
                    if QPointF.dotProduct(input_point - self.__hoverover_pos, input_point - self.__hoverover_pos) <= r2:
                        pen = self.__borderpen_selected
                        brush = self.__connector_brush_hovered
                painter.setPen(pen)
                painter.fillPath(path, brush)
                painter.drawPath(path)

        headershape = self._get_headershape()
        bodyshape = self._get_bodyshape()

        if self.isSelected():
            if screen_rect.width() > 100:
                width_mult = 1
            elif screen_rect.width() > 50:
                width_mult = 4
            elif screen_rect.width() > 25:
                width_mult = 8
            else:
                width_mult = 16
            self.__borderpen_selected.setWidth(self.__line_width*width_mult)
            painter.setPen(self.__borderpen_selected)
        else:
            painter.setPen(self.__borderpen)
        painter.fillPath(headershape, self.__header_brush)
        painter.fillPath(bodyshape, self.__body_brush)
        expand_button_shape = self._get_expandbutton_shape()
        painter.fillPath(expand_button_shape, self.__header_brush)
        painter.drawPath(nodeshape)
        # draw highlighted elements on top
        if self.__hoverover_pos and expand_button_shape.contains(self.__hoverover_pos):
            painter.setPen(self.__borderpen_selected)
            painter.drawPath(expand_button_shape)

        # draw header/text last
        if screen_rect.width() > 50:
            painter.setPen(self.__caption_pen)
            painter.drawText(headershape.boundingRect(), Qt.AlignHCenter | Qt.AlignTop, self.__name)
            painter.setPen(self.__typename_pen)
            painter.drawText(headershape.boundingRect(), Qt.AlignRight | Qt.AlignBottom, self.__node_type)
            painter.drawText(headershape.boundingRect(), Qt.AlignLeft | Qt.AlignBottom, f'{len(self.__tasks)}')

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
        return self.mapToScene(-0.5 * self.__width + (idx + 1) * self.__width/(cnt + 1) - self.__pivot_x,
                               -0.5 * self.__height - self.__pivot_y)

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
        return self.mapToScene(-0.5 * self.__width + (idx + 1) * self.__width/(cnt + 1) - self.__pivot_x,
                               0.5 * self.__height - self.__pivot_y)

    def add_task(self, task: "Task", animated=True):
        if task in self.__tasks:
            return
        logger.debug(f"adding task {task.get_id()} to node {self.get_id()}")
        self.update()  # cuz node displays task number - we should redraw
        pos_id = len(self.__tasks)
        if task.node() is None or not animated:
            task.set_node(self, *self.get_task_pos(task, pos_id))
        else:
            task.set_node_animated(self, *self.get_task_pos(task, pos_id))

        insert_at = self._find_insert_index_for_task(task, prefer_back=True)

        self.__tasks.append(None)  # temporary placeholder, it'll be eliminated either in the loop, or after if task is last
        for i in reversed(range(insert_at + 1, len(self.__tasks))):
            self.__tasks[i] = self.__tasks[i-1]  # TODO: animated param should affect below!
            self.__tasks[i].set_node_animated(self, *self.get_task_pos(self.__tasks[i], i))
        self.__tasks[insert_at] = task
        self.__tasks[insert_at].set_node_animated(self, *self.get_task_pos(task, insert_at))
        task._Task__node = self

    def remove_tasks(self, tasks_to_remove: Iterable["Task"]):
        """
        this should cause much less animation overhead compared to
        if u would call remove-task for each task individually
        """
        logger.debug(f"removeing task {[x.get_id() for x in tasks_to_remove]} from node {self.get_id()}")
        tasks_to_remove = set(tasks_to_remove)
        for task in tasks_to_remove:
            task._Task__node = None
            #task.set_node(None)  # no, currently causes bad recursion

        if self.__tasks is tasks_to_remove:  # special case
            self.__tasks = []
        else:
            self.__tasks: List["Task"] = [None if x in tasks_to_remove else x for x in self.__tasks]
            off = 0
            for i, task in enumerate(self.__tasks):
                if task is None:
                    off += 1
                else:
                    self.__tasks[i - off] = self.__tasks[i]
                    self.__tasks[i - off].set_node_animated(self, *self.get_task_pos(self.__tasks[i - off], i - off))
            self.__tasks = self.__tasks[:-off]
            for x in tasks_to_remove:
                assert x not in self.__tasks
        self.update()  # cuz node displays task number - we should redraw

    def remove_task(self, task_to_remove: "Task"):
        logger.debug(f"removeing task {task_to_remove.get_id()} from node {self.get_id()}")
        task_pid = self.__tasks.index(task_to_remove)
        #task_to_remove.set_node(None)  # no, currently causes bad recursion
        task_to_remove._Task__node = None
        for i in range(task_pid, len(self.__tasks) - 1):
            self.__tasks[i] = self.__tasks[i + 1]
            self.__tasks[i].set_node_animated(self, *self.get_task_pos(self.__tasks[i], i))
        self.__tasks = self.__tasks[:-1]
        assert task_to_remove not in self.__tasks
        self.update()  # cuz node displays task number - we should redraw

    def tasks_iter(self):
        return (x for x in self.__tasks)

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

    def _find_insert_index_for_task(self, task, prefer_back=False):
        if task.state() == TaskState.IN_PROGRESS and not prefer_back:
            return 0

        if task.state() != TaskState.IN_PROGRESS and prefer_back:
            return len(self.__tasks)

        # now fun thing: we either have IN_PROGRESS and prefer_back, or NOT IN_PROGRESS and NOT prefer_back
        #  and both cases have the same logic for position finding
        for i, task in enumerate(self.__tasks):
            if task.state() != TaskState.IN_PROGRESS:
                return i
        else:
            return len(self.__tasks)

    def task_state_changed(self, task):
        """
        here node might decide to highlight the task that changed state one way or another
        """
        if task.state() not in (TaskState.IN_PROGRESS, TaskState.GENERATING, TaskState.POST_GENERATING):
            return

        # find a place
        append_at = self._find_insert_index_for_task(task)

        if append_at == len(self.__tasks):  # this is impossible case (in current impl of _find_insert_index_for_task) (cuz task is in __tasks, and it's not in IN_PROGRESS)
            return

        idx = self.__tasks.index(task)
        if idx <= append_at:  # already in place (and ignore moving further
            return

        # place where it has to be
        for i in reversed(range(append_at + 1, idx+1)):
            self.__tasks[i] = self.__tasks[i-1]
            self.__tasks[i].set_node_animated(self, *self.get_task_pos(self.__tasks[i], i))
        self.__tasks[append_at] = task
        self.__tasks[append_at].set_node_animated(self, *self.get_task_pos(task, append_at))

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
            idstr = f'_{self.get_id()}'
            assert isinstance(parent_layout, ParametersLayoutBase)
            imgui.push_item_width(imgui.get_window_width() * parent_layout.relative_size_for_child(item)[0] * 2 / 3)

            changed = False
            expr_changed = False

            new_item_val = None
            new_item_expression = None

            try:
                if item.has_expression():
                    with imgui.colored(imgui.COLOR_FRAME_BACKGROUND, 0.1, 0.4, 0.1):
                        expr_changed, newval = imgui.input_text('##'.join((param_label, param_name, idstr)), item.expression(), 256, flags=imgui.INPUT_TEXT_ENTER_RETURNS_TRUE)
                    if expr_changed:
                        new_item_expression = newval
                elif item.has_menu():
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
                        changed, val = imgui.combo('##'.join((param_label, param_name, idstr)), menu_order_inv[menu_items_inv[item.value()]], menu_order)
                        if changed:
                            new_item_val = menu_items[menu_order[val]]
                else:
                    if item.is_readonly():
                        imgui.text(f'{item.value()}')
                        return
                    param_type = item.type()
                    if param_type == NodeParameterType.BOOL:
                        changed, newval = imgui.checkbox('##'.join((param_label, param_name, idstr)), item.value())
                    elif param_type == NodeParameterType.INT:
                        #changed, newval = imgui.slider_int('##'.join((param_label, param_name, idstr)), item.value(), 0, 10)
                        slider_limits = item.display_value_limits()
                        if slider_limits[0] is not None:
                            changed, newval = imgui.slider_int('##'.join((param_label, param_name, idstr)), item.value(), *slider_limits)
                        else:
                            changed, newval = imgui.input_int('##'.join((param_label, param_name, idstr)), item.value(), flags=imgui.INPUT_TEXT_ENTER_RETURNS_TRUE)
                        if imgui.begin_popup_context_item(f'item context menu##{param_name}', 2):
                            imgui.selectable('toggle expression')
                            imgui.end_popup()
                    elif param_type == NodeParameterType.FLOAT:
                        #changed, newval = imgui.slider_float('##'.join((param_label, param_name, idstr)), item.value(), 0, 10)
                        slider_limits = item.display_value_limits()
                        if slider_limits[0] is not None and slider_limits[1] is not None:
                            changed, newval = imgui.slider_float('##'.join((param_label, param_name, idstr)), item.value(), *slider_limits)
                        else:
                            changed, newval = imgui.input_float('##'.join((param_label, param_name, idstr)), item.value(), flags=imgui.INPUT_TEXT_ENTER_RETURNS_TRUE)
                    elif param_type == NodeParameterType.STRING:
                        if item.is_text_multiline():
                            # TODO: this below is a temporary solution. it only gives 8192 extra symbols for editing, but currently there is no proper way around with current pyimgui version
                            imgui.begin_group()
                            ed_butt_pressed = imgui.small_button(f'open in external window##{param_name}')
                            changed, newval = imgui.input_text_multiline('##'.join((param_label, param_name, idstr)), item.unexpanded_value(), len(item.unexpanded_value()) + 1024*8, flags=imgui.INPUT_TEXT_ALLOW_TAB_INPUT | imgui.INPUT_TEXT_ENTER_RETURNS_TRUE | imgui.INPUT_TEXT_CTRL_ENTER_FOR_NEW_LINE)
                            imgui.end_group()
                            if ed_butt_pressed:
                                hl = StringParameterEditor.SyntaxHighlight.NO_HIGHLIGHT
                                if item.syntax_hint() == 'python':
                                    hl = StringParameterEditor.SyntaxHighlight.PYTHON
                                wgt = StringParameterEditor(syntax_highlight=hl, parent=drawing_widget)
                                wgt.setAttribute(Qt.WA_DeleteOnClose, True)
                                wgt.set_text(item.unexpanded_value())
                                wgt.edit_done.connect(lambda x, sc=self.scene(), id=self.get_id(), it=item: sc.change_node_parameter(id, item, x))
                                wgt.set_title(f'editing parameter "{param_name}"')
                                wgt.show()
                        else:
                            changed, newval = imgui.input_text('##'.join((param_label, param_name, idstr)), item.unexpanded_value(), 256, flags=imgui.INPUT_TEXT_ENTER_RETURNS_TRUE)
                    else:
                        raise NotImplementedError()
                    if changed:
                        new_item_val = newval

                # item context menu popup
                popupid = '##'.join((param_label, param_name, idstr))  # just to make sure no names will collide with full param imgui lables
                if imgui.begin_popup_context_item(f'Item Context Menu##{popupid}', 2):
                    if item.can_have_expressions() and not item.has_expression():
                        if imgui.selectable(f'enable expression##{popupid}')[0]:
                            expr_changed = True
                            # try to turn backtick expressions into normal one
                            new_item_expression = item.python_from_expandable_string(item.unexpanded_value())
                    if item.has_expression():
                        if imgui.selectable(f'delete expression##{popupid}')[0]:
                            try:
                                value = item.value()
                            except ParameterExpressionError as e:
                                value = item.default_value()
                            expr_changed = True
                            changed = True
                            new_item_val = value
                            new_item_expression = None
                    imgui.end_popup()
            finally:
                imgui.pop_item_width()

            if changed or expr_changed:
                scene: QGraphicsImguiScene = self.scene()
                scene.change_node_parameter(self.get_id(), item,
                                            new_item_val if changed else ...,
                                            new_item_expression if expr_changed else ...)

        elif isinstance(item, Separator):
            imgui.separator()
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
        elif isinstance(item, CollapsableVerticalGroup):
            expanded, _ = imgui.collapsing_header(f'{item.label()}##{item.name()}')
            if expanded:
                for child in item.items(recursive=False):
                    h, w = item.relative_size_for_child(child)
                    imgui.indent(5)
                    self.__draw_single_item(child, (h*size[0], w*size[1]), drawing_widget=drawing_widget)
                    imgui.unindent(5)
                imgui.separator()
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

        if imgui.selectable(f'parameters##{self.__name}', self.__ui_selected_tab == 0, width=imgui.get_window_width() * 0.5 * 0.7)[1]:
            self.__ui_selected_tab = 0
        imgui.same_line()
        if imgui.selectable(f'description##{self.__name}', self.__ui_selected_tab == 1, width=imgui.get_window_width() * 0.5 * 0.7)[1]:
            self.__ui_selected_tab = 1
        imgui.separator()

        if self.__ui_selected_tab == 0:
            if self.__nodeui is not None:
                self.__draw_single_item(self.__nodeui.main_parameter_layout(), drawing_widget=drawing_widget)
        elif self.__ui_selected_tab == 1:
            if self.__node_type in self.scene().node_types() and imgui.button('open manual page'):
                plugin_info = self.scene().node_types()[self.__node_type].plugin_info
                category = plugin_info.category
                package = plugin_info.package_name
                QDesktopServices.openUrl(self.__manual_url_base + f'/nodes/{category}{f"/{package}" if package else ""}/{self.__node_type}.html')
            imgui.text(self.scene().node_types()[self.__node_type].description if self.__node_type in self.scene().node_types() else 'error')

    def add_connection(self, new_connection: "NodeConnection"):
        self.__connections.add(new_connection)

        # if node ui has not yet been updated - we temporary add in/out names to lists
        # it will get overriden by nodeui update
        conno = new_connection.output()
        if conno[0] == self and (self.__outputs is None or conno[1] not in self.__outputs):
            if self.__outputs is None:
                self.__outputs = []
            self.__outputs.append(conno[1])
        conni = new_connection.input()
        if conni[0] == self and (self.__inputs is None or conni[1] not in self.__inputs):
            if self.__inputs is None:
                self.__inputs = []
            self.__inputs.append(conni[1])

    def remove_connection(self, connection: "NodeConnection"):
        self.__connections.remove(connection)

    def itemChange(self, change, value):
        if change == QGraphicsItem.ItemSelectedHasChanged:
            if value and self.scene().get_inspected_item() == self:   # item was just selected, And is the first selected
                self.scene()._node_selected(self)
        elif change == QGraphicsItem.ItemSceneChange:  # just before scene change
            conns = self.__connections.copy()
            if len(self.__tasks):
                logger.warning(f'node {self.get_id()}({self.node_name()}) has tasks at the moment of deletion, orphaning the tasks')
                self.remove_tasks(self.__tasks)
            for connection in conns:
                if self.scene() is not None and value != self.scene():
                    logger.debug('removing connections...')
                    assert connection.scene() is not None
                    connection.scene().removeItem(connection)
            assert len(self.__connections) == 0
        elif change == QGraphicsItem.ItemPositionChange:
            if self.__move_start_position is None:
                self.__move_start_position = self.pos()
            for connection in self.__connections:
                connection.prepareGeometryChange()

        return super(Node, self).itemChange(change, value)

    def mousePressEvent(self, event: QGraphicsSceneMouseEvent):
        if event.button() == Qt.LeftButton and self.__ui_interactor is None:
            pos = event.scenePos()
            r2 = (self.__input_radius + 0.5*self.__line_width)**2
            node_viewer = event.widget().parent()
            assert isinstance(node_viewer, nodeeditor.NodeEditor)

            # check expand button
            expand_button_shape = self._get_expandbutton_shape()
            if expand_button_shape.contains(event.pos()):
                self.set_expanded(not self.__expanded)
                event.ignore()
                return

            if self.__inputs:  # may be None if first nodes update hasn't arrived before mouse event
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

            if self.__outputs:
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
        self.__move_start_selection = {self}
        self.__move_start_position = None

        # check for special picking: shift+move should move all upper connected nodes
        if event.modifiers() & Qt.ShiftModifier or event.modifiers() & Qt.ControlModifier:
            selecting_inputs = event.modifiers() & Qt.ShiftModifier
            selecting_outputs = event.modifiers() & Qt.ControlModifier
            extra_selected_nodes = set()
            if selecting_inputs:
                extra_selected_nodes.update(self.input_nodes())
            if selecting_outputs:
                extra_selected_nodes.update(self.output_nodes())

            extra_selected_nodes_ordered = list(extra_selected_nodes)
            for relnode in extra_selected_nodes_ordered:
                relnode.setSelected(True)
                relrelnodes = set()
                if selecting_inputs:
                    relrelnodes.update(node for node in relnode.input_nodes() if node not in extra_selected_nodes)
                if selecting_outputs:
                    relrelnodes.update(node for node in relnode.output_nodes() if node not in extra_selected_nodes)
                extra_selected_nodes_ordered.extend(relrelnodes)
                extra_selected_nodes.update(relrelnodes)
            self.setSelected(True)
        for item in self.scene().selectedItems():
            if isinstance(item, Node):
                self.__move_start_selection.add(item)
                item.__move_start_position = None

        if event.button() == Qt.RightButton:
            # context menu time
            view = event.widget().parent()
            assert isinstance(view, nodeeditor.NodeEditor)
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
        if self.__move_start_position is not None:
            if self.scene().node_snapping_enabled():
                for node in self.__move_start_selection:
                    pos = node.pos()
                    snapx = node.base_width / 4
                    snapy = node.base_height / 4
                    node.setPos(round(pos.x() / snapx) * snapx,
                                round(pos.y() / snapy) * snapy)
            self.scene()._nodes_were_moved([(node, node.__move_start_position) for node in self.__move_start_selection])
            for node in self.__move_start_selection:
                node.__move_start_position = None

    def hoverMoveEvent(self, event):
        self.__hoverover_pos = event.pos()

    def hoverLeaveEvent(self, event):
        self.__hoverover_pos = None
        self.update()

    @Slot(object)
    def _ui_interactor_finished(self, snap_point: Optional["NodeConnSnapPoint"]):
        assert self.__ui_interactor is not None
        call_later(lambda x: logger.debug(f'later removing {x}') or x.scene().removeItem(x), self.__ui_interactor)
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
        scene.add_connection(snap_point.node().get_id() if setting_out else self.get_id(),
                             snap_point.connection_name() if setting_out else grabbed_conn,
                             snap_point.node().get_id() if not setting_out else self.get_id(),
                             snap_point.connection_name() if not setting_out else grabbed_conn)


class NodeConnection(NetworkItem):
    def __init__(self, id: int, nodeout: Node, nodein: Node, outname: str, inname: str):
        super(NodeConnection, self).__init__(id)
        self.setFlags(QGraphicsItem.ItemSendsGeometryChanges)  # QGraphicsItem.ItemIsSelectable |
        self.setAcceptHoverEvents(True)  # for highlights
        self.__nodeout = nodeout
        self.__nodein = nodein
        self.__outname = outname
        self.__inname = inname
        self.setZValue(-1)
        self.__line_width = 6  # TODO: rename it to match what it represents
        self.__wire_pick_radius = 15
        self.__pick_radius2 = 100**2
        self.__curv = 150
        self.__wire_highlight_radius = 5

        self.__temporary_invalid = False

        self.__ui_interactor: Optional[NodeConnectionCreatePreview] = None
        self.__ui_widget: Optional[nodeeditor.NodeEditor] = None
        self.__ui_last_pos = QPointF()
        self.__ui_grabbed_beginning: bool = True

        self.__pen = QPen(QColor(64, 64, 64, 192))
        self.__pen.setWidthF(3)
        self.__pen_highlight = QPen(QColor(92, 92, 92, 192))
        self.__pen_highlight.setWidthF(3)
        self.__thick_pen = QPen(QColor(144, 144, 144, 128))
        self.__thick_pen.setWidthF(4)
        self.__last_drawn_path: Optional[QPainterPath] = None

        self.__stroker = QPainterPathStroker()
        self.__stroker.setWidth(2*self.__wire_pick_radius)

        self.__hoverover_pos = None

        nodein.add_connection(self)
        nodeout.add_connection(self)

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
        if self.__outname not in self.__nodeout.output_names() or self.__inname not in self.__nodein.input_names():
            self.__temporary_invalid = True
            return QRectF()
        self.__temporary_invalid = False
        hlw = self.__line_width
        line = self.get_painter_path()
        return line.boundingRect().adjusted(-hlw - self.__wire_pick_radius, -hlw, hlw + self.__wire_pick_radius, hlw)
        # inputpos = self.__nodeout.get_output_position(self.__outname)
        # outputpos = self.__nodein.get_input_position(self.__inname)
        # return QRectF(QPointF(min(inputpos.x(), outputpos.x()) - hlw, min(inputpos.y(), outputpos.y()) - hlw),
        #               QPointF(max(inputpos.x(), outputpos.x()) + hlw, max(inputpos.y(), outputpos.y()) + hlw))

    def shape(self):
        # this one is mainly needed for proper selection and item picking
        return self.__stroker.createStroke(self.get_painter_path())

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
        if self.__temporary_invalid:
            return
        if self.__ui_interactor is not None:  # if interactor exists - it does all the drawing
            return
        line = self.get_painter_path()

        painter.setPen(self.__pen)

        if self.__hoverover_pos is not None:
            hldiag = QPointF(self.__wire_highlight_radius, self.__wire_highlight_radius)
            if line.intersects(QRectF(self.__hoverover_pos - hldiag, self.__hoverover_pos + hldiag)):
                painter.setPen(self.__pen_highlight)

        if self.isSelected():
            painter.setPen(self.__thick_pen)

        painter.drawPath(line)
        # painter.drawRect(self.boundingRect())
        self.__last_drawn_path = line

    def output(self) -> Tuple[Node, str]:
        return self.__nodeout, self.__outname

    def input(self) -> Tuple[Node, str]:
        return self.__nodein, self.__inname

    def set_output(self, node: Node, output_name: str = 'main'):
        logger.debug(f'reassigning NodeConnection output to {node.get_id()}, {output_name}')
        assert node is not None
        self.prepareGeometryChange()
        if node != self.__nodeout:
            self.__nodeout.remove_connection(self)
            self.__nodeout = node
            self.__outname = output_name
            self.__nodeout.add_connection(self)
        else:
            self.__outname = output_name

    def set_input(self, node: Node, input_name: str = 'main'):
        logger.debug(f'reassigning NodeConnection input to {node.get_id()}, {input_name}')
        assert node is not None
        self.prepareGeometryChange()
        if node != self.__nodein:
            self.__nodein.remove_connection(self)
            self.__nodein = node
            self.__inname = input_name
            self.__nodein.add_connection(self)
        else:
            self.__inname = input_name

    def hoverMoveEvent(self, event):
        self.__hoverover_pos = event.pos()

    def hoverLeaveEvent(self, event):
        self.__hoverover_pos = None
        self.update()

    def mousePressEvent(self, event: QGraphicsSceneMouseEvent):
        event.ignore()
        if event.button() != Qt.LeftButton:
            return
        line = self.get_painter_path(close_path=True)
        circle = QPainterPath()
        circle.addEllipse(event.scenePos(), self.__wire_pick_radius, self.__wire_pick_radius)
        if self.__ui_interactor is None and line.intersects(circle):
            logger.debug('wire candidate for picking detected')
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

            # this way we report to scene event handler that we are candidates for picking
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
        assert isinstance(node_viewer, nodeeditor.NodeEditor)
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
        logger.debug('ungrabbing mouse')
        self.ungrabMouse()
        super(NodeConnection, self).mouseReleaseEvent(event)

    # _dbg_shitlist = []
    @Slot(object)
    def _ui_interactor_finished(self, snap_point: Optional["NodeConnSnapPoint"]):
        assert self.__ui_interactor is not None
        call_later(lambda x: logger.debug(f'later removing {x}') or x.scene().removeItem(x), self.__ui_interactor)
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
            self.scene().cut_connection_by_id(self.get_id())
            return

        # actual node reconection
        if snap_point is None:
            logger.debug('no change')
            return
        scene: QGraphicsImguiScene = self.scene()
        changing_out = not snap_point.connection_is_input()
        scene.change_connection_by_id(self.get_id(),
                                      to_outnode_id=snap_point.node().get_id() if changing_out else None,
                                      to_outname=snap_point.connection_name() if changing_out else None,
                                      to_innode_id=None if changing_out else snap_point.node().get_id(),
                                      to_inname=None if changing_out else snap_point.connection_name())
        # scene.request_node_connection_change(self.get_id(),
        #                                      snap_point.node().get_id() if changing_out else None,
        #                                      snap_point.connection_name() if changing_out else None,
        #                                      None if changing_out else snap_point.node().get_id(),
        #                                      None if changing_out else snap_point.connection_name())

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

    def __init__(self, task_data: TaskData):
        super(Task, self).__init__(task_data.id)
        self.setAcceptHoverEvents(True)
        self.__hoverover_pos = None
        #self.setFlags(QGraphicsItem.ItemIsSelectable)
        self.setZValue(1)
        # self.__name = name
        # self.__state = TaskState.WAITING
        # self.__paused = False
        # self.__progress = None
        self.__layer = 0  # draw layer from 0 - main up to inf. kinda like LOD with highres being 0

        # self.__state_details_raw = None
        self.__state_details_cached = None
        self.__raw_data: TaskData = task_data

        # self.__groups = set() if groups is None else set(groups)
        self.__log: Dict[int, Dict[int, Union[IncompleteInvocationLogData, InvocationLogData]]] = {}
        self.__inv_log: Optional[List[Tuple[int, int, Union[IncompleteInvocationLogData, InvocationLogData]]]] = None  # for presentation - inv_id -> (node_id, log)
        self.__ui_attributes: dict = {}
        self.__ui_env_res_attributes: Optional[EnvironmentResolverArguments] = None
        self.__requested_invocs_while_selected = set()

        self.__size = 16
        self.__line_width = 1.5
        self.__node: Optional[Node] = None

        self.__ui_interactor = None
        self.__press_pos = None

        self.__animation_group: Optional[QSequentialAnimationGroup] = None
        self.__final_pos = None
        self.__final_layer = None

        self.__visible_layers_count = 2

        self.__mainshape_cache = None  # NOTE: DYNAMIC SIZE OR LINE WIDTH ARE NOT SUPPORTED HERE!
        self.__selshape_cache = None
        self.__pausedshape_cache = None
        self.__bound_cache = None

        if self.__borderpen is None:
            Task.__borderpen = [QPen(QColor(96, 96, 96, 255), self.__line_width),
                                QPen(QColor(128, 128, 128, 255), self.__line_width),
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
                              TaskState.INVOKING: QBrush(QColor(108, 108, 12, 192)),
                              TaskState.IN_PROGRESS: QBrush(QColor(128, 128, 32, 192)),
                              TaskState.POST_WAITING: QBrush(QColor(96, 96, 96, 192)),
                              TaskState.POST_GENERATING: QBrush(QColor(128, 32, 128, 192)),
                              TaskState.DONE: QBrush(QColor(32, 192, 32, 192)),
                              TaskState.ERROR: QBrush(QColor(192, 32, 32, 192)),
                              TaskState.SPAWNED: QBrush(QColor(32, 32, 32, 192)),
                              TaskState.DEAD: QBrush(QColor(16, 19, 22, 192)),
                              TaskState.SPLITTED: QBrush(QColor(64, 32, 64, 192)),
                              TaskState.WAITING_BLOCKED: QBrush(QColor(40, 40, 50, 192)),
                              TaskState.POST_WAITING_BLOCKED: QBrush(QColor(40, 40, 60, 192))}
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
        if self.__bound_cache is None:
            lw = self.__line_width
            self.__bound_cache = QRectF(QPointF(-0.5 * (self.__size + lw), -0.5 * (self.__size + lw)),
                                        QSizeF(self.__size + lw, self.__size + lw))
        return self.__bound_cache

    def _get_mainpath(self) -> QPainterPath:
        if self.__mainshape_cache is None:
            path = QPainterPath()
            path.addEllipse(-0.5 * self.__size, -0.5 * self.__size,
                            self.__size, self.__size)
            self.__mainshape_cache = path
        return self.__mainshape_cache

    def _get_selectshapepath(self) -> QPainterPath:
        if self.__selshape_cache is None:
            path = QPainterPath()
            lw = self.__line_width
            path.addEllipse(-0.5 * (self.__size + lw), -0.5 * (self.__size + lw),
                            self.__size + lw, self.__size + lw)
            self.__selshape_cache = path
        return self.__selshape_cache

    def _get_pausedpath(self) -> QPainterPath:
        if self.__pausedshape_cache is None:
            path = QPainterPath()
            lw = self.__line_width
            path.addEllipse(-0.5 * self.__size + 1.5*lw, -0.5 * self.__size + 1.5*lw,
                            self.__size - 3*lw, self.__size - 3*lw)
            self.__pausedshape_cache = path
        return self.__pausedshape_cache

    def paint(self, painter: PySide2.QtGui.QPainter, option: QStyleOptionGraphicsItem, widget: Optional[QWidget] = None) -> None:
        if self.__layer >= self.__visible_layers_count:
            return
        if self.__node is None:  # probably temporary state due to asyncronous incoming events from scheduler
            return  # or we can draw them somehow else?
        screen_rect = painter.worldTransform().mapRect(self.boundingRect())

        path = self._get_mainpath()
        brush = self.__brushes[self.state()][self.__layer]
        painter.fillPath(path, brush)
        if self.__raw_data.progress:
            arcpath = QPainterPath()
            arcpath.arcTo(QRectF(-0.5*self.__size, -0.5*self.__size, self.__size, self.__size),
                          90, -3.6*self.__raw_data.progress)
            arcpath.closeSubpath()
            painter.fillPath(arcpath, self.__brushes[TaskState.DONE][self.__layer])
        if self.paused():
            painter.setPen(self.__paused_pen[self.__layer])
            painter.drawPath(self._get_pausedpath())

        if screen_rect.width() > 7:
            if self.isSelected():
                painter.setPen(self.__borderpen[2])
            elif self.__hoverover_pos is not None:
                painter.setPen(self.__borderpen[1])
            else:
                painter.setPen(self.__borderpen[0])
            painter.drawPath(path)

    def set_selected(self, selected: bool):
        scene: QGraphicsImguiScene = self.scene()
        scene.clearSelection()
        if selected:
            self.setFlag(QGraphicsItem.ItemIsSelectable, True)
        self.setSelected(selected)

    def name(self):
        return self.__raw_data.name

    def set_name(self, name: str):
        if name == self.__raw_data.name:
            return
        self.__raw_data.name = name
        self.refresh_ui()

    def state(self) -> TaskState:
        return self.__raw_data.state

    def state_details(self) -> Optional[dict]:
        if self.__state_details_cached is None and self.__raw_data.state_details is not None:
            self.__state_details_cached = json.loads(self.__raw_data.state_details)
        return self.__state_details_cached

    def paused(self):
        return self.__raw_data.paused

    def groups(self) -> Set[str]:
        return self.__raw_data.groups

    def set_groups(self, groups: Set[str]):
        if self.__raw_data.groups == groups:
            return
        self.__raw_data.groups = groups
        self.refresh_ui()

    def attributes(self):
        return MappingProxyType(self.__ui_attributes)

    def in_group(self, group_name):
        return group_name in self.__raw_data.groups

    def node(self):
        return self.__node

    def draw_size(self):
        return self.__size

    def layer_visible(self):
        return self.__layer < self.__visible_layers_count

    def set_layer(self, layer: int):
        assert layer >= 0
        self.__layer = layer
        self.setZValue(1.0/(1.0 + layer))

    def set_state_details(self, state_details: Optional[str] = None):
        if self.__raw_data.state_details == state_details:
            return
        self.__raw_data.state_details = state_details
        self.__state_details_cached = None

    def set_state(self, state: Optional[TaskState], paused: Optional[bool]):
        if (state is None or state == self.__raw_data.state) and (paused is None or self.__raw_data.paused == paused):
            return
        if state is not None:
            self.__raw_data.state = state
            self.set_state_details(None)
            if state != TaskState.IN_PROGRESS:
                self.__raw_data.progress = None
        if paused is not None:
            self.__raw_data.paused = paused
        if self.__node:
            self.__node.task_state_changed(self)
        self.update()
        self.refresh_ui()

    def set_task_data(self, raw_data: TaskData):
        self.__state_details_cached = None
        state_changed = self.__raw_data.state != raw_data.state
        self.__raw_data = raw_data
        if state_changed and self.__node:
            self.__node.task_state_changed(self)
            self.update()
            self.refresh_ui()

    def apply_task_delta(self, task_delta: TaskDelta, animated=True):
        if task_delta.paused is not DataNotSet:
            self.set_state(None, task_delta.paused)
        if task_delta.state is not DataNotSet:
            self.set_state(task_delta.state, None)
        if task_delta.name is not DataNotSet:
            self.set_name(task_delta.name)
        if task_delta.node_id is not DataNotSet:
            node = self.scene().get_node(task_delta.node_id)
            if node is not None:
                node.add_task(self, animated)
        if task_delta.work_data_invocation_attempt is not DataNotSet:
            self.__raw_data.work_data_invocation_attempt = task_delta.work_data_invocation_attempt
        if task_delta.node_output_name is not DataNotSet:
            self.__raw_data.node_output_name = task_delta.node_output_name
        if task_delta.node_input_name is not DataNotSet:
            self.__raw_data.node_input_name = task_delta.node_input_name
        if task_delta.invocation_id is not DataNotSet:
            self.__raw_data.invocation_id = task_delta.invocation_id
        if task_delta.split_id is not DataNotSet:
            self.__raw_data.split_id = task_delta.split_id
        if task_delta.children_count is not DataNotSet:
            self.__raw_data.children_count = task_delta.children_count
        if task_delta.active_children_count is not DataNotSet:
            self.__raw_data.active_children_count = task_delta.active_children_count
        if task_delta.groups is not DataNotSet:
            self.set_groups(task_delta.groups)
        if task_delta.split_origin_task_id is not DataNotSet:
            self.__raw_data.split_origin_task_id = task_delta.split_origin_task_id
        if task_delta.split_level is not DataNotSet:
            self.__raw_data.split_level = task_delta.split_level
        if task_delta.progress is not DataNotSet:
            self.__raw_data.progress = task_delta.progress
        if task_delta.parent_id is not DataNotSet:
            self.__raw_data.parent_id = task_delta.parent_id
        if task_delta.state_details is not DataNotSet:
            self.set_state_details(task_delta.state_details)
        self.update()
        self.update_ui()

    def set_progress(self, progress: float):
        self.__raw_data.progress = progress
        # logger.debug('progress %d', progress)
        self.update()
        self.update_ui()

    def get_progress(self) -> Optional[float]:
        return self.__raw_data.progress if self.__raw_data else None

    def update_log(self, alllog: Dict[int, Dict[int, Union[IncompleteInvocationLogData, InvocationLogData]]], full_update: bool):
        """
        This function gets called by scene with new shit from worker. Maybe there's more sense to make it "_protected"
        :param alllog: is expected to be a dict of node_id -> (dict of invocation_id -> (invocation dict) )
        :param full_update: is true, if log dict covers all invocations.
            otherwise update is considered partial, so only updated information counts, no removes are to be done
        :return:
        """
        logger.debug('log updated (full=%s) with %d entries', full_update, sum(len(x.values()) for _, x in alllog.items()))
        # Note that we assume log deletion is not possible
        updated_invocations = set()
        for node_id, invocs in alllog.items():
            updated_invocations.update(invocs.keys())
            if node_id not in self.__log:
                self.__log[node_id] = invocs
                continue
            for inv_id, logs in invocs.items():
                if inv_id in self.__log[node_id]:
                    assert logs is not None
                    if isinstance(logs, IncompleteInvocationLogData):
                        self.__log[node_id][inv_id].copy_from(logs)
                        continue
                self.__log[node_id][inv_id] = logs
        # if it's full update - we clear invocations that are present in task, but not in updated info
        if full_update:
            for node_id, invocs in self.__log.items():
                for inv_id in list(invocs.keys()):
                    if inv_id not in updated_invocations:
                        logger.debug('removing %d invocation from task %d', inv_id, self.get_id())
                        invocs.pop(inv_id)

        # clear cached inverted dict, it will be rebuilt on next access
        self.__inv_log = None

        self.update_ui()

    def remove_invocations_log(self, invocation_ids: List[int]):
        logger.debug('removing invocations for %s', invocation_ids)
        for _, invocs in self.__log:
            for invocation_id in invocation_ids:
                if invocation_id in invocs:
                    invocs.pop(invocation_id)

        # clear cached inverted dict, it will be rebuilt on next access
        self.__inv_log = None

        self.update_ui()

    def invocation_logs(self) -> List[Tuple[int, int, Union[IncompleteInvocationLogData, InvocationLogData]]]:
        """
        TODO: ensure immutable!
        """
        if self.__inv_log is None:
            self.__inv_log = []
            for node_id, logdict in self.__log.items():
                for inv_id, log in logdict.items():
                    self.__inv_log.append((inv_id, node_id, log))
        return self.__inv_log

    def update_attributes(self, attributes: dict):
        logger.debug('attrs updated with %s', attributes)
        self.__ui_attributes = attributes
        self.update_ui()

    def set_environment_attributes(self, env_attrs: Optional[EnvironmentResolverArguments]):
        self.__ui_env_res_attributes = env_attrs
        self.update_ui()

    def environment_attributes(self) -> Optional[EnvironmentResolverArguments]:
        return self.__ui_env_res_attributes

    def set_node(self, node: Optional[Node], pos: Optional[QPointF] = None, layer: Optional[int] = None):
        """
        """
        need_ui_update = node != self.__node

        if self.__node and self.__node != node:
            self.__node.remove_task(self)
        if self.__animation_group is not None:
            self.__animation_group.stop()
            self.__animation_group.deleteLater()
            self.__animation_group = None
        self.__node = node
        self.setParentItem(self.__node)
        if pos is not None:
            self.setPos(pos)
        if layer is not None:
            self.set_layer(layer)
        if need_ui_update:
            self.refresh_ui()

    def set_node_animated(self, node: Optional[Node], pos: QPointF, layer: int):
        # first try to optimize, if we move on the same node to invisible layer - dont animate
        if node == self.__node and layer >= self.__visible_layers_count and self.__animation_group is None:
            return self.set_node(node, pos, layer)
        #
        dist = ((pos if node is None else node.mapToScene(pos)) - self.final_scene_position())
        ldist = sqrt(QPointF.dotProduct(dist, dist))
        self.set_layer(0)
        animgroup = self.__animation_group
        if animgroup is None:
            animgroup = QSequentialAnimationGroup(self.scene())
            animgroup.finished.connect(self._clear_animation_group)
        anim_speed = max(1.0, animgroup.animationCount()-2)  # -2 to start speedup only after a couple anims in queue
        new_animation = TaskAnimation(self, node, pos, duration=max(1, int(ldist / anim_speed)), parent=animgroup)
        if self.__animation_group is None:
            self.setParentItem(None)
            self.__animation_group = animgroup

        self.__final_pos = pos
        self.__final_layer = layer
        # turns out i do NOT need to add animation to group IF animgroup was passed as parent to animation - it's added automatically
        # self.__animation_group.addAnimation(new_animation)
        if self.__animation_group.state() != QAbstractAnimation.Running:
            self.__animation_group.start()
        if self.__node and self.__node != node:
            self.__node.remove_task(self)
        need_ui_update = node != self.__node
        self.__node = node
        if need_ui_update:
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

    def is_in_animation(self):
        return self.__animation_group is not None

    @Slot()
    def _clear_animation_group(self):
        if self.__animation_group is not None:
            ag, self.__animation_group = self.__animation_group, None
            ag.stop()  # just in case some recursion occures
            ag.deleteLater()
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
                if (isinstance(invoc_dict, IncompleteInvocationLogData)
                        or invoc_dict.invocation_state != InvocationState.FINISHED) and invoc_id in self.__requested_invocs_while_selected:
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
                self.scene()._task_selected(self)
            elif not value:
                self.setFlag(QGraphicsItem.ItemIsSelectable, False)  # we are not selectable any more by band selection until directly clicked
                pass

        elif change == QGraphicsItem.ItemSceneChange:
            if value is None:  # removing item from scene
                if self.__animation_group is not None:
                    self.__animation_group.stop()
                    self.__animation_group.clear()
                    self.__animation_group.deleteLater()
                    self.__animation_group = None
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
            assert isinstance(view, nodeeditor.NodeEditor)
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

    def hoverMoveEvent(self, event):
        self.__hoverover_pos = event.pos()

    def hoverLeaveEvent(self, event):
        self.__hoverover_pos = None
        self.update()

    @staticmethod
    def _draw_dict_table(attributes: dict, table_name: str):
        imgui.columns(2, table_name)
        imgui.separator()
        imgui.text('name')
        imgui.next_column()
        imgui.text('value')
        imgui.next_column()
        imgui.separator()
        for key, val in attributes.items():
            imgui.text(key)
            imgui.next_column()
            imgui.text(repr(val))
            imgui.next_column()
        imgui.columns(1)

    #
    # interface
    def draw_imgui_elements(self, drawing_widget):
        imgui.text(f'Task {self.get_id()} {self.__raw_data.name}')
        imgui.text(f'state: {self.__raw_data.state.name}')
        imgui.text(f'groups: {", ".join(self.__raw_data.groups)}')
        imgui.text(f'parent id: {self.__raw_data.parent_id}')
        imgui.text(f'children count: {self.__raw_data.children_count}')
        imgui.text(f'split level: {self.__raw_data.split_level}')
        imgui.text(f'invocation attempts: {self.__raw_data.work_data_invocation_attempt}')

        # first draw attributes
        if self.__ui_attributes:
            self._draw_dict_table(self.__ui_attributes, 'node_task_attributes')

        if self.__ui_env_res_attributes:
            tab_expanded, _ = imgui.collapsing_header(f'environment resolver attributes##collapsing_node_task_environment_resolver_attributes')
            if tab_expanded:
                imgui.text(f'environment resolver: "{self.__ui_env_res_attributes.name()}"')
                if self.__ui_env_res_attributes.arguments():
                    self._draw_dict_table(self.__ui_env_res_attributes.arguments(), 'node_task_environment_resolver_attributes')

        # now draw log
        imgui.text('Logs:')
        for node_id, invocs in self.__log.items():
            node: Node = self.scene().get_node(node_id)
            if node is None:
                logger.warning(f'node for task {self.get_id()} does not exist')
                continue
            node_name: str = node.node_name()
            node_expanded, _ = imgui.collapsing_header(f'node {node_id}' + (f' "{node_name}"' if node_name else ''))
            if not node_expanded:  # or invocs is None:
                continue
            for invoc_id, invoc_log in invocs.items():
                # TODO: pyimgui is not covering a bunch of fancy functions... watch when it's done
                imgui.indent(10)
                invoc_expanded, _ = imgui.collapsing_header(f'invocation {invoc_id}' +
                                                            (f', worker {invoc_log.worker_id}' if isinstance(invoc_log, InvocationLogData) is not None else '') +
                                                            f', time: {timedelta(seconds=round(invoc_log.invocation_runtime)) if invoc_log.invocation_runtime is not None else "N/A"}' +
                                                            f'###logentry_{invoc_id}')
                if not invoc_expanded:
                    imgui.unindent(10)
                    continue
                if invoc_id not in self.__requested_invocs_while_selected:
                    self.__requested_invocs_while_selected.add(invoc_id)
                    self.scene().request_log(invoc_id)
                if isinstance(invoc_log, IncompleteInvocationLogData):
                    imgui.text('...fetching...')
                else:
                    if invoc_log.stdout:
                        if imgui.button(f'open in viewer##{invoc_id}'):
                            fetch_and_open_log_viewer(self.scene(), invoc_id, drawing_widget, update_interval=None if invoc_log.invocation_state == InvocationState.FINISHED else 5)

                        imgui.text_unformatted(invoc_log.stdout or '...nothing here...')
                    if invoc_log.invocation_state == InvocationState.IN_PROGRESS:
                        if imgui.button('update'):
                            logger.debug('clicked')
                            if invoc_id in self.__requested_invocs_while_selected:
                                self.__requested_invocs_while_selected.remove(invoc_id)
                imgui.unindent(10)


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
