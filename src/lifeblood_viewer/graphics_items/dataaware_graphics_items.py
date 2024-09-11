from datetime import timedelta
from math import sqrt
import imgui
from lifeblood import logging
from lifeblood.config import get_config
from lifeblood.enums import TaskState, NodeParameterType, InvocationState
from lifeblood.uidata import CollapsableVerticalGroup, OneLineParametersLayout, Parameter, ParameterExpressionError, ParametersLayoutBase, Separator, NodeUi
from lifeblood.ui_protocol_data import TaskData, IncompleteInvocationLogData, InvocationLogData
from .graphics_items import Node, NodeConnection, Task
from .network_item_watchers import NetworkItemWatcher
from .node_extra_items import ImplicitSplitVisualizer
from .utils import call_later, length2
from ..editor_scene_integration import fetch_and_open_log_viewer
from ..scene_data_controller import SceneDataController
from ..code_editor.editor import StringParameterEditor
from ..graphics_scene_container import GraphicsSceneWithNodesAndTasks
from ..graphics_scene_viewing_widget import GraphicsSceneViewingWidgetBase

from PySide2.QtCore import QAbstractAnimation, Qt, Slot, QPointF, QRectF, QSizeF, QSequentialAnimationGroup
from PySide2.QtGui import QBrush, QColor, QDesktopServices, QLinearGradient, QPainter, QPainterPath, QPainterPathStroker, QPen
from PySide2.QtWidgets import QGraphicsItem, QStyleOptionGraphicsItem, QGraphicsSceneMouseEvent, QWidget

from typing import Callable, Iterable, List, Optional, Set, Tuple


logger = logging.get_logger('viewer')


class SnapPoint:
    def pos(self) -> QPointF:
        raise NotImplementedError()


class NodeConnSnapPoint(SnapPoint):
    def __init__(self, node: Node, connection_name: str, connection_is_input: bool):
        super().__init__()
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


class TaskAnimation(QAbstractAnimation):
    def __init__(self, task: "Task", node1: "Node", pos1: "QPointF",  node2: "Node", pos2: "QPointF", duration: int, parent):
        super().__init__(parent)
        self.__task = task

        self.__node1 = node1
        self.__pos1 = pos1
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


class SceneNode(Node):
    base_height = 100
    base_width = 150

    def __init__(self, scene: GraphicsSceneWithNodesAndTasks, id: int, type: str, name: str, data_controller: SceneDataController):
        super().__init__(scene, id, type, name)
        self.__scene_container = scene
        self.__data_controller: SceneDataController = data_controller
        self.__visual_tasks: List[Task] = []

        # display
        self.setFlags(QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemIsSelectable | QGraphicsItem.ItemSendsGeometryChanges)
        self.setAcceptHoverEvents(True)
        self.__nodeui_menucache = {}
        self.__ui_selected_tab = 0

        self.__hoverover_pos: Optional[QPointF] = None
        self.__height = self.base_height
        self.__width = self.base_width
        self.__pivot_x = 0
        self.__pivot_y = 0

        self.__ui_interactor = None
        self.__ui_grabbed_conn = None
        self.__ui_widget: Optional[GraphicsSceneViewingWidgetBase] = None

        self.__move_start_position = None
        self.__move_start_selection = None

        self.__input_radius = 12
        self.__input_visible_radius = 8
        self.__line_width = 1

        self.__node_ui_for_io_requested = False

        # prepare default drawing tools
        self.__borderpen = QPen(QColor(96, 96, 96, 255))
        self.__borderpen_selected = QPen(QColor(144, 144, 144, 255))
        self.__caption_pen = QPen(QColor(192, 192, 192, 255))
        self.__typename_pen = QPen(QColor(128, 128, 128, 192))
        self.__borderpen.setWidthF(self.__line_width)
        self.__header_brush = QBrush(QColor(48, 64, 48, 192))
        self.__body_brush = QBrush(QColor(48, 48, 48, 128))
        self.__connector_brush = QBrush(QColor(48, 48, 48, 192))
        self.__connector_brush_hovered = QBrush(QColor(96, 96, 96, 128))

        self.__expanded = False

        self.__cached_bounds = None
        self.__cached_nodeshape = None
        self.__cached_bodymask = None
        self.__cached_headershape = None
        self.__cached_bodyshape = None
        self.__cached_expandbutton_shape = None

        # misc
        self.__manual_url_base = get_config('viewer').get_option_noasync('manual_base_url', 'https://pedohorse.github.io/lifeblood')

        # children!
        self.__vismark = ImplicitSplitVisualizer(self)
        self.__vismark.setPos(QPointF(0, self._get_nodeshape().boundingRect().height() * 0.5))
        self.__vismark.setZValue(-2)

    def apply_settings(self, settings_name: str):
        self.__data_controller.request_apply_node_settings(self.get_id(), settings_name)

    def pause_all_tasks(self):
        self.__data_controller.set_tasks_paused([x.get_id() for x in self.tasks_iter()], True)

    def resume_all_tasks(self):
        self.__data_controller.set_tasks_paused([x.get_id() for x in self.tasks_iter()], False)

    def update_nodeui(self, nodeui: NodeUi):
        super().update_nodeui(nodeui)
        self.__nodeui_menucache = {}

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

        for i, task in enumerate(self.tasks()):
            self.__make_task_child_with_position(task, *self.get_task_pos(task, i), animate=True)

    def get_input_position(self, name: str = 'main') -> QPointF:
        if not self.input_names():
            idx = 0
            cnt = 1
        elif name not in self.input_names():
            raise RuntimeError(f'unexpected input name {name}')
        else:
            idx = self.input_names().index(name)
            cnt = len(self.input_names())
        assert cnt > 0
        return self.mapToScene(-0.5 * self.__width + (idx + 1) * self.__width/(cnt + 1) - self.__pivot_x,
                               -0.5 * self.__height - self.__pivot_y)

    def get_output_position(self, name: str = 'main') -> QPointF:
        if not self.output_names():
            idx = 0
            cnt = 1
        elif name not in self.output_names():
            raise RuntimeError(f'unexpected output name {name} , {self.output_names()}')
        else:
            idx = self.output_names().index(name)
            cnt = len(self.output_names())
        assert cnt > 0
        return self.mapToScene(-0.5 * self.__width + (idx + 1) * self.__width/(cnt + 1) - self.__pivot_x,
                               0.5 * self.__height - self.__pivot_y)

    def input_snap_points(self):
        # TODO: cache snap points, don't recalc them every time
        if self.get_nodeui() is None:
            return []
        inputs = []
        for input_name in self.get_nodeui().inputs_names():
            inputs.append(NodeConnSnapPoint(self, input_name, True))
        return inputs

    def output_snap_points(self):
        # TODO: cache snap points, don't recalc them every time
        if self.get_nodeui() is None:
            return []
        outputs = []
        for output_name in self.get_nodeui().outputs_names():
            outputs.append(NodeConnSnapPoint(self, output_name, False))
        return outputs


    # move animation

    def get_task_pos(self, task: "Task", pos_id: int) -> Tuple[QPointF, int]:
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

    def __make_task_child_with_position(self, task: "Task", pos: QPointF, layer: int, *, animate: bool = False):
        """
        helper function that actually changes parent of a task and initializes animations if needed
        """
        assert isinstance(task, SceneTask)  # TODO: hmmm
        if animate:
            task.append_task_move_animation(self, pos, layer)
        else:
            task.set_task_position(self, pos, layer)

    def add_task(self, task: "Task"):
        if task in self.__visual_tasks:
            assert task in self.tasks()
            return

        # the animated part
        pos_id = len(self.__visual_tasks)
        if task.node() is None:
            self.__make_task_child_with_position(task, *self.get_task_pos(task, pos_id))
        else:
            self.__make_task_child_with_position(task, *self.get_task_pos(task, pos_id), animate=True)

        super().add_task(task)
        insert_at = self._find_insert_index_for_task(task, prefer_back=True)

        self.__visual_tasks.append(None)  # temporary placeholder, it'll be eliminated either in the loop, or after if task is last
        for i in reversed(range(insert_at + 1, len(self.__visual_tasks))):
            self.__visual_tasks[i] = self.__visual_tasks[i - 1]  # TODO: animated param should affect below!
            self.__make_task_child_with_position(self.__visual_tasks[i], *self.get_task_pos(self.__visual_tasks[i], i), animate=True)
        self.__visual_tasks[insert_at] = task
        self.__make_task_child_with_position(self.__visual_tasks[insert_at], *self.get_task_pos(task, insert_at), animate=True)

    def remove_tasks(self, tasks_to_remove: Iterable["Task"]):
        tasks_to_remove = set(tasks_to_remove)
        super().remove_tasks(tasks_to_remove)

        self.__visual_tasks: List["Task"] = [None if x in tasks_to_remove else x for x in self.__visual_tasks]
        off = 0
        for i, task in enumerate(self.__visual_tasks):
            if task is None:
                off += 1
            else:
                self.__visual_tasks[i - off] = self.__visual_tasks[i]
                self.__make_task_child_with_position(self.__visual_tasks[i - off], *self.get_task_pos(self.__visual_tasks[i - off], i - off), animate=True)
        self.__visual_tasks = self.__visual_tasks[:-off]
        for x in tasks_to_remove:
            assert x not in self.__visual_tasks

    def remove_task(self, task_to_remove: "Task"):
        super().remove_task(task_to_remove)
        task_pid = self.__visual_tasks.index(task_to_remove)

        for i in range(task_pid, len(self.__visual_tasks) - 1):
            self.__visual_tasks[i] = self.__visual_tasks[i + 1]
            self.__make_task_child_with_position(self.__visual_tasks[i], *self.get_task_pos(self.__visual_tasks[i], i), animate=True)
        self.__visual_tasks = self.__visual_tasks[:-1]
        assert task_to_remove not in self.__visual_tasks
        self.item_updated(redraw=True, ui=False)  # cuz node displays task number - we should redraw

    def _find_insert_index_for_task(self, task, prefer_back=False):
        if task.state() == TaskState.IN_PROGRESS and not prefer_back:
            return 0

        if task.state() != TaskState.IN_PROGRESS and prefer_back:
            return len(self.__visual_tasks)

        # now fun thing: we either have IN_PROGRESS and prefer_back, or NOT IN_PROGRESS and NOT prefer_back
        #  and both cases have the same logic for position finding
        for i, task in enumerate(self.__visual_tasks):
            if task.state() != TaskState.IN_PROGRESS:
                return i
        else:
            return len(self.__visual_tasks)

    def task_state_changed(self, task):
        """
        here node might decide to highlight the task that changed state one way or another
        """
        if task.state() not in (TaskState.IN_PROGRESS, TaskState.GENERATING, TaskState.POST_GENERATING):
            return

        # find a place
        append_at = self._find_insert_index_for_task(task)

        if append_at == len(self.__visual_tasks):  # this is impossible case (in current impl of _find_insert_index_for_task) (cuz task is in __visual_tasks, and it's not in IN_PROGRESS)
            return

        idx = self.__visual_tasks.index(task)
        if idx <= append_at:  # already in place (and ignore moving further
            return

        # place where it has to be
        for i in reversed(range(append_at + 1, idx+1)):
            self.__visual_tasks[i] = self.__visual_tasks[i-1]
            self.__make_task_child_with_position(self.__visual_tasks[i], *self.get_task_pos(self.__visual_tasks[i], i), animate=True)
        self.__visual_tasks[append_at] = task
        self.__make_task_child_with_position(self.__visual_tasks[append_at], *self.get_task_pos(task, append_at), animate=True)

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
                    if item.is_readonly() or item.is_locked():  # TODO: treat locked items somehow different, but for now it's fine
                        imgui.text(menu_items_inv[item.value()])
                        return
                    else:
                        changed, val = imgui.combo('##'.join((param_label, param_name, idstr)), menu_order_inv[menu_items_inv[item.value()]], menu_order)
                        if changed:
                            new_item_val = menu_items[menu_order[val]]
                else:
                    if item.is_readonly() or item.is_locked():  # TODO: treat locked items somehow different, but for now it's fine
                        imgui.text(f'{item.value()}')
                        if item.label():
                            imgui.same_line()
                            imgui.text(f'{item.label()}')
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
                            if item.type() == NodeParameterType.STRING:
                                new_item_expression = item.python_from_expandable_string(item.unexpanded_value())
                            else:
                                new_item_expression = str(item.value())
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
                # TODO: op below may fail, so callback to display error should be provided
                self.__data_controller.change_node_parameter(self.get_id(), item,
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
                imgui.indent(5)
                for child in item.items(recursive=False):
                    h, w = item.relative_size_for_child(child)
                    self.__draw_single_item(child, (h*size[0], w*size[1]), drawing_widget=drawing_widget)
                imgui.unindent(5)
                imgui.separator()
        elif isinstance(item, ParametersLayoutBase):
            imgui.indent(5)
            for child in item.items(recursive=False):
                h, w = item.relative_size_for_child(child)
                if isinstance(child, Parameter):
                    if not child.visible():
                        continue
                self.__draw_single_item(child, (h*size[0], w*size[1]), drawing_widget=drawing_widget)
            imgui.unindent(5)
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
        imgui.text(f'Node {self.get_id()}, type "{self.node_type()}", name {self.node_name()}')

        if imgui.selectable(f'parameters##{self.node_name()}', self.__ui_selected_tab == 0, width=imgui.get_window_width() * 0.5 * 0.7)[1]:
            self.__ui_selected_tab = 0
        imgui.same_line()
        if imgui.selectable(f'description##{self.node_name()}', self.__ui_selected_tab == 1, width=imgui.get_window_width() * 0.5 * 0.7)[1]:
            self.__ui_selected_tab = 1
        imgui.separator()

        if self.__ui_selected_tab == 0:
            if (nodeui := self.get_nodeui()) is not None:
                self.__draw_single_item(nodeui.main_parameter_layout(), drawing_widget=drawing_widget)
        elif self.__ui_selected_tab == 1:

            if (node_type := self.node_type()) in self.__data_controller.node_types() and imgui.button('open manual page'):
                plugin_info = self.__data_controller.node_types()[node_type].plugin_info
                category = plugin_info.category
                package = plugin_info.package_name
                QDesktopServices.openUrl(self.__manual_url_base + f'/nodes/{category}{f"/{package}" if package else ""}/{self.node_type()}.html')
            imgui.text(self.__data_controller.node_types()[self.node_type()].description if self.node_type() in self.__data_controller.node_types() else 'error')

    #
    # scene item
    #

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

    def reanalyze_nodeui(self):
        self.prepareGeometryChange()  # not calling this seem to be able to break scene's internal index info on our connections
        # bug that appears - on first scene load deleting a node with more than 1 input/output leads to crash
        # on open nodes have 1 output, then they receive interface update and this func is called, and here's where bug may happen

        super().reanalyze_nodeui()
        css = self.get_nodeui().color_scheme()
        if css.secondary_color() is not None:
            gradient = QLinearGradient(-self.__width*0.1, 0, self.__width*0.1, 16)
            gradient.setColorAt(0.0, QColor(*(x * 255 for x in css.main_color()), 192))
            gradient.setColorAt(1.0, QColor(*(x * 255 for x in css.secondary_color()), 192))
            self.__header_brush = QBrush(gradient)
        else:
            self.__header_brush = QBrush(QColor(*(x * 255 for x in css.main_color()), 192))
        self.item_updated(redraw=True, ui=True)  # cuz input count affects visualization in the graph

    def prepareGeometryChange(self):
        super().prepareGeometryChange()
        self.__cached_bounds = None
        self.__cached_nodeshape = None
        self.__cached_bodymask = None
        self.__cached_headershape = None
        self.__cached_bodyshape = None
        self.__cached_expandbutton_shape = None
        for conn in self.all_connections():
            conn.prepareGeometryChange()

    def paint(self, painter: QPainter, option: QStyleOptionGraphicsItem, widget: Optional[QWidget] = None) -> None:
        screen_rect = painter.worldTransform().mapRect(self.boundingRect())
        painter.pen().setWidthF(self.__line_width)
        nodeshape = self._get_nodeshape()

        # this request from paint here is SUS
        if not self.__node_ui_for_io_requested:
            self.__node_ui_for_io_requested = True
            self.__data_controller.request_node_ui(self.get_id())

        if screen_rect.width() > 40:
            ninputs = len(self.input_names())
            noutputs = len(self.output_names())
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
            painter.drawText(headershape.boundingRect(), Qt.AlignHCenter | Qt.AlignTop, self.node_name())
            painter.setPen(self.__typename_pen)
            painter.drawText(headershape.boundingRect(), Qt.AlignRight | Qt.AlignBottom, self.node_type())
            painter.drawText(headershape.boundingRect(), Qt.AlignLeft | Qt.AlignBottom, f'{len(self.tasks())}')

    def itemChange(self, change, value):
        if change == QGraphicsItem.ItemSelectedHasChanged:
            if value and self.graphics_scene().get_inspected_item() == self:   # item was just selected, And is the first selected
                self.__data_controller.request_node_ui(self.get_id())
        elif change == QGraphicsItem.ItemPositionChange:
            if self.__move_start_position is None:
                self.__move_start_position = self.pos()
            for connection in self.all_connections():
                connection.prepareGeometryChange()

        return super().itemChange(change, value)

    def mousePressEvent(self, event: QGraphicsSceneMouseEvent):
        if event.button() == Qt.LeftButton and self.__ui_interactor is None:
            wgt = event.widget().parent()
            assert isinstance(wgt, GraphicsSceneViewingWidgetBase)
            pos = event.scenePos()
            r2 = (self.__input_radius + 0.5*self.__line_width)**2

            # check expand button
            expand_button_shape = self._get_expandbutton_shape()
            if expand_button_shape.contains(event.pos()):
                self.set_expanded(not self.__expanded)
                event.ignore()
                return

            for input in self.input_names():
                inpos = self.get_input_position(input)
                if QPointF.dotProduct(inpos - pos, inpos - pos) <= r2 and wgt.request_ui_focus(self):
                    snap_points = [y for x in self.__scene_container.nodes() if x != self for y in x.output_snap_points()]
                    displayer = NodeConnectionCreatePreview(None, self, '', input, snap_points, 15, self._ui_interactor_finished)
                    self.scene().addItem(displayer)
                    self.__ui_interactor = displayer
                    self.__ui_grabbed_conn = input
                    self.__ui_widget = wgt
                    event.accept()
                    self.__ui_interactor.mousePressEvent(event)
                    return

            for output in self.output_names():
                outpos = self.get_output_position(output)
                if QPointF.dotProduct(outpos - pos, outpos - pos) <= r2 and wgt.request_ui_focus(self):
                    snap_points = [y for x in self.__scene_container.nodes() if x != self for y in x.input_snap_points()]
                    displayer = NodeConnectionCreatePreview(self, None, output, '', snap_points, 15, self._ui_interactor_finished)
                    self.scene().addItem(displayer)
                    self.__ui_interactor = displayer
                    self.__ui_grabbed_conn = output
                    self.__ui_widget = wgt
                    event.accept()
                    self.__ui_interactor.mousePressEvent(event)
                    return

            if not self._get_nodeshape().contains(event.pos()):
                event.ignore()
                return

        super().mousePressEvent(event)
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
            assert isinstance(view, GraphicsSceneViewingWidgetBase)
            view.item_requests_context_menu(self)
            event.accept()

    def mouseMoveEvent(self, event: QGraphicsSceneMouseEvent):
        # if self.__ui_interactor is not None:
        #     event.accept()
        #     self.__ui_interactor.mouseMoveEvent(event)
        #     return
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event: QGraphicsSceneMouseEvent):
        # if self.__ui_interactor is not None:
        #     event.accept()
        #     self.__ui_interactor.mouseReleaseEvent(event)
        #     return
        super().mouseReleaseEvent(event)
        if self.__move_start_position is not None:
            if self.__scene_container.node_snapping_enabled():
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
    def _ui_interactor_finished(self, snap_point: Optional[NodeConnSnapPoint]):
        assert self.__ui_interactor is not None
        call_later(lambda x: logger.debug(f'later removing {x}') or x.scene().removeItem(x), self.__ui_interactor)
        if self.scene() is None:  # if scheduler deleted us while interacting
            return
        if self.__ui_widget is None:
            raise RuntimeError('interaction finalizer called, but ui widget is not set')

        grabbed_conn = self.__ui_grabbed_conn
        self.__ui_widget.release_ui_focus(self)
        self.__ui_widget = None
        self.__ui_interactor = None
        self.__ui_grabbed_conn = None

        # actual node reconection
        if snap_point is None:
            logger.debug('no change')
            return

        setting_out = not snap_point.connection_is_input()
        self.__data_controller.add_connection(snap_point.node().get_id() if setting_out else self.get_id(),
                                              snap_point.connection_name() if setting_out else grabbed_conn,
                                              snap_point.node().get_id() if not setting_out else self.get_id(),
                                              snap_point.connection_name() if not setting_out else grabbed_conn)


class SceneNodeConnection(NodeConnection):
    def __init__(self, scene: GraphicsSceneWithNodesAndTasks, id: int, nodeout: Node, nodein: Node, outname: str, inname: str, data_controller: SceneDataController):
        super().__init__(scene, id, nodeout, nodein, outname, inname)
        self.__scene_container = scene
        self.__data_controller: SceneDataController = data_controller
        self.setFlags(QGraphicsItem.ItemSendsGeometryChanges)  # QGraphicsItem.ItemIsSelectable |
        self.setAcceptHoverEvents(True)  # for highlights

        self.setZValue(-1)
        self.__line_width = 6  # TODO: rename it to match what it represents
        self.__wire_pick_radius = 15
        self.__pick_radius2 = 100 ** 2
        self.__curv = 150
        self.__wire_highlight_radius = 5

        self.__temporary_invalid = False

        self.__ui_interactor: Optional[NodeConnectionCreatePreview] = None

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
        self.__stroker.setWidth(2 * self.__wire_pick_radius)

        self.__hoverover_pos = None

        # to ensure correct interaction
        self.__ui_widget: Optional[GraphicsSceneViewingWidgetBase] = None

    def distance_to_point(self, pos: QPointF):
        """
        returns approx distance to a given point
        currently it has the most crude implementation
        :param pos:
        :return:
        """

        line = self.get_painter_path()
        # determine where to start
        outnode, outname = self.output()
        innode, inname = self.input()
        p0 = outnode.get_output_position(outname)
        p1 = innode.get_input_position(inname)

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
        outnode, outname = self.output()
        innode, inname = self.input()
        if outname not in outnode.output_names() or inname not in innode.input_names():
            self.__temporary_invalid = True
            return QRectF()
        self.__temporary_invalid = False
        hlw = self.__line_width
        line = self.get_painter_path()
        return line.boundingRect().adjusted(-hlw - self.__wire_pick_radius, -hlw, hlw + self.__wire_pick_radius, hlw)

    def shape(self):
        # this one is mainly needed for proper selection and item picking
        return self.__stroker.createStroke(self.get_painter_path())

    def get_painter_path(self, close_path=False):
        line = QPainterPath()

        outnode, outname = self.output()
        innode, inname = self.input()
        p0 = outnode.get_output_position(outname)
        p1 = innode.get_input_position(inname)
        curv = self.__curv
        curv = min((p0-p1).manhattanLength()*0.5, curv)
        line.moveTo(p0)
        line.cubicTo(p0 + QPointF(0, curv), p1 - QPointF(0, curv), p1)
        if close_path:
            line.cubicTo(p1 - QPointF(0, curv), p0 + QPointF(0, curv), p0)
        return line

    def paint(self, painter: QPainter, option: QStyleOptionGraphicsItem, widget: Optional[QWidget] = None) -> None:
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
            outnode, outname = self.output()
            innode, inname = self.input()
            p0 = outnode.get_output_position(outname)
            p1 = innode.get_input_position(inname)
            d02 = QPointF.dotProduct(p0 - p, p0 - p)
            d12 = QPointF.dotProduct(p1 - p, p1 - p)
            if d02 > self.__pick_radius2 and d12 > self.__pick_radius2:  # if picked too far from ends - just select
                super().mousePressEvent(event)
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
        wgt = event.widget().parent()
        p = event.scenePos()
        outnode, outname = self.output()
        innode, inname = self.input()
        p0 = outnode.get_output_position(outname)
        p1 = innode.get_input_position(inname)
        d02 = QPointF.dotProduct(p0 - p, p0 - p)
        d12 = QPointF.dotProduct(p1 - p, p1 - p)

        assert isinstance(wgt, GraphicsSceneViewingWidgetBase)
        if wgt.request_ui_focus(self):
            event.accept()

            output_picked = d02 < d12
            if output_picked:
                snap_points = [y for x in self.__scene_container.nodes() if x != innode for y in x.output_snap_points()]
            else:
                snap_points = [y for x in self.__scene_container.nodes() if x != outnode for y in x.input_snap_points()]
            self.__ui_interactor = NodeConnectionCreatePreview(None if output_picked else outnode,
                                                               innode if output_picked else None,
                                                               outname, inname,
                                                               snap_points, 15, self._ui_interactor_finished, True)
            self.update()
            self.__ui_widget = wgt
            self.scene().addItem(self.__ui_interactor)
            self.__ui_interactor.mousePressEvent(event)

    def mouseMoveEvent(self, event: QGraphicsSceneMouseEvent) -> None:
        # if self.__ui_interactor is not None:  # redirect input, cuz scene will direct all events to this item. would be better to change focus, but so far scene.setFocusItem did not work as expected
        #     self.__ui_interactor.mouseMoveEvent(event)
        #     event.accept()
        super().mouseMoveEvent(event)

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
        super().mouseReleaseEvent(event)

    # _dbg_shitlist = []
    @Slot(object)
    def _ui_interactor_finished(self, snap_point: Optional[NodeConnSnapPoint]):
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
            self.__data_controller.cut_connection_by_id(self.get_id())
            return

        # actual node reconection
        if snap_point is None:
            logger.debug('no change')
            return

        changing_out = not snap_point.connection_is_input()
        self.__data_controller.change_connection_by_id(
            self.get_id(),
            to_outnode_id=snap_point.node().get_id() if changing_out else None,
            to_outname=snap_point.connection_name() if changing_out else None,
            to_innode_id=None if changing_out else snap_point.node().get_id(),
            to_inname=None if changing_out else snap_point.connection_name()
        )
        # scene.request_node_connection_change(self.get_id(),
        #                                      snap_point.node().get_id() if changing_out else None,
        #                                      snap_point.connection_name() if changing_out else None,
        #                                      None if changing_out else snap_point.node().get_id(),
        #                                      None if changing_out else snap_point.connection_name())


class SceneTask(Task):
    __brushes = None
    __borderpen = None
    __paused_pen = None

    def __init__(self, scene: GraphicsSceneWithNodesAndTasks, task_data: TaskData, data_controller: SceneDataController):
        super().__init__(scene, task_data)
        self.__scene_container = scene
        self.__data_controller = data_controller
        self.setAcceptHoverEvents(True)
        self.__hoverover_pos = None
        # self.setFlags(QGraphicsItem.ItemIsSelectable)
        self.setZValue(1)
        self.__layer = 0  # draw layer from 0 - main up to inf. kinda like LOD with highres being 0
        self.__visible_layers_count = 2

        self.__size = 16
        self.__line_width = 1.5

        self.__ui_interactor = None
        self.__press_pos = None

        self.__animation_group: Optional[QSequentialAnimationGroup] = None
        self.__final_pos = None
        self.__final_layer = None

        self.__mainshape_cache = None  # NOTE: DYNAMIC SIZE OR LINE WIDTH ARE NOT SUPPORTED HERE!
        self.__selshape_cache = None
        self.__pausedshape_cache = None
        self.__bound_cache = None

        self.__requested_invocs_while_selected = set()

        def lerpclr(c1, c2, t):
            color = c1
            color.setAlphaF(lerp(color.alphaF(), c2.alphaF(), t))
            color.setRedF(lerp(color.redF(), c2.redF(), t))
            color.setGreenF(lerp(color.greenF(), c2.redF(), t))
            color.setBlueF(lerp(color.blueF(), c2.redF(), t))
            return color

        if self.__borderpen is None:
            SceneTask.__borderpen = [QPen(QColor(96, 96, 96, 255), self.__line_width),
                                QPen(QColor(128, 128, 128, 255), self.__line_width),
                                QPen(QColor(192, 192, 192, 255), self.__line_width)]

        if self.__brushes is None:
            # brushes and paused_pen are precalculated for several layers with different alphas, just not to calc them in paint
            def lerp(a, b, t):
                return a*(1.0-t) + b*t

            SceneTask.__brushes = {
                TaskState.WAITING: QBrush(QColor(64, 64, 64, 192)),
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
                TaskState.POST_WAITING_BLOCKED: QBrush(QColor(40, 40, 60, 192))
            }
            for k, v in SceneTask.__brushes.items():
                ocolor = v.color()
                SceneTask.__brushes[k] = []
                for i in range(self.__visible_layers_count):
                    color = lerpclr(ocolor, QColor.fromRgbF(0, 0, 0, 1), i*1.0/self.__visible_layers_count)
                    SceneTask.__brushes[k].append(QColor(color))
        if self.__paused_pen is None:
            ocolor = QColor(64, 64, 128, 192)
            SceneTask.__paused_pen = []
            for i in range(self.__visible_layers_count):
                color = lerpclr(ocolor, QColor.fromRgbF(0, 0, 0, 1), i*1.0/self.__visible_layers_count)
                SceneTask.__paused_pen.append(QPen(color, self.__line_width*3))

    def layer_visible(self):
        return self.__layer < self.__visible_layers_count

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

    def paint(self, painter: QPainter, option: QStyleOptionGraphicsItem, widget: Optional[QWidget] = None) -> None:
        if self.__layer >= self.__visible_layers_count:
            return
        if self.node() is None:  # probably temporary state due to asyncronous incoming events from scheduler
            return  # or we can draw them somehow else?
        screen_rect = painter.worldTransform().mapRect(self.boundingRect())

        path = self._get_mainpath()
        brush = self.__brushes[self.state()][self.__layer]
        painter.fillPath(path, brush)
        if progress := self.get_progress():
            arcpath = QPainterPath()
            arcpath.arcTo(QRectF(-0.5*self.__size, -0.5*self.__size, self.__size, self.__size),
                          90, -3.6*progress)
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

    def draw_size(self):
        return self.__size

    def set_layer(self, layer: int):
        assert layer >= 0
        self.__layer = layer
        self.setZValue(1.0/(1.0 + layer))

    def add_item_watcher(self, watcher: "NetworkItemWatcher"):
        super().add_item_watcher(watcher)
        # additionally refresh ui if we are not being watched
        if len(self.item_watchers()) == 1:  # it's a first watcher
            self.refresh_ui()

    def set_name(self, name: str):
        super().set_name(name)
        self.refresh_ui()

    def set_groups(self, groups: Set[str]):
        super().set_groups(groups)
        self.refresh_ui()

    def refresh_ui(self):
        """
        unlike update - this method actually queries new task ui status
        if task is not selected or not watched- does nothing
        :return:
        """
        if not self.isSelected() and len(self.item_watchers()) == 0:
            return
        self.__data_controller.request_log_meta(self.get_id())  # update all task metadata: which nodes it ran on and invocation numbers only
        self.__data_controller.request_attributes(self.get_id())

        for invoc_id, nid, invoc_dict in self.invocation_logs():
            if invoc_dict is None:
                continue
            if (isinstance(invoc_dict, IncompleteInvocationLogData)
                    or invoc_dict.invocation_state != InvocationState.FINISHED) and invoc_id in self.__requested_invocs_while_selected:
                self.__requested_invocs_while_selected.remove(invoc_id)

    def final_location(self) -> (Node, QPointF):
        if self.__animation_group is not None:
            assert self.__final_pos is not None
            return self.node(), self.__final_pos
        else:
            return self.node(), self.pos()

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
            self.setParentItem(self.node())
            self.setPos(self.__final_pos)
            self.set_layer(self.__final_layer)
            self.__final_pos = None
            self.__final_layer = None

    def set_task_position(self, node: Node, pos: QPointF, layer: int):
        """
        set task position to given node and give pos/layer inside that node
        also cancels any active move animation
        """
        if self.__animation_group is not None:
            self.__animation_group.stop()
            self.__animation_group.deleteLater()
            self.__animation_group = None

        self.setParentItem(node)
        if pos is not None:
            self.setPos(pos)
        if layer is not None:
            self.set_layer(layer)

    def append_task_move_animation(self, node: Node, pos: QPointF, layer: int):
        """
        set task position to given node and give pos/layer inside that node,
        but do it with animation
        """
        # first try to optimize, if we move on the same node to invisible layer - don't animate
        if node == self.node() and layer >= self.__visible_layers_count and self.__animation_group is None:
            return self.set_task_position(node, pos, layer)

        #
        dist = ((pos if node is None else node.mapToScene(pos)) - self.final_scene_position())
        ldist = sqrt(QPointF.dotProduct(dist, dist))
        self.set_layer(0)
        animgroup = self.__animation_group
        if animgroup is None:
            animgroup = QSequentialAnimationGroup(self.scene())
            animgroup.finished.connect(self._clear_animation_group)
        anim_speed = max(1.0, animgroup.animationCount() - 2)  # -2 to start speedup only after a couple anims in queue
        start_node, start_pos = self.final_location()
        new_animation = TaskAnimation(self, start_node, start_pos, node, pos, duration=max(1, int(ldist / anim_speed)), parent=animgroup)
        if self.__animation_group is None:
            self.setParentItem(None)
            self.__animation_group = animgroup

        self.__final_pos = pos
        self.__final_layer = layer
        # turns out i do NOT need to add animation to group IF animgroup was passed as parent to animation - it's added automatically
        # self.__animation_group.addAnimation(new_animation)
        if self.__animation_group.state() != QAbstractAnimation.Running:
            self.__animation_group.start()

    def itemChange(self, change, value):
        if change == QGraphicsItem.ItemSelectedHasChanged:
            if value and self.node() is not None:   # item was just selected
                self.refresh_ui()
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
                if self.node() is not None:
                    self.node().remove_task(self)
        return super().itemChange(change, value)  # TODO: maybe move this to scene's remove item?

    def mousePressEvent(self, event: QGraphicsSceneMouseEvent) -> None:
        if not self._get_selectshapepath().contains(event.pos()):
            event.ignore()
            return
        self.setFlag(QGraphicsItem.ItemIsSelectable, True)  # if we are clicked - we are now selectable until unselected. This is to avoid band selection
        super().mousePressEvent(event)
        self.__press_pos = event.scenePos()

        if event.button() == Qt.RightButton:
            # context menu time
            view = event.widget().parent()
            assert isinstance(view, GraphicsSceneViewingWidgetBase)
            view.item_requests_context_menu(self)
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
            super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event: QGraphicsSceneMouseEvent) -> None:
        if self.__ui_interactor:
            self.__ui_interactor.mouseReleaseEvent(event)
            nodes = [x for x in self.scene().items(event.scenePos(), Qt.IntersectsItemBoundingRect) if isinstance(x, Node)]  # TODO: dirty, implement such method in one of scene subclasses
            if len(nodes) > 0:
                logger.debug(f'moving item {self} to node {nodes[0]}')
                self.__data_controller.request_set_task_node(self.get_id(), nodes[0].get_id())
            call_later(self.__ui_interactor.scene().removeItem, self.__ui_interactor)
            self.__ui_interactor = None

        else:
            super().mouseReleaseEvent(event)

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
        imgui.text(f'Task {self.get_id()} {self.name()}')
        imgui.text(f'state: {self.state().name}')
        imgui.text(f'groups: {", ".join(self.groups())}')
        imgui.text(f'parent id: {self.parent_task_id()}')
        imgui.text(f'children count: {self.children_tasks_count()}')
        imgui.text(f'split level: {self.split_level()}')
        imgui.text(f'invocation attempts: {self.latest_invocation_attempt()}')

        # first draw attributes
        if self.attributes():
            self._draw_dict_table(self.attributes(), 'node_task_attributes')

        if env_res_args := self.environment_attributes():
            tab_expanded, _ = imgui.collapsing_header(f'environment resolver attributes##collapsing_node_task_environment_resolver_attributes')
            if tab_expanded:
                imgui.text(f'environment resolver: "{env_res_args.name()}"')
                if env_res_args.arguments():
                    self._draw_dict_table(env_res_args.arguments(), 'node_task_environment_resolver_attributes')

        # now draw log
        imgui.text('Logs:')
        for node_id, invocs in self.invocation_logs_mapping().items():
            node: Node = self.__scene_container.get_node(node_id)
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
                    self.__data_controller.request_log(invoc_id)
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


class NodeConnectionCreatePreview(QGraphicsItem):
    def __init__(self, nodeout: Optional[Node], nodein: Optional[Node], outname: str, inname: str, snap_points: List[NodeConnSnapPoint], snap_radius: float, report_done_here: Callable, do_cutting: bool = False):
        super().__init__()
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

    def paint(self, painter: QPainter, option: QStyleOptionGraphicsItem, widget: Optional[QWidget] = None) -> None:
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
        super().__init__()
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

    def paint(self, painter: QPainter, option: QStyleOptionGraphicsItem, widget: Optional[QWidget] = None) -> None:
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
