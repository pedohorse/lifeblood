from lifeblood import logging
from lifeblood.enums import TaskState
from ..graphics_items import Node, Task
from ..graphics_scene_base import GraphicsSceneBase
from .drawable_task import DrawableTask


from PySide6.QtCore import Qt, QPointF, QRectF
from PySide6.QtGui import QBrush, QColor, QLinearGradient, QPainter, QPainterPath, QPen
from PySide6.QtWidgets import QStyleOptionGraphicsItem, QWidget

from typing import Iterable, List, Optional, Tuple


logger = logging.get_logger('viewer')


class DrawableNode(Node):
    base_height = 100
    base_width = 150

    def __init__(self, scene: GraphicsSceneBase, id: int, type: str, name: str):
        super().__init__(scene, id, type, name)

        self.__visual_tasks: List[DrawableTask] = []

        # display
        self.__hoverover_pos: Optional[QPointF] = None
        self.__height = self.base_height
        self.__width = self.base_width
        self.__pivot_x = 0
        self.__pivot_y = 0

        self.__input_radius = 12
        self.__input_visible_radius = 8
        self.__line_width = 1

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

    # query

    def __pos_transform_maybe(self, pos: QPointF, local: bool):
        if local:
            return pos
        else:
            return self.mapToScene(pos)

    def body_bottom_center(self, local=True) -> QPointF:
        """
        return position of bottom center of the body of the node
        """
        return self.__pos_transform_maybe(
            QPointF(-self.__pivot_x, 0.5 * self.__height - self.__pivot_y),
            local
        )

    def _input_radius(self) -> float:
        return self.__input_radius

    def _line_width(self) -> float:
        return self.__line_width

    #

    def is_expanded(self) -> bool:
        return self.__expanded

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

        for i, task in enumerate(self.drawable_tasks()):
            self.__make_task_child_with_position(task, *self.get_task_pos(task, i), animate=True)
        self.item_updated()

    def get_input_position(self, name: str = 'main', *, local: bool = False) -> QPointF:
        if not self.input_names():
            idx = 0
            cnt = 1
        elif name not in self.input_names():
            raise RuntimeError(f'unexpected input name {name}')
        else:
            idx = self.input_names().index(name)
            cnt = len(self.input_names())
        assert cnt > 0
        return self.__pos_transform_maybe(
            QPointF(
                -0.5 * self.__width + (idx + 1) * self.__width/(cnt + 1) - self.__pivot_x,
                -0.5 * self.__height - self.__pivot_y
            ),
            local
        )

    def get_output_position(self, name: str = 'main', *, local: bool = False) -> QPointF:
        if not self.output_names():
            idx = 0
            cnt = 1
        elif name not in self.output_names():
            raise RuntimeError(f'unexpected output name {name} , {self.output_names()}')
        else:
            idx = self.output_names().index(name)
            cnt = len(self.output_names())
        assert cnt > 0
        return self.__pos_transform_maybe(
            QPointF(
                -0.5 * self.__width + (idx + 1) * self.__width/(cnt + 1) - self.__pivot_x,
                0.5 * self.__height - self.__pivot_y
            ),
            local
        )

    # move animation

    def get_task_pos(self, task: DrawableTask, pos_id: int) -> Tuple[QPointF, int]:
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

    def __make_task_child_with_position(self, task: DrawableTask, pos: QPointF, layer: int, *, animate: bool = False):
        """
        helper function that actually changes parent of a task and initializes animations if needed
        """
        if animate:
            task.append_task_move_animation(self, pos, layer)
        else:
            task.set_task_position(self, pos, layer)

    def drawable_tasks(self) -> Iterable[DrawableTask]:
        for task in self.tasks():
            if not isinstance(task, DrawableTask):
                continue
            yield task

    def add_task(self, task: Task):
        if not isinstance(task, DrawableTask):
            return super().add_task(task)

        # TODO: can anything be done to avoid this need for sorta dynamic_cast ?
        assert isinstance(task, DrawableTask)
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

        self.__visual_tasks = [None if x in tasks_to_remove else x for x in self.__visual_tasks]
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
        assert isinstance(task_to_remove, DrawableTask)  # TODO: see TODO in add_task
        task_pid = self.__visual_tasks.index(task_to_remove)

        for i in range(task_pid, len(self.__visual_tasks) - 1):
            self.__visual_tasks[i] = self.__visual_tasks[i + 1]
            self.__make_task_child_with_position(self.__visual_tasks[i], *self.get_task_pos(self.__visual_tasks[i], i), animate=True)
        self.__visual_tasks = self.__visual_tasks[:-1]
        assert task_to_remove not in self.__visual_tasks
        self.item_updated()

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
            gradient = QLinearGradient(-self.__width * 0.1, 0, self.__width * 0.1, 16)
            gradient.setColorAt(0.0, QColor(*(x * 255 for x in css.main_color()), 192))
            gradient.setColorAt(1.0, QColor(*(x * 255 for x in css.secondary_color()), 192))
            self.__header_brush = QBrush(gradient)
        else:
            self.__header_brush = QBrush(QColor(*(x * 255 for x in css.main_color()), 192))
        self.item_updated()

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

        if screen_rect.width() > 40:
            ninputs = len(self.input_names())
            noutputs = len(self.output_names())
            r2 = (self.__input_radius + 0.5 * self.__line_width) ** 2
            for fi in range(ninputs + noutputs):
                path = QPainterPath()
                is_inputs = fi < ninputs
                i = fi if is_inputs else fi - ninputs
                input_point = QPointF(-0.5 * self.__width + (i + 1) * self.__width / ((ninputs if is_inputs else noutputs) + 1) - self.__pivot_x,
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
            self.__borderpen_selected.setWidth(self.__line_width * width_mult)
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
            painter.drawText(headershape.boundingRect(), Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignTop, self.node_name())
            painter.setPen(self.__typename_pen)
            painter.drawText(headershape.boundingRect(), Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignBottom, self.node_type())
            painter.drawText(headershape.boundingRect(), Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignBottom, f'{len(self.tasks())}')

    def hoverMoveEvent(self, event):
        self.__hoverover_pos = event.pos()

    def hoverLeaveEvent(self, event):
        self.__hoverover_pos = None
        self.update()
