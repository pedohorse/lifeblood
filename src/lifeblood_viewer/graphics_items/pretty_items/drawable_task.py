from math import sqrt
from lifeblood import logging
from lifeblood.enums import TaskState
from lifeblood.ui_protocol_data import TaskData
from .task_animation import TaskAnimation
from ..graphics_items import Node, Task
from ..graphics_scene_container import GraphicsSceneWithNodesAndTasks

from PySide6.QtCore import QAbstractAnimation, Slot, QPointF, QRectF, QSizeF, QSequentialAnimationGroup
from PySide6.QtGui import QBrush, QColor, QPainter, QPainterPath, QPen
from PySide6.QtWidgets import QGraphicsItem, QStyleOptionGraphicsItem, QWidget

from typing import Optional


logger = logging.get_logger('viewer')


class DrawableTask(Task):
    __brushes = None
    __borderpen = None
    __paused_pen = None

    def __init__(self, scene: GraphicsSceneWithNodesAndTasks, task_data: TaskData):
        super().__init__(scene, task_data)
        self.setZValue(1)
        self.__layer = 0  # draw layer from 0 - main up to inf. kinda like LOD with highres being 0
        self.__visible_layers_count = 2

        self.__size = 16
        self.__line_width = 1.5

        self.__animation_group: Optional[QSequentialAnimationGroup] = None
        self.__final_pos = None
        self.__final_layer = None
        self.__hoverover_pos = None

        self.__mainshape_cache = None  # NOTE: DYNAMIC SIZE OR LINE WIDTH ARE NOT SUPPORTED HERE!
        self.__selshape_cache = None
        self.__pausedshape_cache = None
        self.__bound_cache = None

        def lerpclr(c1, c2, t):
            color = c1
            color.setAlphaF(lerp(color.alphaF(), c2.alphaF(), t))
            color.setRedF(lerp(color.redF(), c2.redF(), t))
            color.setGreenF(lerp(color.greenF(), c2.redF(), t))
            color.setBlueF(lerp(color.blueF(), c2.redF(), t))
            return color

        if self.__borderpen is None:
            DrawableTask.__borderpen = [
                QPen(QColor(96, 96, 96, 255), self.__line_width),
                QPen(QColor(128, 128, 128, 255), self.__line_width),
                QPen(QColor(192, 192, 192, 255), self.__line_width)
            ]

        if self.__brushes is None:
            # brushes and paused_pen are precalculated for several layers with different alphas, just not to calc them in paint
            def lerp(a, b, t):
                return a*(1.0-t) + b*t

            DrawableTask.__brushes = {
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
            for k, v in DrawableTask.__brushes.items():
                ocolor = v.color()
                DrawableTask.__brushes[k] = []
                for i in range(self.__visible_layers_count):
                    color = lerpclr(ocolor, QColor.fromRgbF(0, 0, 0, 1), i*1.0/self.__visible_layers_count)
                    DrawableTask.__brushes[k].append(QColor(color))
        if self.__paused_pen is None:
            ocolor = QColor(64, 64, 128, 192)
            DrawableTask.__paused_pen = []
            for i in range(self.__visible_layers_count):
                color = lerpclr(ocolor, QColor.fromRgbF(0, 0, 0, 1), i*1.0/self.__visible_layers_count)
                DrawableTask.__paused_pen.append(QPen(color, self.__line_width*3))

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
        if change == QGraphicsItem.ItemSceneChange:
            if value is None:  # removing item from scene
                if self.__animation_group is not None:
                    self.__animation_group.stop()
                    self.__animation_group.clear()
                    self.__animation_group.deleteLater()
                    self.__animation_group = None
                if self.node() is not None:
                    self.node().remove_task(self)
        return super().itemChange(change, value)

    def hoverMoveEvent(self, event):
        self.__hoverover_pos = event.pos()

    def hoverLeaveEvent(self, event):
        self.__hoverover_pos = None
        self.update()
