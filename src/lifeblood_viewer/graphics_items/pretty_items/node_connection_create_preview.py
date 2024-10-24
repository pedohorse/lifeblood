from ..utils import length2
from .drawable_node import DrawableNode
from .node_connection_snap_point import NodeConnSnapPoint

from PySide6.QtCore import Qt, QPointF, QRectF
from PySide6.QtGui import QColor, QPainter, QPainterPath, QPen
from PySide6.QtWidgets import QGraphicsItem, QStyleOptionGraphicsItem, QGraphicsSceneMouseEvent, QWidget

from typing import Callable, List, Optional


class NodeConnectionCreatePreview(QGraphicsItem):
    def __init__(self, nodeout: Optional[DrawableNode], nodein: Optional[DrawableNode], outname: str, inname: str, snap_points: List[NodeConnSnapPoint], snap_radius: float, report_done_here: Callable, do_cutting: bool = False):
        super().__init__()
        assert nodeout is None and nodein is not None or \
               nodeout is not None and nodein is None
        self.setFlags(QGraphicsItem.GraphicsItemFlag.ItemSendsGeometryChanges)
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
        self.__cutpen.setStyle(Qt.PenStyle.DotLine)

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
        if event.button() != Qt.MouseButton.LeftButton:
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
        if event.button() != Qt.MouseButton.LeftButton:
            event.ignore()
            return
        if self.__finished_callback is not None:
            self.__finished_callback(self.get_closest_snappoint(event.scenePos()))
        event.accept()
        self.ungrabMouse()
