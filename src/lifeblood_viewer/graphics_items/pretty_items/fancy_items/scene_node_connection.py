from math import sqrt
from lifeblood import logging
from ...graphics_items import NodeConnection
from ...utils import call_later, length2
from ..node_connection_create_preview import NodeConnectionCreatePreview
from ..node_connection_snap_point import NodeConnSnapPoint
from ..drawable_node_with_snap_points import DrawableNodeWithSnapPoints
from ...graphics_scene_container import GraphicsSceneWithNodesAndTasks

from lifeblood_viewer.scene_data_controller import SceneDataController
from lifeblood_viewer.graphics_scene_viewing_widget import GraphicsSceneViewingWidgetBase

from PySide6.QtCore import Qt, Slot, QPointF, QRectF
from PySide6.QtGui import QColor, QPainter, QPainterPath, QPainterPathStroker, QPen
from PySide6.QtWidgets import QGraphicsItem, QStyleOptionGraphicsItem, QGraphicsSceneMouseEvent, QWidget

from typing import Optional, Tuple

logger = logging.get_logger('viewer')


class SceneNodeConnection(NodeConnection):
    def __init__(self, scene: GraphicsSceneWithNodesAndTasks, id: int, nodeout: DrawableNodeWithSnapPoints, nodein: DrawableNodeWithSnapPoints, outname: str, inname: str, data_controller: SceneDataController):
        super().__init__(scene, id, nodeout, nodein, outname, inname)
        self.__scene_container = scene
        self.__data_controller: SceneDataController = data_controller
        self.setFlags(QGraphicsItem.GraphicsItemFlag.ItemSendsGeometryChanges)  # QGraphicsItem.ItemIsSelectable |
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

    def output(self) -> Tuple[DrawableNodeWithSnapPoints, str]:
        node, name = super().output()
        assert isinstance(node, DrawableNodeWithSnapPoints)
        return node, name

    def input(self) -> Tuple[DrawableNodeWithSnapPoints, str]:
        node, name = super().input()
        assert isinstance(node, DrawableNodeWithSnapPoints)
        return node, name

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
        if event.button() != Qt.MouseButton.LeftButton:
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
            if hasattr(event, 'item_event_candidates'):
                event.item_event_candidates.append((self.distance_to_point(p), self))

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
                snap_points = [y for x in self.__scene_container.nodes() if x != innode and isinstance(x, DrawableNodeWithSnapPoints) for y in x.output_snap_points()]
            else:
                snap_points = [y for x in self.__scene_container.nodes() if x != outnode and isinstance(x, DrawableNodeWithSnapPoints) for y in x.input_snap_points()]
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
