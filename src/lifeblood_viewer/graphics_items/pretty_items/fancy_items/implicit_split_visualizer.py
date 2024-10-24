from PySide6.QtWidgets import QWidget, QStyleOptionGraphicsItem
from PySide6.QtCore import Qt, Slot, QRectF, QPointF
from PySide6.QtGui import QPainterPath, QPainter, QBrush, QPen, QColor
from typing import Optional

from ..node_decorator_base import NodeDecorator
from ..drawable_node import DrawableNode

from typing import Tuple


class ImplicitSplitVisualizer(NodeDecorator):
    _arc_path: QPainterPath = None
    _text_pen = None

    def __init__(self, parent_node: DrawableNode):
        super(ImplicitSplitVisualizer, self).__init__(parent_node)
        self.__output_names: Tuple[str, ...] = ()
        self.__output_poss: Tuple[QPointF, ...] = ()

        self.__brush = QBrush(QColor.fromRgbF(0.9, 0.6, 0.2, 0.2))
        self.__text_bounds = QRectF(-20, 15, 40, 16)

        if self._arc_path is None:
            newpath = QPainterPath()
            arcbb = QRectF(-40, 0, 80, 50)
            newpath.arcMoveTo(arcbb, 225)
            newpath.arcTo(arcbb, 225, 90)
            arcbb1 = QRectF(arcbb)
            arcbb1.setTopLeft(arcbb.topLeft() * 0.8)
            arcbb1.setBottomRight(arcbb.bottomRight() * 0.8)
            newpath.arcTo(arcbb1, 315, -90)
            newpath.closeSubpath()

            ImplicitSplitVisualizer._arc_path = newpath
            ImplicitSplitVisualizer._text_pen = QPen(QColor.fromRgbF(1, 1, 1, 0.2))

    def boundingRect(self) -> QRectF:
        arcrect = self._arc_path.boundingRect()
        if self.__output_poss:
            rect: QRectF = QRectF(
                QPointF(min(p.x() for p in self.__output_poss), min(p.x() for p in self.__output_poss)),
                QPointF(max(p.x() for p in self.__output_poss), max(p.x() for p in self.__output_poss))
            )
        else:
            rect: QRectF = QRectF()
        rect.united(self.__text_bounds)
        return QRectF(
            rect.left() + arcrect.left(),
            arcrect.top(),
            rect.width() + arcrect.width(),
            arcrect.height()
        ).adjusted(-1, -1, 1, 1)  # why adjusted? just not to forget later to adjust when pen gets involved. currently it's not needed

    def paint(self, painter: QPainter, option: QStyleOptionGraphicsItem, widget: Optional[QWidget] = None) -> None:
        for pos, name in zip(self.__output_poss, self.__output_names):
            if len(self.node().output_connections(name)) <= 1:
                continue
            painter.fillPath(self._arc_path.translated(pos), self.__brush)
            painter.setPen(self._text_pen)
            painter.drawText(self.__text_bounds.translated(pos), Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignTop, 'split')

    def node_updated(self):
        output_names = self.node().output_names()
        output_poss = tuple(self.node().get_output_position(name, local=True) for name in output_names)
        if output_names == self.__output_names and output_poss == self.__output_poss:
            return
        self.__output_names = output_names
        self.__output_poss = output_poss
        self.prepareGeometryChange()
