from PySide2.QtWidgets import QWidget, QGraphicsItem, QStyleOptionGraphicsItem
from PySide2.QtCore import Qt, Slot, QRectF, QPointF
from PySide2.QtGui import QPainterPath, QPainter, QBrush, QPen, QColor
from typing import Optional


class ImplicitSplitVisualizer(QGraphicsItem):
    _arc_path: QPainterPath = None
    _text_pen = None

    def __init__(self, parent_node):
        super(ImplicitSplitVisualizer, self).__init__(parent_node)

        self.__brush = QBrush(QColor.fromRgbF(0.9, 0.6, 0.2, 0.2))

        if self._arc_path is None:
            newpath = QPainterPath()
            arcbb = QRectF(-40, -30, 80, 60)
            newpath.arcMoveTo(arcbb, 225)
            newpath.arcTo(arcbb, 225, 90)
            arcbb1 = QRectF(arcbb)
            arcbb1.setTopLeft(arcbb.topLeft() * 0.8)
            arcbb1.setBottomRight(arcbb.bottomRight() * 0.8)
            newpath.arcTo(arcbb1, 315, -90)
            newpath.closeSubpath()

            ImplicitSplitVisualizer._arc_path = newpath
            ImplicitSplitVisualizer._text_pen = QPen(QColor.fromRgbF(1, 1, 1, 0.2))

        self.__parent_node = parent_node

    def boundingRect(self) -> QRectF:
        rect: QRectF = self.__parent_node.boundingRect()
        arcrect = self._arc_path.boundingRect()
        return QRectF(rect.left() + arcrect.left(), arcrect.top(), rect.width() + arcrect.width(), arcrect.height()).adjusted(-1, -1, 1, 1)  # why adjusted? just not to forget later to adjust when pen gets involved. currently it's not needed

    def paint(self, painter: QPainter, option: QStyleOptionGraphicsItem, widget: Optional[QWidget] = None) -> None:
        for name in self.__parent_node.output_names():
            if len(self.__parent_node.output_connections(name)) > 1:
                shift = QPointF(self.__parent_node.mapFromScene(self.__parent_node.get_output_position(name)).x(), 0)
                painter.fillPath(self._arc_path.translated(shift), self.__brush)
                painter.setPen(self._text_pen)
                painter.drawText(QRectF(-20, 2, 40, 16).translated(shift), Qt.AlignHCenter | Qt.AlignTop, 'split')
