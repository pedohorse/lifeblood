from PySide2.QtWidgets import QGraphicsItem, QGraphicsSceneMouseEvent


class QGraphicsItemExtended(QGraphicsItem):
    def __init__(self):
        super().__init__()
        # cheat cuz Shiboken.Object does not respect mro
        mro = self.__class__.mro()
        cur_mro_i = mro.index(QGraphicsItemExtended)
        if len(mro) > cur_mro_i + 2:
            super(mro[cur_mro_i + 2], self).__init__()

    def post_mousePressEvent(self, event: QGraphicsSceneMouseEvent):
        """
        special "event" when mousePressEvent uses candidates
        """
        pass
