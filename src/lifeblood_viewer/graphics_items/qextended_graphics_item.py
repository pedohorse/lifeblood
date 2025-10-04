from PySide6.QtWidgets import QGraphicsItem, QGraphicsSceneMouseEvent


class QGraphicsItemExtended(QGraphicsItem):
    def __init__(self):
        super().__init__()

        # the bug the cheat below is working around seem to have been fixed in pyside 6.8 (maybe earlier)
        # # cheat cuz Shiboken.Object does not respect mro
        # mro = self.__class__.mro()
        # cur_mro_i = mro.index(QGraphicsItemExtended)
        # if len(mro) > cur_mro_i + 2:
        #     super(mro[cur_mro_i + 2], self).__init__()

    def post_mousePressEvent(self, event: QGraphicsSceneMouseEvent):
        """
        special "event" when mousePressEvent uses candidates
        """
        pass
