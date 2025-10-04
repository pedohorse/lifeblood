from ..graphics_items import Node, Task

from PySide6.QtCore import QAbstractAnimation, QPointF


class TaskAnimation(QAbstractAnimation):
    def __init__(self, task: Task, node1: Node, pos1: QPointF,  node2: Node, pos2: QPointF, duration: int, parent):
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
