from ..uidata import UiData
from .connection_worker import SchedulerConnectionWorker

from PySide2.QtWidgets import QWidget, QListView
from PySide2.QtCore import Slot, Qt, QAbstractListModel, QModelIndex

from typing import Optional, Dict, List

class WorkerListWidget(QWidget):
    def __init__(self, parent=None):
        super(WorkerListWidget, self).__init__(parent)


class WorkerModel(QAbstractListModel):
    def __init__(self, worker: SchedulerConnectionWorker, parent=None):
        super(WorkerModel, self).__init__(parent)
        self.__workers: Dict[str, dict] = {}  # address is the key
        self.__order: List[str] = []
        self.__inv_order: Dict[str, int] = {}

    def rowCount(self, parent: QModelIndex = None) -> int:
        return len(self.__workers)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid():
            return None
        if role != Qt.DisplayRole:
            return None
        row = index.row()
        col = index.column()
        # TODO: this is just a placeholder
        return list(self.__workers[self.__order[row]].items())[col]

    @Slot(object)
    def full_update(self, uidata: UiData):
        new_workers = {x['last_address']: x for x in uidata.workers()}
        new_keys = set(new_workers.keys())
        old_keys = set(self.__workers.keys())

        # update
        for key in old_keys.intersection(new_keys):
            self.__workers[key].update(new_workers[key])
            self.dataChanged.emit(self.index(self.__inv_order[key], 0), self.index(self.__inv_order[key], self.rowCount() - 1))

        # insert
        to_insert = new_keys - old_keys
        if len(to_insert) > 0:
            self.beginInsertRows(QModelIndex(), self.rowCount(), self.rowCount() + len(to_insert))
            for key in to_insert:
                assert key not in self.__workers
                assert key not in self.__inv_order
                self.__workers[key] = new_workers[key]
                self.__inv_order[key] = len(self.__order)
                self.__order.append(key)
            self.endInsertRows()

        # remove
        to_remove = old_keys - new_keys
        if len(to_remove) > 0:
            for key in to_remove:
                assert key in self.__workers
                assert key in self.__inv_order
                self.beginRemoveRows(QModelIndex(), self.__inv_order[key], self.__inv_order[key])
                del self.__workers[key]
                self.__order.remove(key)
                self.__inv_order = {x: i for i, x in enumerate(self.__order)}
                self.endRemoveRows()
