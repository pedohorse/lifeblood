from datetime import datetime
from ..uidata import UiData
from ..enums import WorkerType
from .connection_worker import SchedulerConnectionWorker

from PySide2.QtWidgets import QWidget, QTableView, QHBoxLayout, QHeaderView
from PySide2.QtCore import Slot, Qt, QAbstractTableModel, QModelIndex

from typing import Optional, Dict, List


class WorkerListWidget(QWidget):
    def __init__(self, worker: SchedulerConnectionWorker, parent=None):
        super(WorkerListWidget, self).__init__(parent, Qt.Tool)
        self.__worker_list = QTableView()
        self.__worker_model = WorkerModel(worker, self)
        self.__worker_list.setModel(self.__worker_model)
        self.__worker_list.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)

        layout = QHBoxLayout(self)
        layout.addWidget(self.__worker_list)


class WorkerModel(QAbstractTableModel):
    def __init__(self, worker: SchedulerConnectionWorker, parent=None):
        super(WorkerModel, self).__init__(parent)
        self.__scheduler_worker = worker
        self.__workers: Dict[str, dict] = {}  # address is the key
        self.__order: List[str] = []
        self.__inv_order: Dict[str, int] = {}
        self.__cols = {'id': 'id', 'last_address': 'address', 'cpu_count': 'cpus', 'mem_size': 'mem', 'gpu_count': 'gpus', 'gmem_size': 'gmem', 'last_seen': 'last seen', 'worker_type': 'type'}
        self.__cols_order = ('id', 'last_address', 'cpu_count', 'mem_size', 'gpu_count', 'gmem_size', 'last_seen', 'worker_type')
        assert len(self.__cols) == len(self.__cols_order)

        self.start()

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        if orientation == Qt.Vertical:
            return None
        if role != Qt.DisplayRole:
            return None
        return self.__cols[self.__cols_order[section]]

    def columnCount(self, parent: QModelIndex = None) -> int:
        return len(self.__cols)

    def rowCount(self, parent: QModelIndex = None) -> int:
        return len(self.__workers)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid():
            return None
        if role != Qt.DisplayRole:
            return None
        row = index.row()
        col = index.column()
        col_name = self.__cols_order[col]
        raw_data = self.__workers[self.__order[row]][col_name]
        if col_name == 'last_seen':
            return datetime.fromtimestamp(raw_data).strftime('%H:%M:%S %d.%m.%Y')
        if col_name == 'worker_type':
            return str(WorkerType(raw_data))
        return raw_data

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

    def start(self):
        self.__scheduler_worker.full_update.connect(self.full_update)

    def stop(self):
        self.__scheduler_worker.disconnect(self)