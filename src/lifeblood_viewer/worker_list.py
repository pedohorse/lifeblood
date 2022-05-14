from datetime import datetime
from lifeblood.uidata import UiData
from lifeblood.enums import WorkerType, WorkerState
from lifeblood.text import nice_memory_formatting
from .connection_worker import SchedulerConnectionWorker

from PySide2.QtWidgets import QWidget, QTableView, QHBoxLayout, QHeaderView
from PySide2.QtCore import Slot, Qt, QAbstractTableModel, QModelIndex, QSortFilterProxyModel
from PySide2.QtGui import QColor

from typing import Optional, Dict, List


class WorkerListWidget(QWidget):
    def __init__(self, worker: SchedulerConnectionWorker, parent=None):
        super(WorkerListWidget, self).__init__(parent, Qt.Tool)
        self.__worker_list = QTableView()
        self.__worker_model = WorkerModel(worker, self)
        self.__sort_model = QSortFilterProxyModel(self)
        self.__sort_model.setSourceModel(self.__worker_model)
        self.__sort_model.setSortRole(WorkerModel.SORT_ROLE)
        self.__sort_model.setDynamicSortFilter(True)  # careful with this if we choose to modify model through interface in future

        self.__sort_model.setFilterRole(WorkerModel.SORT_ROLE)
        self.__sort_model.setFilterRegExp(rf'^(?:(?!{WorkerState.UNKNOWN.value}).)*$')
        self.__sort_model.setFilterKeyColumn(self.__worker_model.column_by_name('state'))

        self.__worker_list.setModel(self.__sort_model)
        self.__worker_list.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.__worker_list.setSortingEnabled(True)
        self.__worker_list.sortByColumn(0, Qt.AscendingOrder)
        self.__worker_list.setFocusPolicy(Qt.NoFocus)

        layout = QHBoxLayout(self)
        layout.addWidget(self.__worker_list)

    def stop(self):
        self.__worker_model.stop()


class WorkerModel(QAbstractTableModel):
    __cols_with_totals = {'cpu_count', 'mem_size', 'gpu_count', 'gmem_size'}
    __mem_cols = {'mem_size', 'gmem_size'}
    SORT_ROLE = Qt.UserRole + 0

    def __init__(self, worker: SchedulerConnectionWorker, parent=None):
        super(WorkerModel, self).__init__(parent)
        self.__scheduler_worker = worker
        self.__workers: Dict[str, dict] = {}  # address is the key
        self.__order: List[str] = []
        self.__inv_order: Dict[str, int] = {}
        self.__cols = {'id': 'id', 'state': 'state', 'last_address': 'address', 'cpu_count': 'cpus', 'mem_size': 'mem',
                       'gpu_count': 'gpus', 'gmem_size': 'gmem', 'last_seen': 'last seen', 'worker_type': 'type',
                       'progress': 'task progress', 'groups': 'groups', 'task_id': 'task id'}
        self.__cols_order = ('id', 'state', 'progress', 'task_id', 'last_address', 'cpu_count', 'mem_size', 'gpu_count', 'gmem_size', 'groups', 'last_seen', 'worker_type')
        self.__colname_to_index = {k: i for i, k in enumerate(self.__cols_order)}
        assert len(self.__cols) == len(self.__cols_order)

        self.start()

    def column_by_name(self, name):
        return self.__colname_to_index[name]

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
        def format_display(col_name, raw):
            if col_name in self.__mem_cols:
                return nice_memory_formatting(raw)
            return raw

        if not index.isValid():
            return None
        if role not in (Qt.DisplayRole, Qt.BackgroundRole, self.SORT_ROLE):
            return None
        row = index.row()
        col = index.column()
        col_name = self.__cols_order[col]
        raw_data = self.__workers[self.__order[row]][col_name]
        if role == self.SORT_ROLE:  # for sorting
            return raw_data

        if col_name == 'state':
            data = WorkerState(raw_data)
            if role == Qt.DisplayRole:
                return data.name
            elif role == Qt.BackgroundRole:
                if data in (WorkerState.BUSY, WorkerState.INVOKING):
                    return QColor.fromRgb(255, 255, 0, 64)
                if data == WorkerState.IDLE:
                    return QColor.fromRgb(0, 255, 0, 64)
                if data == WorkerState.OFF:
                    return QColor.fromRgb(0, 0, 0, 64)
                if data == WorkerState.ERROR:
                    return QColor.fromRgb(255, 0, 0, 64)
                return None
        if col_name == 'progress':
            if self.__workers[self.__order[row]]['state'] == WorkerState.BUSY.value and raw_data is not None:
                return f'{raw_data}%'
            return ''
        if col_name == 'last_seen':
            return datetime.fromtimestamp(raw_data).strftime('%H:%M:%S %d.%m.%Y')
        if col_name == 'worker_type':
            return WorkerType(raw_data).name

        raw_data = format_display(col_name, raw_data)
        if col_name in self.__cols_with_totals:  # if there's total - make value in format if val/total, like 20/32
            raw_data = f'{raw_data}/{format_display(col_name, self.__workers[self.__order[row]][f"total_{col_name}"])}'

        return raw_data

    @Slot(object)
    def full_update(self, uidata: UiData):
        new_workers = {x['last_address']: x for x in uidata.workers()}
        new_keys = set(new_workers.keys())
        old_keys = set(self.__workers.keys())

        # update
        for key in old_keys.intersection(new_keys):
            self.__workers[key].update(new_workers[key])
            self.dataChanged.emit(self.index(self.__inv_order[key], 0), self.index(self.__inv_order[key], self.columnCount() - 1))

        # insert
        to_insert = new_keys - old_keys
        if len(to_insert) > 0:
            self.beginInsertRows(QModelIndex(), self.rowCount(), self.rowCount() + len(to_insert) - 1)
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
