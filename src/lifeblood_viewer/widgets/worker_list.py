from datetime import datetime
from dataclasses import dataclass
from lifeblood.ui_protocol_data import UiData, WorkerData, WorkerBatchData, WorkerResources, WorkerMetadata
from lifeblood.enums import WorkerType, WorkerState
from lifeblood.text import nice_memory_formatting
from lifeblood.logging import get_logger
from lifeblood.misc import timeit, performance_measurer
from lifeblood_viewer.connection_worker import SchedulerConnectionWorker
from lifeblood_viewer.models.multiple_sort_model import MultipleFilterSortProxyModel

from PySide2.QtWidgets import QWidget, QTableView, QTreeView, QHBoxLayout, QVBoxLayout, QHeaderView, QMenu, QLineEdit
from PySide2.QtCore import Slot, Signal, Qt, QAbstractItemModel, QAbstractTableModel, QModelIndex, QSortFilterProxyModel, QAbstractProxyModel, QPoint, QObject
from PySide2.QtGui import QColor

from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union


_init_column_order_prototype = ('id', 'state', 'progress', 'task_id', 'metadata.hostname', 'last_address', 'last_seen', '__resources__', 'devices', 'groups', 'worker_type')


class WorkerListWidget(QWidget):
    def __init__(self, worker: SchedulerConnectionWorker, parent=None):
        super(WorkerListWidget, self).__init__(parent, Qt.Tool)
        self.__worker_list = QTreeView()
        self.__worker_model = WorkerModel(worker, self)

        col_id = self.__worker_model.column_by_name('id')
        col_hostname = self.__worker_model.column_by_name('metadata.hostname')
        col_task_id = self.__worker_model.column_by_name('task_id')
        self.__sort_model = MultipleFilterSortProxyModel(self.__worker_model, [
            col_id,
            col_hostname,
            col_task_id,
        ], self)
        self.__sort_model.setSortRole(WorkerModel.SORT_ROLE)
        self.__sort_model.setFilterRole(WorkerModel.SORT_ROLE)

        self.__worker_list.setModel(self.__sort_model)
        # self.__worker_list.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)  # this cause incredible lag with QSplitter

        ico = _init_column_order_prototype
        self.__worker_list.header().resizeSection(ico.index('id'), 64)
        self.__worker_list.header().resizeSection(ico.index('state'), 64)
        self.__worker_list.header().resizeSection(ico.index('progress'), 64)
        self.__worker_list.header().resizeSection(ico.index('task_id'), 64)
        self.__worker_list.header().resizeSection(ico.index('last_address'), 200)
        self.__worker_list.header().resizeSection(ico.index('worker_type'), 135)
        self.__worker_list.header().resizeSection(ico.index('last_seen'), 140)

        self.__worker_list.setSortingEnabled(True)
        self.__worker_list.sortByColumn(0, Qt.AscendingOrder)
        self.__worker_list.setFocusPolicy(Qt.NoFocus)

        #
        search_field1 = QLineEdit()
        search_field2 = QLineEdit()
        search_field3 = QLineEdit()
        search_field1.setPlaceholderText('filter by id')
        search_field2.setPlaceholderText('filter by task')
        search_field3.setPlaceholderText('filter by hostname')
        search_field1.textChanged.connect(lambda text: self.__sort_model.set_filter_for_column(col_id, text))
        search_field2.textChanged.connect(lambda text: self.__sort_model.set_filter_for_column(col_task_id, text))
        search_field3.textChanged.connect(lambda text: self.__sort_model.set_filter_for_column(col_hostname, text))

        search_layout = QHBoxLayout()
        search_layout.addWidget(search_field1)
        search_layout.addWidget(search_field2)
        search_layout.addWidget(search_field3)

        #

        layout = QVBoxLayout(self)
        layout.addLayout(search_layout)
        layout.addWidget(self.__worker_list)

        self.__worker_list.setContextMenuPolicy(Qt.CustomContextMenu)

        # connec
        self.__worker_list.customContextMenuRequested.connect(self.show_context_menu)

    def stop(self):
        self.__worker_model.stop()

    def show_context_menu(self, pos: QPoint):
        gpos = self.__worker_list.mapToGlobal(pos)
        index = self.__sort_model.mapToSource(self.__worker_list.indexAt(pos))
        if not index.isValid():
            print('oh no:(')
            return
        menu = QMenu(self)

        menu.addAction('select currently running task').setEnabled(False)
        menu.addAction('stop currently running invocation', lambda: self.__worker_model.cancel_running_invocations(index))
        menu.aboutToHide.connect(menu.deleteLater)
        menu.popup(gpos)


@dataclass
class WorkerModelData:  # almost like WorkerData, but better for display
    id: Union[int, str]
    #worker_resources: WorkerResources
    hwid: int
    last_address: str
    last_seen_timestamp: int
    state: Union[WorkerState, Tuple[WorkerState, str]]
    type: Optional[WorkerType]
    current_invocation_node_id: Optional[int]
    current_invocation_task_id: Optional[int]
    current_invocation_id: Optional[int]
    current_invocation_progress: Optional[float]
    groups: Set[str]
    metadata: Optional[WorkerMetadata]

    @classmethod
    def from_worker_data(cls, data: WorkerData) -> "WorkerModelData":
        kwargs = {}
        for field in WorkerData.__dict__['__dataclass_fields__']:
            value = getattr(data, field)
            kwargs[field] = value
        return WorkerModelData(**kwargs)


class WorkerModel(QAbstractItemModel):
    SORT_ROLE = Qt.UserRole + 0

    group_update_requested = Signal(object, list)  # not int, cuz int in PySide is signed 32bit only
    cancel_invocation_for_worker = Signal(object)  # same about not int

    def __init__(self, worker: SchedulerConnectionWorker, parent=None):
        super(WorkerModel, self).__init__(parent)
        self.__logger = get_logger('viewer.worker_model')
        self.__scheduler_worker = worker

        self.__qt_gc_roots = {}

        self.__workers: Dict[int, WorkerModelData] = {}  # worker id -> worker data
        self.__worker_resources_names: List[str] = []
        self.__worker_hw_resources: Dict[int, WorkerResources] = {}

        #
        self.__hwid_summaries: Dict[int, Optional[WorkerModelData]] = {}  # Dict[hwid, ...

        #
        self.__hwid_order: List[int] = []  # List[hwid]
        self.__wid_order: Dict[int, List[int]] = {}  # Dict[hwid, List[worker_id]]

        #
        self.__cols = {}
        self.__cols_order = []
        self.__colname_to_index = {}
        assert len(self.__cols) == len(self.__cols_order)
        #

        self.__last_update_structure_resources = None

        self.__maybe_update_structure()

        self.start()

    def index(self, row: int, column: int, parent: QModelIndex=None):
        if parent is None:
            parent = QModelIndex()
        hwid = self.__hwid_order[parent.row()]
        if hwid not in self.__qt_gc_roots:
            self.__qt_gc_roots[hwid] = (hwid,)
        inter_pointer = self.__qt_gc_roots[hwid]
        # this old pyside2 (not tested in 6) bug: createIndex does not bump ref count, so we must ensure ref is not gc-ed
        #  another pyside2 (not tested in 6) bug: ints are clamped to 32bit, but hwid uses all 64 bits
        #  but yes, self.__qt_gc_roots is a POTENTIAL MEMORY LEAK. fortunately this scenario happens in an almost impossible use case.
        return self.createIndex(row, column,
                                inter_pointer if parent.isValid() else None,
                                )

    def parent(self, index: QModelIndex):
        if not index.isValid() or index.internalPointer() is None:
            return QModelIndex()
        hwid, = index.internalPointer()
        return self.createIndex(self.__hwid_order.index(hwid), 0, None)

    def __maybe_update_structure(self):
        """
        this update is updating data structure if resource definitions have changed.
        appropriate qt events are emitted
        """
        if self.__worker_resources_names == self.__last_update_structure_resources:
            return

        if self.__last_update_structure_resources is not None:  # None is only on initial update/init
            self.__logger.debug("worker resource definitions changed, updating data structure")
        self.beginResetModel()
        try:
            self.__cols_order = list(_init_column_order_prototype)
            i = self.__cols_order.index('__resources__')
            self.__cols_order = self.__cols_order[:i] + self.__worker_resources_names + self.__cols_order[i+1:]

            self.__cols = {'id': 'id', 'state': 'state', 'metadata.hostname': 'hostname', 'last_address': 'address',
                           'last_seen': 'last seen', 'worker_type': 'type',
                           'progress': 'progress', 'devices': 'devices', 'groups': 'groups', 'task_id': 'task id'}

            self.__cols.update({k: k.replace('_', ' ') for k in self.__worker_resources_names})

            self.__colname_to_index = {k: i for i, k in enumerate(self.__cols_order)}
            assert len(self.__cols) == len(self.__cols_order)
        finally:
            self.endResetModel()
        self.__last_update_structure_resources = list(self.__worker_resources_names)

    def column_by_name(self, name) -> int:
        return self.__colname_to_index[name]

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        if orientation == Qt.Vertical:
            return None
        if role != Qt.DisplayRole:
            return None
        return self.__cols[self.__cols_order[section]]

    def columnCount(self, parent: QModelIndex = None) -> int:
        return len(self.__cols_order)

    def rowCount(self, parent: QModelIndex = None) -> int:
        if parent is None or not parent.isValid():  # root
            return len(self.__hwid_order)
        if parent.internalPointer() is not None:  # wid level, has no children
            return 0
        return len(self.__wid_order[self.__hwid_order[parent.row()]])

    def __generate_summary(self, hwid):
        workers = [self.__workers[x] for x in self.__wid_order[hwid]]
        # make a pseudo worker statistical entry
        state = WorkerState.UNKNOWN
        if len(workers):
            state = max((x.state for x in workers), key=lambda state: {
                    WorkerState.ERROR: 10,  # just sorting order
                    WorkerState.BUSY: 5,
                    WorkerState.INVOKING: 4,
                    WorkerState.IDLE: 1,
                }.get(state, 0))

        w_instate_count = 0
        w_total_count = 0
        summ_address = workers[0].last_address.rsplit('|', 1)[0] if len(workers) else 'unknown'
        for worker in workers:
            if worker.state == state:
                w_instate_count += 1
            if worker.state != WorkerState.OFF:
                w_total_count += 1
            # address
            if summ_address != worker.last_address.rsplit('|', 1)[0]:
                summ_address = '<multiple>'


        return WorkerModelData(
            '',
            #workers[0].worker_resources if len(workers) else WorkerResources([]),
            hwid,
            summ_address,
            max(x.last_seen_timestamp for x in workers) if len(workers) else 0.0,
            (state, f'{w_instate_count}/{w_total_count}') if len(workers) else state,
            None,
            None,
            None,
            None,
            min(x.current_invocation_progress or 100 for x in workers) if len(workers) else None,
            workers[0].groups if len(workers) else (),
            workers[0].metadata if len(workers) else None,
        )

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        def format_display(col_name, raw):
            if col_name.endswith('_mem') or col_name == 'mem':
                return nice_memory_formatting(raw)
            return raw

        if not index.isValid():
            return None
        if role not in (Qt.DisplayRole, Qt.ToolTipRole, Qt.EditRole, Qt.BackgroundRole, self.SORT_ROLE):
            return None
        row = index.row()
        col = index.column()
        col_name = self.__cols_order[col]
        if not index.parent().isValid():  # first level (hwid)
            hwid = self.__hwid_order[row]
            if self.__hwid_summaries.get(hwid, None) is None:
                self.__hwid_summaries[hwid] = self.__generate_summary(hwid)
            worker = self.__hwid_summaries[hwid]
        else:
            worker = self.__workers[self.__wid_order[self.__hwid_order[index.parent().row()]][row]]

        if col_name == 'id':
            raw_data = worker.id
            return raw_data
        if col_name == 'state':
            if isinstance(worker.state, tuple):
                state, extra = worker.state
            else:
                state = worker.state
                extra = None
            if role == Qt.DisplayRole:
                return f'{state.name} {extra}' if extra else state.name
            elif role == Qt.BackgroundRole:
                if state in (WorkerState.BUSY, WorkerState.INVOKING):
                    return QColor.fromRgb(255, 255, 0, 64)
                if state == WorkerState.IDLE:
                    return QColor.fromRgb(0, 255, 0, 64)
                if state == WorkerState.OFF:
                    return QColor.fromRgb(0, 0, 0, 64)
                if state == WorkerState.ERROR:
                    return QColor.fromRgb(255, 0, 0, 64)
                return None
            elif role == self.SORT_ROLE:  # for sorting
                return state.value
        if col_name.startswith('metadata.'):
            metafield = col_name.split('.', 1)[1]
            return getattr(worker.metadata, metafield)
        if col_name == 'progress':
            data = worker.current_invocation_progress
            if role == self.SORT_ROLE:  # for sorting
                return data
            if (worker.state == WorkerState.BUSY or isinstance(worker.state, tuple) and worker.state[0] == WorkerState.BUSY) and data is not None:
                return f'{data}%'
            return ''
        if col_name == 'task_id':
            return worker.current_invocation_task_id
        if col_name == 'last_address':
            return worker.last_address
        if col_name == 'groups':
            if index.parent().isValid():  # do not display hw groups on children since it's all the same
                return
            return ', '.join(worker.groups)
        if col_name == 'last_seen':
            if role == self.SORT_ROLE:  # for sorting
                return worker.last_seen_timestamp
            return datetime.fromtimestamp(worker.last_seen_timestamp).strftime('%H:%M:%S %d.%m.%Y')
        if col_name == 'worker_type':
            if worker.type is None:
                return None
            return worker.type.name

        raw_data = 'none'
        if col_name in self.__worker_resources_names:
            if index.parent().isValid():  # do not display hw resources on children since it's all the same
                return
            i = self.__worker_resources_names.index(col_name)
            if not (worker_resources := self.__worker_hw_resources.get(worker.hwid)):
                return 'internal error'
            if len(worker_resources) <= i or worker_resources[i].name != col_name:  # if data is called after worker_resource_names update, but before actual worker data update:
                raw_data = '...'
            else:
                raw_data = f'{format_display(col_name, worker_resources[i].value)}/{format_display(col_name, worker_resources[i].total)}'
        if col_name == 'devices':
            if index.parent().isValid():  # do not display hw devices on children since it's all the same
                return
            if not (worker_resources := self.__worker_hw_resources.get(worker.hwid)):
                return 'internal error'
            dev_info_parts = []
            for worker_device in worker_resources.devices:
                text = f'{worker_device.type_name}: {worker_device.name}[{"idle" if worker_device.available else "busy"}]'
                if role == Qt.ToolTipRole:
                    res_parts = []
                    for dev_res in worker_device.resources:
                        res_parts.append(f'{dev_res.name}={format_display(dev_res.name, dev_res.value)}')
                        text += f' ({",".join(res_parts)})'
                dev_info_parts.append(text)
            return '\n'.join(dev_info_parts)

        return raw_data

    def flags(self, index: QModelIndex) -> Qt.ItemFlags:
        flags = super(WorkerModel, self).flags(index)
        if self.__cols_order[index.column()] == 'groups':
            flags |= Qt.ItemIsEditable
        return flags

    def setData(self, index: QModelIndex, value: Any, role: int = Qt.DisplayRole) -> bool:
        # FOR NOW THIS ONLY SETS groups
        print(f'!setting {role}')
        if not index.isValid():
            return False
        if role not in (Qt.DisplayRole, Qt.EditRole, Qt.BackgroundRole, self.SORT_ROLE):
            return False
        row = index.row()
        col = index.column()
        col_name = self.__cols_order[col]
        if not index.parent().isValid():  # first level (hwid)
            hwid = self.__hwid_order[row]
        else:
            hwid = self.__hwid_order[index.parent().row()]
            #hwid = self.__workers[self.__order[row]].hwid
        print(f'setting for {hwid}')
        if isinstance(value, str):
            groups = [y for y in (x.strip() for x in value.split(',')) if y != '']
        elif isinstance(value, (list, tuple)):
            groups = value
        else:
            raise NotImplementedError()
        if col_name == 'groups':  # need to signal that group change was requested
            self.group_update_requested.emit(hwid, groups)
        return True

    def cancel_running_invocations(self, index: QModelIndex):
        if not index.isValid():
            return
        if not index.parent().isValid():  # first level (hwid)
            return
        row = index.row()
        wid = self.__wid_order[self.__hwid_order[index.parent().row()]][row]
        #wid = self.__workers[self.__order[row]].id
        self.cancel_invocation_for_worker.emit(wid)

    @Slot(object)
    def workers_full_update(self, workers_data: WorkerBatchData):
        if len(workers_data.workers) == 0:
            return
        # update resources and resource definitions
        any_hwres = next(iter(workers_data.resources.values()))
        # TODO: use resource definitions here
        self.__worker_resources_names = [x.name for x in any_hwres]
        self.__maybe_update_structure()

        # we filter UNKNOWNS here, currently i see no point
        # displaying them any time.
        # maybe unknowns should not be returned by update at all?
        with performance_measurer() as pm:
            new_workers: Dict[int, WorkerData] = {
                x_id: x for x_id, x in workers_data.workers.items()
                if x.state != WorkerState.UNKNOWN
            }
            new_keys = set(new_workers.keys())
            old_keys = set(self.__workers.keys())
        _perf_preinit = pm.elapsed()

        # update
        _perf_signals = 0
        with performance_measurer() as pm:
            rows_to_update = {}
            workers_changed_hwid = []
            for worker_key in old_keys.intersection(new_keys):
                for field in WorkerData.__dict__['__dataclass_fields__']:
                    value = getattr(new_workers[worker_key], field)
                    old_value = getattr(self.__workers[worker_key], field)
                    if old_value != value:
                        setattr(self.__workers[worker_key], field, value)
                        hwid = self.__workers[worker_key].hwid
                        wid = self.__workers[worker_key].id
                        hwid_row = self.__hwid_order.index(hwid)
                        wid_row = self.__wid_order[hwid].index(wid)
                        if hwid_row not in rows_to_update:
                            rows_to_update[hwid_row] = [wid_row, wid_row]
                        else:
                            rows_to_update[hwid_row][0] = min(wid_row, rows_to_update[hwid_row][0])
                            rows_to_update[hwid_row][1] = max(wid_row, rows_to_update[hwid_row][1])
                        if field == 'hwid':
                            assert value == self.__workers[worker_key].hwid
                            workers_changed_hwid.append([old_value, value, self.__workers[worker_key]])
                        if hwid in self.__hwid_summaries:  # reset summary cache
                            self.__hwid_summaries.pop(hwid)
                # since resources are separate - need to check changes there too
                hwid = self.__workers[worker_key].hwid
                res = workers_data.resources.get(hwid)
                if res != self.__worker_hw_resources.get(hwid):
                    hwid_row = self.__hwid_order.index(hwid)
                    rows_to_update[hwid_row] = [0, len(self.__wid_order[hwid])-1]  # update all children

            # actually update resource values
            self.__worker_hw_resources = workers_data.resources

            # emit update signal
            if len(rows_to_update) > 0:
                self.dataChanged.emit(self.index(min(rows_to_update), 0), self.index(max(rows_to_update), self.columnCount()-1))
                for hwid_row, (wid_row_min, wid_row_max) in rows_to_update.items():
                    parent_index = self.index(hwid_row, 0)
                    self.dataChanged.emit(self.index(wid_row_min, 0, parent_index), self.index(wid_row_max, self.columnCount(parent_index)-1, parent_index))
        _perf_update = pm.elapsed()

        # note, at this point after update there MAY be workers in incorrect hwid parent (in case wid was reassigned by scheduler, that can rarely happen)
        #  we have them in workers_changed_hwid list

        # insert new
        with performance_measurer() as pm:
            hwid_to_new_worker = {}
            for wid in new_keys - old_keys:
                worker = new_workers[wid]
                hwid_to_new_worker.setdefault(worker.hwid, []).append(worker)
            for _, hwid, worker in workers_changed_hwid:
                hwid_to_new_worker.setdefault(hwid, []).append(worker)
            # first - insert new hwids
            new_hwids = {x for x in hwid_to_new_worker if x not in self.__hwid_order}
            if len(new_hwids) > 0:
                self.beginInsertRows(QModelIndex(), self.rowCount(), self.rowCount() + len(new_hwids) - 1)
                for hwid in new_hwids:
                    self.__hwid_order.append(hwid)
                    self.__wid_order[hwid] = []
                self.endInsertRows()
            # now that all hwids exist - insert children
            for hwid, new_workers_for_hwid in hwid_to_new_worker.items():
                if len(new_workers_for_hwid) == 0:
                    continue
                if hwid in self.__hwid_summaries:  # reset summary cache
                    self.__hwid_summaries.pop(hwid)
                parent_index = self.index(self.__hwid_order.index(hwid), 0)
                self.beginInsertRows(parent_index, self.rowCount(parent_index), self.rowCount(parent_index) + len(new_workers_for_hwid) - 1)
                for worker in new_workers_for_hwid:
                    assert worker.id not in self.__workers
                    self.__workers[worker.id] = WorkerModelData.from_worker_data(worker)
                    self.__wid_order[hwid].append(worker.id)
                self.endInsertRows()

            # signal parent update
            if len(hwid_to_new_worker) > 0:
                rows_to_update = [self.__hwid_order.index(x) for x in hwid_to_new_worker]
                self.dataChanged.emit(self.index(min(rows_to_update), 0), self.index(max(rows_to_update), self.columnCount() - 1))

        _perf_insert = pm.elapsed()

        # remove
        with performance_measurer() as pm:
            hwid_to_wid_to_remove = {}
            for wid in old_keys - new_keys:
                hwid = self.__workers[wid].hwid
                hwid_to_wid_to_remove.setdefault(hwid, []).append(wid)
            for hwid, _, worker in workers_changed_hwid:
                hwid_to_wid_to_remove.setdefault(hwid, []).append(worker.id)

            hwids_to_remove = set()
            hwids_to_update = set()
            for hwid, wids_to_remove in hwid_to_wid_to_remove.items():
                if len(wids_to_remove) == 0:
                    continue
                for wid in wids_to_remove:
                    assert wid in self.__workers
                    parent_index = self.index(self.__hwid_order.index(hwid), 0)
                    self.beginRemoveRows(parent_index, self.__wid_order[hwid].index(wid), self.__wid_order[hwid].index(wid))
                    self.__workers.pop(wid)
                    self.__wid_order[hwid].remove(wid)
                    if len(self.__wid_order[hwid]) == 0:
                        hwids_to_remove.add(hwid)
                    else:
                        hwids_to_update.add(hwid)
                    self.endRemoveRows()
            for hwid in hwids_to_remove:
                assert len(self.__wid_order[hwid]) == 0
                self.beginRemoveRows(QModelIndex(), self.__hwid_order.index(hwid), self.__hwid_order.index(hwid))
                self.__hwid_order.remove(hwid)
                self.endRemoveRows()
            if len(hwids_to_update) > 0:
                rows_to_update = [self.__hwid_order.index(x) for x in hwids_to_update]
                self.dataChanged.emit(self.index(min(rows_to_update), 0), self.index(max(rows_to_update), self.columnCount() - 1))
            for hwid in hwids_to_update:
                if hwid in self.__hwid_summaries:  # reset summary cache
                    self.__hwid_summaries.pop(hwid)

        _perf_remove = pm.elapsed()

        if _perf_preinit + _perf_update + _perf_signals + _perf_insert + _perf_remove > 0.04:  # arbitrary threshold ~ 1/25 of a sec
            self.__logger.debug(f'update performed:\n'
                                f'{_perf_preinit:04f}:\tpreinit\n'
                                f'{_perf_update:04f}:\tupdate\n'
                                f'{_perf_signals:04f}:\tsignals\n'
                                f'{_perf_insert:04f}:\tinsert\n'
                                f'{_perf_remove:04f}:\tremove')

    def start(self):
        self.__scheduler_worker.workers_full_update.connect(self.workers_full_update)
        self.group_update_requested.connect(self.__scheduler_worker.set_worker_groups)
        self.cancel_invocation_for_worker.connect(self.__scheduler_worker.cancel_task_for_worker)

    def stop(self):
        self.__scheduler_worker.disconnect(self)
