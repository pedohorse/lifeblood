import os
import pathlib
from datetime import datetime, timezone, timedelta
from PySide6.QtWidgets import *
from PySide6.QtGui import *
from PySide6.QtCore import Qt, Slot, Signal, QAbstractItemModel, QItemSelection, QModelIndex, QSortFilterProxyModel, QItemSelectionModel, QThread, QTimer
from lifeblood.config import get_config
from lifeblood.enums import TaskGroupArchivedState
from lifeblood.ui_protocol_data import TaskGroupBatchData, TaskGroupData
from lifeblood import paths
from lifeblood.logging import get_logger
from .nodeeditor import NodeEditor
from .graphics_scene_with_data_controller import QGraphicsImguiSceneWithDataController
from .connection_worker import SchedulerConnectionWorker
from .ui_scene_elements import FindNodePopup
from .menu_entry_base import MainMenuLocation
from .nodeeditor_windows.ui_create_node_popup import CreateNodePopup
from .nodeeditor_windows.ui_undo_window import UndoWindow
from .nodeeditor_windows.ui_longop_window import LongOpWindow
from .nodeeditor_windows.ui_parameters_window import ParametersWindow
from .nodeeditor_windows.ui_task_list_window import TaskListWindow
from .widgets.worker_list import WorkerListWidget
from .nodeeditor_overlays.task_history_overlay import TaskHistoryOverlay
from .task_group_actions import TaskGroupViewerAction, TaskGroupViewerActionPerformerBase, ActionTypeNotSupported, TaskGroupViewerActionRegistry
from .task_group_action_performers.noop_action_performer import NoopViewerActionPerformer
from .task_group_action_performers.submit_action_performer import SubmitViewerActionPerformer
from .task_group_actions_impl.submit_action import TaskGroupViewerSubmitAction
from .task_group_actions_impl.noop_action import TaskGroupViewerNoopAction

from typing import Dict, List, Optional, Tuple

mem_debug = 'LIFEBLOOD_VIEWER_MEM_DEBUG' in os.environ

if mem_debug:
    import tracemalloc
    tracemalloc.start()


def confirm_operation_gui(parent: QWidget, opname):
    res = QMessageBox.warning(parent, 'confirm action', f'confirm {opname}', QMessageBox.StandardButton.Ok | QMessageBox.StandardButton.Cancel, QMessageBox.StandardButton.Cancel)
    return res == QMessageBox.StandardButton.Ok


class GroupsModel(QAbstractItemModel):
    SortRole = Qt.ItemDataRole.UserRole + 0

    __set_group_priority_signal = Signal(str, float)

    GROUP_NAME_COL = 0
    CREATION_TIME_COL = 1
    START_TIME_COL = 2
    END_TIME_COL = 3
    TOTAL_RUNTIME_COL = 4
    PRIORITY_COL = 5
    SUMMARY_COL = 6
    COL_COUNT = 7

    def __init__(self, parent, connection_worker: SchedulerConnectionWorker):
        # TODO: this should work with data controller abstraction instead of connection_worker
        #  cuz connection worker is not even an implementation of data controller,
        #  but it's an implementation detail.
        super(GroupsModel, self).__init__(parent=parent)
        self.__items: Dict[str, TaskGroupData] = {}
        self.__items_order = []
        self.__connection_worker = connection_worker

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.ItemDataRole.DisplayRole):
        if role != Qt.ItemDataRole.DisplayRole:
            return
        if section == self.GROUP_NAME_COL:
            return 'group name'
        elif section == self.CREATION_TIME_COL:
            return 'creation time'
        elif section == self.START_TIME_COL:
            return 'start time'
        elif section == self.END_TIME_COL:
            return 'end time'
        elif section == self.TOTAL_RUNTIME_COL:
            return 'total runtime'
        elif section == self.PRIORITY_COL:
            return 'priority'
        elif section == self.SUMMARY_COL:
            return 'summary'

    def rowCount(self, parent: QModelIndex = None) -> int:
        if parent is None:
            parent = QModelIndex()
        if not parent.isValid():
            return len(self.__items)
        return 0

    def columnCount(self, parent: QModelIndex = None) -> int:
        return self.COL_COUNT

    def is_archived(self, index) -> bool:
        return self.__items[self.__items_order[index.row()]].state == TaskGroupArchivedState.ARCHIVED

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole):
        if role == Qt.ItemDataRole.ForegroundRole:
            archived = self.is_archived(index)
            if archived:
                return QColor.fromRgbF(0.5, 0.5, 0.5)
            item = self.__items[self.__items_order[index.row()]]
            done_count = item.statistics.tasks_done
            prog_count = item.statistics.tasks_in_progress
            err_count = item.statistics.tasks_with_error
            total_count = item.statistics.tasks_total
            if done_count == total_count:
                return QColor.fromRgbF(0.65, 1.0, 0.65)
            elif err_count > 0:
                return QColor.fromRgbF(1.0, 0.6, 0.6)
            elif prog_count > 0:
                return QColor.fromRgbF(1.0, 0.9, 0.65)
        if role != Qt.ItemDataRole.DisplayRole and role != self.SortRole:
            return None
        if index.column() == self.GROUP_NAME_COL:  # name
            return self.__items_order[index.row()]
        elif index.column() == self.CREATION_TIME_COL:  # creation time
            if role == Qt.ItemDataRole.DisplayRole:
                return datetime.fromtimestamp(self.__items[self.__items_order[index.row()]].creation_timestamp).replace(tzinfo=timezone.utc).astimezone().strftime(r'%H:%M:%S %d %b %y')
            elif role == self.SortRole:
                return self.__items[self.__items_order[index.row()]].creation_timestamp
        elif index.column() == self.START_TIME_COL:  # start time
            val = self.__items[self.__items_order[index.row()]].statistics.first_start
            if role == Qt.DisplayRole:
                return datetime.utcfromtimestamp(val).strftime(r'%H:%M:%S %d %b %y') if val is not None else 'N/A'
            elif role == self.SortRole:
                return val or 0
        elif index.column() == self.END_TIME_COL:  # end time
            val = self.__items[self.__items_order[index.row()]].statistics.last_finish
            if role == Qt.DisplayRole:
                return datetime.utcfromtimestamp(val).strftime(r'%H:%M:%S %d %b %y') if val is not None else 'N/A'
            elif role == self.SortRole:
                return val or 0
        elif index.column() == self.TOTAL_RUNTIME_COL:  # total runtime
            val = self.__items[self.__items_order[index.row()]].statistics.total_runtime
            if role == Qt.DisplayRole:
                return str(timedelta(seconds=val)) if val is not None else 'N/A'
            elif role == self.SortRole:
                return val or 0
        elif index.column() == self.PRIORITY_COL:  # priority
            return self.__items[self.__items_order[index.row()]].priority
        elif index.column() == self.SUMMARY_COL:  # completion progress
            item = self.__items[self.__items_order[index.row()]]
            return f"{item.statistics.tasks_in_progress}:{item.statistics.tasks_with_error}:{item.statistics.tasks_done}/{item.statistics.tasks_total}"

    def flags(self, index) -> Qt.ItemFlags:
        flags = super().flags(index)
        if index.column() == self.PRIORITY_COL:  # priority
            flags |= Qt.ItemIsEditable
        return flags

    def setData(self, index: QModelIndex, value, role: int = Qt.EditRole):
        if role != Qt.EditRole:
            return False
        col = index.column()
        if col == self.PRIORITY_COL:
            task_group = self.__items[self.__items_order[index.row()]]
            task_group.priority = float(value)
            self.__set_group_priority_signal.emit(task_group.name, task_group.priority)
            return True
        else:
            return False

    def index(self, row: int, column: int, parent: QModelIndex = None) -> QModelIndex:
        if parent is None:
            parent = QModelIndex()
        return self.createIndex(row, column)

    def parent(self, index: QModelIndex):
        # for now it's one level model
        return QModelIndex()

    @Slot(list)
    def update_groups(self, groups: TaskGroupBatchData):

        orig_row_count = len(self.__items_order)

        # 1. remove removed
        for inv_row, group_name in enumerate(reversed(self.__items_order[:])):
            row = orig_row_count - 1 - inv_row
            if group_name in groups.task_groups:
                continue
            self.beginRemoveRows(QModelIndex(), row, row)
            self.__items_order.pop(row)
            self.__items.pop(group_name)
            self.endRemoveRows()

        # 2. add new
        for group_name, group_data in groups.task_groups.items():
            if group_name in self.__items:
                continue
            self.beginInsertRows(QModelIndex(), len(self.__items_order), len(self.__items_order))
            self.__items[group_name] = group_data
            self.__items_order.append(group_name)
            self.endInsertRows()

        # 3. update existing
        for row, group_name in enumerate(self.__items_order):
            if group_data := groups.task_groups.get(group_name):
                min_col = self.columnCount()
                max_col = -1
                existing_group = self.__items[group_name]
                for col in range(self.columnCount()):
                    if (
                            col == self.GROUP_NAME_COL and existing_group.name != group_data.name
                            or col == self.CREATION_TIME_COL and existing_group.creation_timestamp != group_data.creation_timestamp
                            or col == self.START_TIME_COL and existing_group.statistics.first_start != group_data.statistics.first_start
                            or col == self.END_TIME_COL and existing_group.statistics.last_finish != group_data.statistics.last_finish
                            or col == self.TOTAL_RUNTIME_COL and existing_group.statistics.total_runtime != group_data.statistics.total_runtime
                            or col == self.PRIORITY_COL and existing_group.priority != group_data.priority
                            or col == self.SUMMARY_COL and existing_group.statistics != group_data.statistics
                    ):
                        min_col = min(min_col, col)
                        max_col = max(max_col, col)
                if max_col < min_col:  # nothing changed
                    continue
                self.__items[group_name] = group_data
                self.dataChanged.emit(self.index(row, min_col), self.index(row, max_col))

    def start(self):
        self.__connection_worker.groups_full_update.connect(self.update_groups)
        self.__set_group_priority_signal.connect(self.__connection_worker.set_task_group_priority)

    def stop(self):
        self.__connection_worker.disconnect(self)


class GroupsView(QTreeView):
    selection_changed = Signal(set)
    group_pause_state_change_requested = Signal(list, bool)
    task_group_archived_state_change_requested = Signal(list, TaskGroupArchivedState)
    task_group_delete_requested = Signal(list)
    task_group_actions_requested = Signal(str)
    task_group_action_perform_requested = Signal(str, TaskGroupViewerAction)

    def __init__(self, parent=None):
        super(GroupsView, self).__init__(parent)
        self.setSelectionMode(GroupsView.SelectionMode.ExtendedSelection)
        self.setSortingEnabled(True)
        self.__sorting_model = QSortFilterProxyModel(self)
        self.__stashed_selection = None
        self.__block_selection_signals = False
        self.__last_active_actions_submenu: Optional[Tuple[QMenu, str]] = None

    def selectionChanged(self, selected: QItemSelection, deselected: QItemSelection) -> None:
        super(GroupsView, self).selectionChanged(selected, deselected)
        if not self.__block_selection_signals:
            self.selection_changed.emit(set(index.data(Qt.ItemDataRole.DisplayRole) for index in self.selectedIndexes() if index.column() == 0))

    def contextMenuEvent(self, event):
        model: QSortFilterProxyModel = self.model()
        if model is None:
            return
        index: QModelIndex = self.indexAt(event.pos())
        if not index.isValid():
            return

        if len(self.selectedIndexes()) == 0:
            groups = [index.siblingAtColumn(0).data(Qt.ItemDataRole.DisplayRole)]
        else:
            groups = list({x.siblingAtColumn(0).data(Qt.ItemDataRole.DisplayRole) for x in self.selectedIndexes()})
        event.accept()
        menu = QMenu(parent=self)

        if len(groups) == 1:
            # only show actions for a single group, cuz it's simpler for now
            actions_submenu = menu.addMenu('actions')
            actions_submenu.addAction('...loading...')
            menu.addSeparator()
            self.task_group_actions_requested.emit(groups[0])
            self.__last_active_actions_submenu = (actions_submenu, groups[0])

        menu.addAction('pause all tasks').triggered.connect(lambda: self.group_pause_state_change_requested.emit(groups, True))
        menu.addAction('resume all tasks').triggered.connect(lambda: self.group_pause_state_change_requested.emit(groups, False))
        menu.addSeparator()
        if model.sourceModel().is_archived(index):
            menu.addAction('restore').triggered.connect(
                lambda: confirm_operation_gui(
                    self,
                    f'restoration of groups: {", ".join(x for x in groups)}'
                ) and self.task_group_archived_state_change_requested.emit(
                    groups,
                    TaskGroupArchivedState.NOT_ARCHIVED
                )
            )
        else:
            menu.addAction('archive').triggered.connect(
                lambda: confirm_operation_gui(
                    self,
                    f'archivation of groups: {", ".join(x for x in groups)}'
                ) and self.task_group_archived_state_change_requested.emit(
                    groups,
                    TaskGroupArchivedState.ARCHIVED
                )
            )
        menu.addSeparator()
        menu.addAction('delete').triggered.connect(
            lambda: confirm_operation_gui(
                self,
                f'permanent deletion of groups: {", ".join(x for x in groups)}'
            ) and self.task_group_delete_requested.emit(
                groups,
            ))

        def _menu_cleanup():
            menu.deleteLater()
            self.__last_active_actions_submenu = None

        menu.aboutToHide.connect(_menu_cleanup)
        menu.popup(event.globalPos())

    def set_current_index_from_main_model(self, index: QModelIndex):
        self.setCurrentIndex(self.__sorting_model.mapFromSource(index))

    def set_main_model(self, model: QAbstractItemModel):
        if self.model():
            self.model().modelAboutToBeReset.disconnect(self._pre_model_reset)
            self.model().modelReset.disconnect(self._post_model_reset)
        self.__sorting_model.setSourceModel(model)
        self.__sorting_model.setSortRole(GroupsModel.SortRole)
        self.__sorting_model.setDynamicSortFilter(True)
        self.sortByColumn(1, Qt.SortOrder.DescendingOrder)
        self.setModel(self.__sorting_model)
        model.modelAboutToBeReset.connect(self._pre_model_reset)
        model.modelReset.connect(self._post_model_reset)

        # some visual adjustment
        header = self.header()
        header.moveSection(GroupsModel.PRIORITY_COL, 0)
        header.moveSection(GroupsModel.SUMMARY_COL, 2)
        # header.setSectionResizeMode(3, QHeaderView.ResizeToContents)  # this cause incredible lag with QSplitter
        header.resizeSection(GroupsModel.GROUP_NAME_COL, 200)
        header.resizeSection(GroupsModel.CREATION_TIME_COL, 128)
        header.resizeSection(GroupsModel.PRIORITY_COL, 32)
        header.resizeSection(GroupsModel.SUMMARY_COL, 80)
        # header.setSectionResizeMode(3, QHeaderView.Fixed)
        # header.resizeSection(3, 16)

    @Slot(str, object)
    def task_group_actions_updated(self, task_group_name: str, user_data_actions: Dict[str, TaskGroupViewerAction]):
        # should be connected to something that returns updated action lists
        if self.__last_active_actions_submenu is None:
            return

        submenu, submenu_task_group_name = self.__last_active_actions_submenu
        if submenu_task_group_name != task_group_name:
            # not ours, probably from last menu call
            return

        submenu.clear()
        for action_name, action_data in user_data_actions.items():
            submenu.addAction(
                action_name,
                lambda group_name=task_group_name, adata=action_data:
                    self.task_group_action_perform_requested.emit(group_name, adata)
            )
        if not user_data_actions:
            submenu.addAction('No actions available').setEnabled(False)

    @Slot()
    def _pre_model_reset(self):
        self.__stashed_selection = set(x.data(Qt.ItemDataRole.DisplayRole) for x in self.selectedIndexes() if x.column() == GroupsModel.GROUP_NAME_COL)

    @Slot()
    def _post_model_reset(self):
        if self.__stashed_selection is None:
            return
        model = self.model()
        selmodel = self.selectionModel()

        _prev_blocked = self.__block_selection_signals
        self.__block_selection_signals = True
        try:
            for i in range(model.rowCount(QModelIndex())):
                idx = model.index(i, GroupsModel.GROUP_NAME_COL)
                if idx.data(Qt.ItemDataRole.DisplayRole) in self.__stashed_selection:
                    selmodel.select(idx, QItemSelectionModel.SelectionFlag.Select | QItemSelectionModel.SelectionFlag.Rows)
        finally:
            self.__block_selection_signals = _prev_blocked

        # emit signal IF sel changed
        new_selection = set(x.data(Qt.ItemDataRole.DisplayRole) for x in self.selectedIndexes() if x.column() == GroupsModel.GROUP_NAME_COL)
        if self.__stashed_selection != new_selection:
            self.selection_changed.emit(new_selection)

        self.__stashed_selection = None


class LifebloodViewer(QMainWindow):
    def __init__(self, db_path: str = None, parent=None):
        super(LifebloodViewer, self).__init__(parent)
        self.__logger = get_logger('viewer')

        # icon
        self.setWindowIcon(QIcon(str(pathlib.Path(__file__).parent/'icons'/'lifeblood.svg')))
        self.setWindowTitle('Lifeblood Viewer')
        self.__do_select = False

        if db_path is None:
            db_path = paths.config_path('node_viewer.db', 'viewer')

        # worker thread
        self.__ui_connection_thread = QThread(self)  # SchedulerConnectionThread(self)
        self.__ui_connection_worker = SchedulerConnectionWorker()
        self.__ui_connection_worker.moveToThread(self.__ui_connection_thread)

        self.__ui_connection_thread.started.connect(self.__ui_connection_worker.start)
        self.__ui_connection_thread.finished.connect(self.__ui_connection_worker.finish)

        # interface
        self.__central_widget = QSplitter()
        self.setCentralWidget(self.__central_widget)
        self.__workerview_splitter = QSplitter(Qt.Orientation.Vertical)
        #self.__main_layout = QHBoxLayout(self.centralWidget())
        self.__node_editor = NodeEditor(db_path, self.__ui_connection_worker)
        self.__group_list = GroupsView()
        self.__group_list.setSizePolicy(QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Minimum)
        self.__overlay_connection_message = QLabel(self)  # no layout for this one
        font = self.__overlay_connection_message.font()
        font.setPixelSize(18)
        self.__overlay_connection_message.setFont(font)
        # main menu
        mbar: QMenuBar = self.menuBar()
        main_menu = mbar.addMenu('main')
        main_menu.addAction('Quit').triggered.connect(self.close)
        view_menu = mbar.addMenu('view')
        act: QAction = view_menu.addAction('show dead tasks')
        act.setCheckable(True)
        show_dead = get_config('viewer').get_option_noasync('viewer.nodeeditor.display_dead_tasks', self.__node_editor.dead_shown())
        self.set_dead_shown(show_dead)
        act.setChecked(show_dead)
        act.toggled.connect(self.set_dead_shown)

        # initialize node editor
        find_node_window = FindNodePopup(self.__node_editor, 'Find Node')
        create_node_popup = CreateNodePopup(self.__node_editor)
        undo_window = UndoWindow(self.__node_editor)
        parameters_window = ParametersWindow(self.__node_editor)
        task_list_window = TaskListWindow(self.__node_editor)
        op_status_window = LongOpWindow(self.__node_editor)

        def _task_list_for_node(ne=self.__node_editor):
            tlist = TaskListWindow(ne)
            nodes = ne.selected_nodes()
            if len(nodes) == 0:
                return
            tlist.set_display_node(nodes[0])
            tlist.pin()
            tlist.popup()

        self.__node_editor.add_overlay(TaskHistoryOverlay(self.__node_editor.scene()))

        self.__node_editor.add_action('nodeeditor.undo_history', lambda: undo_window.popup(), 'Ctrl+u', MainMenuLocation(('Edit',), 'Undo Stack'), insert_menu_after_label='Undo')
        self.__node_editor.add_action('nodeeditor.find_node', lambda: find_node_window.popup(), 'Ctrl+f', MainMenuLocation(('Nodes',), 'Find Node'))
        self.__node_editor.add_action('nodeeditor.parameters', lambda: parameters_window.popup(), 'Ctrl+p', MainMenuLocation(('Nodes',), 'Parameters'))
        self.__node_editor.add_action('nodeeditor.task_list', lambda: task_list_window.popup(), 'Ctrl+t', MainMenuLocation(('Windows',), 'Task List'))
        self.__node_editor.add_action('nodeeditor.op_status', lambda: op_status_window.popup(), 'Ctrl+r', MainMenuLocation(('Windows',), 'Operations'))
        self.__node_editor.add_action('nodeeditor.task_list_for_selected_node',
                                      _task_list_for_node,
                                      None, None)  # TODO: implement context menu fillings here too
        self.__node_editor.add_action('nodeeditor.create_node', lambda: create_node_popup.popup(), 'Tab', MainMenuLocation(('Nodes',), 'Create'))
        self.__node_editor.add_action(
            'nodeeditor.snap_nodes',
            lambda: self.__node_editor.scene().set_node_snapping_enabled(not self.__node_editor.scene().node_snapping_enabled()),
            None,
            MainMenuLocation(('Edit',), lambda: f'{"[x]" if self.__node_editor.scene().node_snapping_enabled() else "[ ]"} Snap Nodes')
        )
        undo_window.popup()
        op_status_window.popup()
        parameters_window.popup()

        act: QAction = view_menu.addAction('show archived groups')
        act.setCheckable(True)
        self.__node_editor.set_archived_groups_shown(False)
        act.setChecked(False)
        act.toggled.connect(self.__node_editor.set_archived_groups_shown)

        self.__model_main = GroupsModel(self, self.__ui_connection_worker)
        self.__group_list.set_main_model(self.__model_main)
        self.__group_list.header().setStretchLastSection(True)

        self.__worker_list = WorkerListWidget(self.__ui_connection_worker, self)

        #self.__main_layout.addWidget(self.__group_list)
        #self.__main_layout.addWidget(self.__node_editor)
        self.__central_widget.addWidget(self.__group_list)
        self.__central_widget.addWidget(self.__workerview_splitter)

        self.__workerview_splitter.addWidget(self.__node_editor)
        self.__workerview_splitter.addWidget(self.__worker_list)

        self.__central_widget.setSizes([1, 999999])
        self.__workerview_splitter.setSizes([999999, 1])

        self.__central_widget.setFocusPolicy(Qt.FocusPolicy.ClickFocus)
        self.__workerview_splitter.setFocusPolicy(Qt.FocusPolicy.ClickFocus)
        self.__group_list.setFocusPolicy(Qt.FocusPolicy.ClickFocus)
        self.__node_editor.setFocusPolicy(Qt.FocusPolicy.ClickFocus)
        self.__worker_list.setFocusPolicy(Qt.FocusPolicy.ClickFocus)

        # TODO: Now that lifeblood_viewer owns connection worker - we may reconnect these in a more straight way...
        scene = self.__node_editor.scene()
        assert isinstance(scene, QGraphicsImguiSceneWithDataController)

        # action types
        self.__viewer_action_registry = TaskGroupViewerActionRegistry()
        self.__viewer_action_registry.register_action_type(('submit',), TaskGroupViewerSubmitAction)
        self.__viewer_action_registry.register_action_type(('noop',), TaskGroupViewerNoopAction)

        # action performers
        self.__viewer_action_performers: List[TaskGroupViewerActionPerformerBase] = []

        # init specific action performers
        self.__viewer_action_performers.extend(
            [
                NoopViewerActionPerformer(),
                SubmitViewerActionPerformer('testcreator', scene, scene, self),
            ]
        )

        # cOnNeC1
        self.__model_main.modelAboutToBeReset.connect(self._pre_groups_update)
        self.__model_main.modelReset.connect(self._post_groups_update)
        self.__ui_connection_worker.scheduler_connection_lost.connect(self._show_connection_message)
        self.__ui_connection_worker.scheduler_connection_established.connect(self._hide_connection_message)
        self.__group_list.selection_changed.connect(scene.set_task_group_filter)
        self.__group_list.group_pause_state_change_requested.connect(scene.set_tasks_paused)
        self.__group_list.task_group_archived_state_change_requested.connect(scene.set_task_group_archived_state)
        self.__group_list.task_group_delete_requested.connect(scene.delete_task_groups)
        self.__group_list.task_group_actions_requested.connect(scene.request_task_group_user_data)
        scene.task_group_user_data_fetched.connect(self._task_group_user_data_updated)
        self.__group_list.task_group_action_perform_requested.connect(self._task_group_action_perform)

        if mem_debug:
            self.__tracemalloc_timer = QTimer(self)
            self.__tracemalloc_timer.setInterval(60000)
            self.__tracemalloc_timer.timeout.connect(self._tmlc_print)
            self.__tracemalloc_timer.start()

        # start
        self.start()

    def _task_group_user_data_updated(self, task_group: str, success: bool, user_data: Optional[bytes]):
        if not success:
            self.__group_list.task_group_actions_updated(task_group, {})
            return

        try:
            actions = self.__viewer_action_registry.from_user_data(user_data)
        except Exception as e:
            self.__logger.warning(f'task group user data does not contain actions I can understand: {str(e)}')
            self.__group_list.task_group_actions_updated(task_group, {})
        else:
            self.__group_list.task_group_actions_updated(task_group, actions)

    def _task_group_action_perform(self, task_group, action: TaskGroupViewerAction):
        self.__logger.debug(f'preforming viewer action "{action}" for group "{task_group}"')
        for performer in self.__viewer_action_performers:
            if not performer.is_action_supported(action):
                continue
            try:
                performer.perform_action(action)
            except ActionTypeNotSupported:
                self.__logger.warning(f'action performer "{performer}" failed to perform a supported action "{action}", skipping')
                continue
            except Exception as e:
                self.__logger.exception('unexpected error performing viewer action')
                continue
            break

    def resizeEvent(self, event):
        self.__layout_overlay_items()
        return super().resizeEvent(event)

    @Slot()
    def _show_connection_message(self):
        self.__overlay_connection_message.setText('disconnected. trying to reconnect...')
        self.__overlay_connection_message.resize(self.__overlay_connection_message.sizeHint())
        self.__layout_overlay_items()
        self.__overlay_connection_message.show()

    def __layout_overlay_items(self):
        self.__overlay_connection_message.move(self.width() // 2 - self.__overlay_connection_message.width() // 2, self.height() * 1 // 6)

    @Slot()
    def _hide_connection_message(self):
        self.__overlay_connection_message.hide()

    if mem_debug:
        def _tmlc_print(self):
            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics('lineno')
            print('\n\n[ Top 10 MEM USERS]\n{}\n\n'.format("\n".join(str(stat) for stat in top_stats[:10])))

    def set_dead_shown(self, show):
        get_config('viewer').set_option_noasync('viewer.nodeeditor.display_dead_tasks', show)
        self.__node_editor.set_dead_shown(show)

    def _pre_groups_update(self):
        self.__do_select = self.__model_main.rowCount() == 0

    def _post_groups_update(self):
        if self.__do_select and self.__model_main.rowCount() > 0:
            self.__group_list.set_current_index_from_main_model(self.__model_main.index(0, 0))

    def setSceneRect(self, *args, **kwargs):
        return self.__node_editor.setSceneRect(*args, **kwargs)

    def sceneRect(self):
        return self.__node_editor.sceneRect()

    def closeEvent(self, event: QCloseEvent) -> None:
        self.stop()
        super(LifebloodViewer, self).closeEvent(event)

    def start(self):
        self.__node_editor.start()
        self.__ui_connection_thread.start()
        self.__model_main.start()

    def stop(self):
        self.__node_editor.stop()
        self.__worker_list.stop()
        self.__model_main.stop()
        self.__ui_connection_worker.request_interruption()
        self.__ui_connection_thread.exit()
        self.__ui_connection_thread.wait()
