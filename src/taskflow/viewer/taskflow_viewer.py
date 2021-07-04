from datetime import datetime, timezone
from PySide2.QtWidgets import *
from PySide2.QtGui import *
from PySide2.QtCore import Qt, Slot, Signal, QAbstractItemModel, QItemSelection, QModelIndex, QSortFilterProxyModel
from .nodeeditor import NodeEditor, QGraphicsImguiScene

from typing import Dict


class GroupsModel(QAbstractItemModel):
    SortRole = Qt.UserRole + 0

    def __init__(self, parent):
        super(GroupsModel, self).__init__(parent=parent)
        self.__items = {}
        self.__items_order = []

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return
        if section == 0:
            return 'name'
        elif section == 1:
            return 'creation time'

    def rowCount(self, parent: QModelIndex = None) -> int:
        if parent is None:
            parent = QModelIndex()
        if not parent.isValid():
            return len(self.__items)
        return 0

    def columnCount(self, parent: QModelIndex = None) -> int:
        return 2

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if role != Qt.DisplayRole and role != self.SortRole:
            return
        if index.column() == 0:
            return self.__items_order[index.row()]
        elif index.column() == 1:
            if role == Qt.DisplayRole:
                return datetime.fromtimestamp(self.__items[self.__items_order[index.row()]].get('ctime', 0) or 0).replace(tzinfo=timezone.utc).astimezone().strftime(r'%H:%M:%S %d %b %y')
            elif role == self.SortRole:
                return self.__items[self.__items_order[index.row()]].get('ctime', -1)

    def index(self, row: int, column: int, parent: QModelIndex = None) -> QModelIndex:
        if parent is None:
            parent = QModelIndex()
        return self.createIndex(row, column)

    def parent(self, index: QModelIndex):
        # for now it's one level model
        return QModelIndex()

    @Slot(list)
    def update_groups(self, groups: Dict[str, dict]):
        self.beginResetModel()
        self.__items = groups
        self.__items_order = sorted(list(groups.keys()), key=lambda x: self.__items[x].get('ctime', -1) or -1, reverse=True)
        self.endResetModel()


class GroupsView(QTreeView):
    selection_changed = Signal(set)
    group_pause_state_change_requested = Signal(str, bool)

    def __init__(self, parent=None):
        super(GroupsView, self).__init__(parent)
        self.setSelectionMode(GroupsView.ExtendedSelection)
        self.setSortingEnabled(True)
        self.__sorting_model = QSortFilterProxyModel(self)

    def selectionChanged(self, selected: QItemSelection, deselected: QItemSelection) -> None:
        super(GroupsView, self).selectionChanged(selected, deselected)
        self.selection_changed.emit(set(index.data() for index in self.selectedIndexes() if index.column() == 0))

    def contextMenuEvent(self, event):
        model: GroupsModel = self.model()
        if model is None:
            return
        index: QModelIndex = self.indexAt(event.pos())
        if not index.isValid():
            return

        group = index.siblingAtColumn(0).data(Qt.DisplayRole)
        event.accept()
        menu = QMenu(parent=self)
        menu.addAction('pause all tasks').triggered.connect(lambda: self.group_pause_state_change_requested.emit(group, True))
        menu.addAction('resume all tasks').triggered.connect(lambda: self.group_pause_state_change_requested.emit(group, False))
        menu.popup(event.globalPos())

    def setModel(self, model):
        self.__sorting_model.setSourceModel(model)
        self.__sorting_model.setSortRole(GroupsModel.SortRole)
        self.__sorting_model.setDynamicSortFilter(True)
        self.sortByColumn(1, Qt.DescendingOrder)
        super(GroupsView, self).setModel(self.__sorting_model)


class TaskflowViewer(QMainWindow):
    def __init__(self, db_path: str, parent=None):
        super(TaskflowViewer, self).__init__(parent)
        self.__central_widget = QSplitter()
        self.setCentralWidget(self.__central_widget)
        #self.__main_layout = QHBoxLayout(self.centralWidget())
        self.__node_editor = NodeEditor(db_path)
        self.__group_list = GroupsView()
        self.__group_list.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Minimum)

        self.__model_main = GroupsModel(self)
        self.__group_list.setModel(self.__model_main)
        self.__group_list.header().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.__group_list.header().setStretchLastSection(True)

        #self.__main_layout.addWidget(self.__group_list)
        #self.__main_layout.addWidget(self.__node_editor)
        self.__central_widget.addWidget(self.__group_list)
        self.__central_widget.addWidget(self.__node_editor)
        self.__central_widget.setSizes([1, 999999])
        self.__group_list.setFocusPolicy(Qt.ClickFocus)
        self.__node_editor.setFocusPolicy(Qt.ClickFocus)

        # cOnNeC1
        scene = self.__node_editor.scene()
        assert isinstance(scene, QGraphicsImguiScene)
        scene.task_groups_updated.connect(self.update_groups)
        self.__group_list.selection_changed.connect(scene.set_task_group_filter)
        self.__group_list.group_pause_state_change_requested.connect(scene.set_tasks_paused)

        # start
        self.__node_editor.start()

    def update_groups(self, groups):
        do_select = self.__model_main.rowCount() == 0
        self.__model_main.update_groups(groups)
        if do_select and self.__model_main.rowCount() > 0:
            self.__group_list.setCurrentIndex(self.__model_main.index(0, 0))

    def setSceneRect(self, *args, **kwargs):
        return self.__node_editor.setSceneRect(*args, **kwargs)

    def sceneRect(self):
        return self.__node_editor.sceneRect()

    def closeEvent(self, event: QCloseEvent) -> None:
        self.__node_editor.stop()
        super(TaskflowViewer, self).closeEvent(event)
