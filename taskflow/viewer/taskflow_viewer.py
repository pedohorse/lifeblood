from PySide2.QtWidgets import *
from PySide2.QtGui import *
from PySide2.QtCore import Qt, Slot, Signal, QStringListModel, QItemSelection
from .nodeeditor import NodeEditor, QGraphicsImguiScene


class GroupsModel(QStringListModel):
    def __init__(self, parent):
        super(GroupsModel, self).__init__(parent=parent)

    @Slot(list)
    def update_groups(self, groups):
        self.setStringList(list(groups))


class GroupsView(QListView):
    selection_changed = Signal(set)

    def __init__(self, parent=None):
        super(GroupsView, self).__init__(parent)
        self.setSelectionMode(GroupsView.ExtendedSelection)

    def selectionChanged(self, selected: QItemSelection, deselected: QItemSelection) -> None:
        super(GroupsView, self).selectionChanged(selected, deselected)
        self.selection_changed.emit(set(index.data() for index in self.selectedIndexes()))


class TaskflowViewer(QMainWindow):
    def __init__(self, db_path: str, parent=None):
        super(TaskflowViewer, self).__init__(parent)
        self.__central_widget = QWidget()
        self.setCentralWidget(self.__central_widget)
        self.__main_layout = QHBoxLayout(self.centralWidget())
        self.__node_editor = NodeEditor(db_path)
        self.__group_list = GroupsView()
        self.__group_list.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Minimum)

        self.__model_main = GroupsModel(self)
        self.__group_list.setModel(self.__model_main)

        self.__main_layout.addWidget(self.__group_list)
        self.__main_layout.addWidget(self.__node_editor)
        self.__group_list.setFocusPolicy(Qt.ClickFocus)
        self.__node_editor.setFocusPolicy(Qt.ClickFocus)

        # cOnNeC1
        scene = self.__node_editor.scene()
        assert isinstance(scene, QGraphicsImguiScene)
        scene.task_groups_updated.connect(self.__model_main.update_groups)
        self.__group_list.selection_changed.connect(scene.set_task_group_filter)

        # start
        self.__node_editor.start()

    def setSceneRect(self, *args, **kwargs):
        return self.__node_editor.setSceneRect(*args, **kwargs)

    def sceneRect(self):
        return self.__node_editor.sceneRect()

    def closeEvent(self, event: QCloseEvent) -> None:
        self.__node_editor.stop()
        super(TaskflowViewer, self).closeEvent(event)
