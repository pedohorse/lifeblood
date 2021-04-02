from PySide2.QtWidgets import *
from PySide2.QtGui import *
from PySide2.QtCore import Qt, Slot, Signal, QStringListModel
from .nodeeditor import NodeEditor


class GroupsModel(QStringListModel):
    def __init__(self, parent):
        super(GroupsModel, self).__init__(parent=parent)

    @Slot(list)
    def update_groups(self, groups):
        self.setStringList(groups)


class TaskflowViewer(QMainWindow):
    def __init__(self, db_path: str, parent=None):
        super(TaskflowViewer, self).__init__(parent)
        self.__central_widget = QWidget()
        self.setCentralWidget(self.__central_widget)
        self.__main_layout = QHBoxLayout(self.centralWidget())
        self.__node_editor = NodeEditor(db_path)
        self.__group_list = QListView()
        self.__group_list.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Minimum)

        self.__main_layout.addWidget(self.__group_list)
        self.__main_layout.addWidget(self.__node_editor)

    def setSceneRect(self, *args, **kwargs):
        return self.__node_editor.setSceneRect(*args, **kwargs)

    def sceneRect(self):
        return self.__node_editor.sceneRect()

    def closeEvent(self, event: QCloseEvent) -> None:
        self.__node_editor.finalize()
        super(TaskflowViewer, self).closeEvent(event)
