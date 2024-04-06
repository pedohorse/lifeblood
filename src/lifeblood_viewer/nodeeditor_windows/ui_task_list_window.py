import imgui

from lifeblood.enums import TaskState
from lifeblood_viewer.nodeeditor import NodeEditor
from lifeblood_viewer.ui_scene_elements import ImguiViewWindow
from ..graphics_items import Node
from PySide2.QtCore import QPoint
from PySide2.QtGui import QCursor

from typing import Optional

class TaskListWindow(ImguiViewWindow):
    def __init__(self, editor_widget: NodeEditor):
        super().__init__(editor_widget, 'Task List')
        self.__displayed_node: Optional[Node] = None
        self.__pinned = False

    def set_display_node(self, display_node: Node):
        self.__displayed_node = display_node
        self._update_title()

    def pin(self, pin: bool = True):
        self.__pinned = pin
        self._update_title()

    def _update_title(self):
        self.set_title(f'Task List "{self.__displayed_node.node_name() if self.__displayed_node else ""}"'
                       f'{" pinned" if self.__pinned else ""}')

    def draw_window_elements(self):
        iitem = self.scene().get_inspected_item()
        if iitem and isinstance(iitem, Node) and not self.__pinned:
            self.set_display_node(iitem)

        if self.__displayed_node is not None:
            imgui.text(f'node: {self.__displayed_node.node_name()}')
            base_name = f'table_{self._imgui_key_name()}'
            with imgui.begin_table(f'tasks##{base_name}', 4, imgui.TABLE_SIZING_STRETCH_PROP |
                                                             imgui.TABLE_BORDERS_INNER_VERTICAL |
                                                             imgui.TABLE_ROW_BACKGROUND
                                   ) as table:
                if table.opened:
                    imgui.table_setup_column('ID', imgui.TABLE_COLUMN_DEFAULT_SORT)
                    imgui.table_setup_column('name')
                    imgui.table_setup_column('paused', imgui.TABLE_COLUMN_WIDTH_FIXED, 64)
                    imgui.table_setup_column('state', imgui.TABLE_COLUMN_WIDTH_FIXED, 128.0)
                    imgui.table_headers_row()

                    imgui.table_next_row()
                    imgui.table_next_column()

                    prev_task = None
                    select_next_task = False
                    task_to_reselect = None
                    for task in self.__displayed_node.tasks_iter(order=self.__displayed_node.TaskSortOrder.ID):
                        if task.isSelected():
                            imgui.table_set_background_color(imgui.TABLE_BACKGROUND_TARGET_ROW_BG1, 2155896928)
                            if imgui.is_window_focused():
                                if imgui.is_key_pressed(imgui.KEY_UP_ARROW, False):
                                    task_to_reselect = prev_task
                                elif imgui.is_key_pressed(imgui.KEY_DOWN_ARROW, False):
                                    select_next_task = True
                        else:
                            imgui.table_set_background_color(imgui.TABLE_BACKGROUND_TARGET_ROW_BG1, 0)
                            if select_next_task:
                                select_next_task = False
                                task_to_reselect = task

                        if imgui.selectable(str(task.get_id()), False, imgui.SELECTABLE_SPAN_ALL_COLUMNS)[0]:
                            task.set_selected(True)
                        if imgui.is_item_hovered() and imgui.is_mouse_clicked(imgui.BUTTON_MOUSE_BUTTON_RIGHT):
                            self.editor_widget().show_task_menu(task, pos=QCursor.pos())

                        imgui.table_next_column()

                        imgui.text(str(task.name()))
                        imgui.table_next_column()

                        if task.paused():
                            imgui.text('paused')
                        imgui.table_next_column()

                        if task.state() == TaskState.IN_PROGRESS:
                            imgui.push_item_width(-1)
                            imgui.progress_bar((task.get_progress() or 0) / 100, (0, 0), f'{task.get_progress() or 0}%')
                        else:
                            imgui.text(task.state().name)
                        imgui.table_next_row()
                        imgui.table_next_column()
                        prev_task = task

                    # if keys were pressed during frame drawing
                    if task_to_reselect is not None:
                        task_to_reselect.set_selected(True)

    def initial_geometry(self):
        return 512, 512, 550, 300

    def shortcut_context_id(self):
        return None
