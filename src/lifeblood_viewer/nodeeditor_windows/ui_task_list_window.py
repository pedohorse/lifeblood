import imgui

from lifeblood.enums import TaskState
from lifeblood_viewer.nodeeditor import NodeEditor
from lifeblood_viewer.ui_scene_elements import ImguiViewWindow
from ..graphics_items import Node

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
            with imgui.begin_table(f'tasks##{base_name}', 3, imgui.TABLE_SIZING_STRETCH_PROP |
                                                             imgui.TABLE_BORDERS_INNER_VERTICAL |
                                                             imgui.TABLE_ROW_BACKGROUND
                                   ) as table:
                if table.opened:
                    imgui.table_setup_column('ID', imgui.TABLE_COLUMN_DEFAULT_SORT)
                    imgui.table_setup_column('name')
                    imgui.table_setup_column('state', imgui.TABLE_COLUMN_WIDTH_FIXED, 128.0)
                    imgui.table_headers_row()

                    imgui.table_next_row()
                    imgui.table_next_column()

                    for task in self.__displayed_node.tasks_iter():
                        if task.isSelected():
                            imgui.table_set_background_color(imgui.TABLE_BACKGROUND_TARGET_ROW_BG1, 2155896928)
                        else:
                            imgui.table_set_background_color(imgui.TABLE_BACKGROUND_TARGET_ROW_BG1, 0)

                        if imgui.selectable(str(task.get_id()), False, imgui.SELECTABLE_SPAN_ALL_COLUMNS)[0]:
                            task.set_selected(True)
                        imgui.table_next_column()

                        for val in (task.name(),):
                            imgui.text(str(val))
                            imgui.table_next_column()

                        if task.state() == TaskState.IN_PROGRESS:
                            imgui.push_item_width(-1)
                            imgui.progress_bar((task.get_progress() or 0) / 100, (0, 0), f'{task.get_progress() or 0}%')
                        else:
                            imgui.text(task.state().name)
                        imgui.table_next_row()
                        imgui.table_next_column()

    def initial_geometry(self):
        return 512, 512, 550, 300

    def shortcut_context_id(self):
        return None
