import imgui

from lifeblood_viewer.nodeeditor import NodeEditor
from lifeblood_viewer.ui_scene_elements import ImguiViewWindow
from ..graphics_items import Node


class TaskListWindow(ImguiViewWindow):
    def __init__(self, editor_widget: NodeEditor):
        super().__init__(editor_widget, 'Task List')
        self.__displayed_node = None

    def draw_window_elements(self):
        iitem = self.scene().get_inspected_item()
        if iitem and isinstance(iitem, Node):
            self.__displayed_node = iitem

        if self.__displayed_node is not None:
            imgui.text(f'node: {self.__displayed_node.node_name()}')
            base_name = self._imgui_window_name()
            imgui.columns(3, f'tasks##{base_name}')
            imgui.separator()
            for col in ('ID', 'name', 'state'):
                imgui.text(col)
                imgui.next_column()
            imgui.separator()
            imgui.set_column_offset(1, 32)
            imgui.set_column_offset(2, 450)

            for task in self.__displayed_node.tasks_iter():
                for val in (task.get_id(), task.name(), task.state().name):
                    imgui.text(str(val))
                    imgui.next_column()
            imgui.columns(1)

    def initial_geometry(self):
        return 512, 512, 450, 300

    def shortcut_context_id(self):
        return None
