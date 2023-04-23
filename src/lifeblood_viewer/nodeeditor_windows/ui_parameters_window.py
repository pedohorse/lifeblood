from lifeblood_viewer.nodeeditor import NodeEditor
from lifeblood_viewer.ui_scene_elements import ImguiViewWindow
from ..graphics_items import NetworkItemWithUI


class ParametersWindow(ImguiViewWindow):
    def __init__(self, editor_widget: NodeEditor):
        super().__init__(editor_widget, 'Parameters')

    def draw_window_elements(self):
        iitem = self.scene().get_inspected_item()
        if iitem and isinstance(iitem, NetworkItemWithUI):
            iitem.draw_imgui_elements(self.editor_widget())

    def initial_geometry(self):
        return 1065, 48, 561, 697

    def shortcut_context_id(self):
        return None