import imgui
from lifeblood_viewer.nodeeditor import NodeEditor
from lifeblood_viewer.ui_scene_elements import ImguiViewWindow


class UndoWindow(ImguiViewWindow):
    def __init__(self, editor_widget: NodeEditor):
        super().__init__(editor_widget, 'Undo History')

    def draw_window_elements(self):
        for name in reversed(self.scene().undo_stack_names()):
            if len(name) > 40:
                name = name[:40] + '...'
            imgui.bullet_text(name)
            if not imgui.is_item_visible():
                break

    def initial_geometry(self):
        return 32, 48, 256, 300

    def shortcut_context_id(self):
        return None