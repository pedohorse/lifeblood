import imgui
from lifeblood_viewer.nodeeditor import NodeEditor
from lifeblood_viewer.ui_scene_elements import ImguiViewWindow


class LongOpWindow(ImguiViewWindow):
    def __init__(self, editor_widget: NodeEditor):
        super().__init__(editor_widget, 'Operations')

    def draw_window_elements(self):
        ops, queues = self.scene().long_operation_statuses()
        for opid, (fraction, status) in ops:
            imgui.progress_bar(fraction or 1.0, overlay=f'{opid}:{status or ""}')
        imgui.separator()
        imgui.text('queues:')
        if queues:
            for qname, qval in queues.items():
                imgui.text(f'  {qval}: {qname}')
        else:
            imgui.text('none')

    def initial_geometry(self):
        return 32, 548, 256, 150

    def shortcut_context_id(self):
        return None
