import imgui
from .ui_elements_base import ImguiWindow
from .nodeeditor import NodeEditor

from typing import Tuple


class ImguiViewWindow(ImguiWindow):
    def __init__(self, editor_widget: NodeEditor, title: str = ''):
        super().__init__(title)
        self.__editor = editor_widget

    def editor_widget(self) -> NodeEditor:
        return self.__editor

    def popup(self):
        super().popup()
        self.__editor._window_opened(self)

    def _close(self):
        super()._close()
        self.__editor._window_closed(self)


class FindNodePopup(ImguiViewWindow):
    def __init__(self, editor_widget: NodeEditor, title: str = ''):
        super().__init__(editor_widget, title)
        self.__val = ''
        self.__cached_node_sids = set()

    def default_size(self) -> Tuple[float, float]:
        return 256, 128

    def draw_window_elements(self):
        changed, val = imgui.input_text('find node', self.__val, 128)
        if self._was_just_opened():
            imgui.set_keyboard_focus_here(-1)

        if changed:
            self.__val = val
            if val:
                self.__cached_node_sids = tuple(x.get_session_id() for x in self.editor_widget().scene().find_nodes_by_name(val, match_partly=True))
            else:
                self.__cached_node_sids = set()

        if self.__val:
            pass
        imgui.text(f'found {len(self.__cached_node_sids)} nodes')

        if imgui.button('select'):
            self.editor_widget().scene().clearSelection()
            for node in (self.editor_widget().scene().get_node_by_session_id(sid) for sid in self.__cached_node_sids):
                if node is None:
                    continue
                node.setSelected(True)

        imgui.same_line()
        if imgui.button('close'):
            self._close()

    def shortcut_context_id(self) -> str:
        return f'nodeeditor.findnode.{id(self)}'

