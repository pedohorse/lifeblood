import imgui
from .ui_elements_base import ImguiWindow, ImguiPopup
from .nodeeditor import NodeEditor
from .graphics_scene import QGraphicsImguiScene

from typing import Optional, Tuple


class ImguiViewWindow(ImguiWindow):
    def __init__(self, editor_widget: NodeEditor, title: str = '', closable: bool = True):
        super().__init__(title, closable)
        self.__editor = editor_widget

    def editor_widget(self) -> NodeEditor:
        return self.__editor

    def scene(self) -> QGraphicsImguiScene:
        return self.__editor.scene()

    def popup(self):
        super().popup()
        self.__editor._window_opened(self)

    def _close(self):
        super()._close()
        self.__editor._window_closed(self)


class ImguiViewPopup(ImguiPopup):
    def __init__(self, editor_widget: NodeEditor, title: str = ''):
        super().__init__(title)
        self.__editor = editor_widget

    def editor_widget(self) -> NodeEditor:
        return self.__editor

    def scene(self) -> QGraphicsImguiScene:
        return self.__editor.scene()

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
        self.__i = 0
        self.__cached_node_sids = set()

    def default_size(self) -> Tuple[float, float]:
        return 256, 128

    def draw_window_elements(self):
        changed, val = imgui.input_text('find node', self.__val, 128, flags=imgui.INPUT_TEXT_ENTER_RETURNS_TRUE)
        if self._was_just_opened():
            imgui.set_keyboard_focus_here(-1)

        do_select = False
        if changed:
            do_select = True
            self.__i = 0
            self.__val = val
            if val:
                self.__cached_node_sids = tuple(x.get_session_id() for x in self.editor_widget().scene().find_nodes_by_name(val, match_partly=True))
            else:
                self.__cached_node_sids = set()

        if self.__val:
            pass
        imgui.text(f'found {len(self.__cached_node_sids)} nodes')

        if imgui.button('next'):
            do_select = True
            self.__i += 1
        imgui.same_line()
        if imgui.button('prev'):
            do_select = True
            self.__i -= 1

        if do_select:
            self.__cached_node_sids = [x for x in self.__cached_node_sids if self.editor_widget().scene().get_node_by_session_id(x) is not None]
            if len(self.__cached_node_sids) > 0:
                self.editor_widget().scene().clearSelection()
                self.__i %= len(self.__cached_node_sids)
                node = self.editor_widget().scene().get_node_by_session_id(self.__cached_node_sids[self.__i])

                node.setSelected(True)
                self.editor_widget().focus_on_selected()

        imgui.same_line()
        if imgui.button('select all'):
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

