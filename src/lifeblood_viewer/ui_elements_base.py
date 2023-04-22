import imgui

from typing import Optional, Tuple


class ImguiElement:
    def draw(self):
        """
        should be called in imgui draw loop
        """
        raise NotImplementedError()


class ImguiWindow(ImguiElement):
    def __init__(self, title: str = '', closable: bool = True):
        self.__opened = False
        self.__just_opened = False
        # wanted to use uuid.uuid4().hex below, but that would bloat imgui's internal db after multiple launches
        self.__imgui_name = f'{title}##{type(self).__name__}'
        self.__focused_last_draw: bool = False
        self.__closable = closable

    def _imgui_window_name(self):
        return self.__imgui_name

    def popup(self):
        self.__opened = True
        self.__just_opened = True

    def _close(self):
        """
        call ONLY from draw_window_elements instead of imgui.close_current_popup()
        """
        self.__opened = False

    def _was_just_opened(self) -> bool:
        """
        call ONLY from draw_window_elements
        returns True if this is the first frame when the window is drawn
        """
        return self.__just_opened

    def default_size(self) -> Optional[Tuple[float, float]]:
        """
        default size on each popup
        """
        return None

    def initial_geometry(self) -> Optional[Tuple[int, int, int, int]]:
        """
        return initial position and size of the window
        will be only applied ON FIRST EVER CREATION if returns not None
        """
        return None

    def draw(self):
        if not self.__opened:
            return

        if self.__just_opened:
            pos = imgui.get_mouse_pos()
            imgui.set_next_window_position(pos.x, pos.y, imgui.APPEARING)
            if init_geo := self.initial_geometry():
                x, y, w, h =init_geo
                imgui.set_next_window_size(w, h, imgui.FIRST_USE_EVER)
                imgui.set_next_window_position(x, y, imgui.FIRST_USE_EVER)
            if size := self.default_size():
                imgui.set_next_window_size(size[0], size[1], imgui.APPEARING)
            imgui.set_next_window_focus()

        (expanded, opened) = imgui.begin(self.__imgui_name, closable=self.__closable)
        if not opened:
            imgui.end()
            self._close()
            return
        try:
            self.__focused_last_draw = imgui.is_window_focused()
            self.draw_window_elements()
        finally:
            imgui.end()
            self.__just_opened = False

    def is_focused(self) -> bool:
        return self.__focused_last_draw

    def shortcut_context_id(self) -> str:
        """
        when this window is active - the returned shortcut context will be active
        """
        raise NotImplementedError

    def draw_window_elements(self):
        """
        override this to draw custom popup stuff without caring about
        how it's managed
        """
        raise NotImplementedError()


class ImguiPopup(ImguiWindow):
    def __init__(self, title: str = ''):
        super().__init__(title)
        self.__to_be_opened = False

    def popup(self):
        self.__to_be_opened = True

    def _close(self):
        """
        call ONLY from draw_window_elements instead of imgui.close_current_popup()
        """
        imgui.close_current_popup()

    def draw(self):
        if self.__to_be_opened:
            imgui.open_popup(self._imgui_window_name())
        self.__to_be_opened = False

        if imgui.begin_popup(self._imgui_window_name()):
            self.__focused_last_draw = imgui.is_window_focused()
            self.draw_window_elements()
            imgui.end_popup()