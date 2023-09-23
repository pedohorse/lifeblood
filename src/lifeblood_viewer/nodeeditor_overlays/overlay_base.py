import re
from lifeblood_viewer.graphics_scene import QGraphicsImguiScene
from PySide2.QtCore import Qt, Slot, Signal, QRectF, QPointF
from PySide2.QtWidgets import QWidget, QGraphicsView
from PySide2.QtGui import QPainter, QMouseEvent


class NodeEditorOverlayBase:
    def __init__(self, scene: QGraphicsImguiScene):
        self.__scene = scene
        self.__enabled = True

    def name(self) -> str:
        return re.sub(r'(?<=[a-z0-9])(?=[A-Z])', ' ', self.__class__.__name__)

    def scene(self) -> QGraphicsImguiScene:
        return self.__scene

    def enabled(self) -> bool:
        return self.__enabled

    def set_enabled(self, enabled: bool):
        self.__enabled = enabled

    def toggle(self):
        self.__enabled = not self.__enabled

    # wrappers
    def wrapped_handle_mouse_press(self, event: QMouseEvent) -> bool:
        if not self.enabled():
            return False
        return self.handle_mouse_press(event)

    def wrapped_draw_scene_foreground(self, painter: QPainter, rect: QRectF):
        if not self.enabled():
            return
        return self.draw_scene_foreground(painter, rect)

    def wrapped_draw_imgui_foreground(self, viewer: QGraphicsView):
        if not self.enabled():
            return
        return self.draw_imgui_foreground(viewer)

    # overridables
    def handle_mouse_press(self, event: QMouseEvent) -> bool:
        """
        :return: was event handled or no
        """
        return False

    def draw_scene_foreground(self, painter: QPainter, rect: QRectF):
        return

    def draw_imgui_foreground(self, viewer: QGraphicsView):
        return

