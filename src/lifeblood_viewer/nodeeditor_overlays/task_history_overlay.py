from datetime import timedelta
from lifeblood.logging import get_logger
from lifeblood.ui_protocol_data import InvocationLogData
from lifeblood_viewer.code_editor.editor import StringParameterEditor
from lifeblood_viewer.long_op import LongOperation, LongOperationData
from lifeblood_viewer.graphics_scene import QGraphicsImguiScene
from lifeblood_viewer.graphics_items import Task

from PySide2.QtCore import Qt, Slot, Signal, QRectF, QPointF
from PySide2.QtWidgets import QWidget, QGraphicsView
from PySide2.QtGui import QPainter, QPainterPath, QPen, QColor, QMouseEvent
import imgui

from .overlay_base import NodeEditorOverlayBase


class TaskHistoryOverlay(NodeEditorOverlayBase):
    logger = get_logger('viewer.task_history_overlay')

    def __init__(self, scene: QGraphicsImguiScene):
        super().__init__(scene)
        self.__scene = scene
        self.__pen_line = QPen(QColor(192, 192, 192, 96), 3)
        self.__pen_line.setStyle(Qt.DashDotLine)
        self.__tangent = QPointF(0, -100)
        self.__pen_mark = QPen(QColor(192, 192, 192, 192), 3)
        self.__node_offset = QPointF(-10, 0)
        self.__dot_start_offset = QPointF(-10, 15)
        node_mark_offset = QPointF(-15, 0)
        node_mark_shrink = 10

        self.__node_mark_offset_top = node_mark_offset + QPointF(0, node_mark_shrink)
        self.__node_mark_offset_bottom = node_mark_offset - QPointF(0, node_mark_shrink)

        self.__highlighted_task = None
        self.__buttons = None

    def draw_scene_foreground(self, painter: QPainter, rect: QRectF):
        """
        called by drawForeground to draw all qt elements, NOT imgui ones
        """
        sel = self.__scene.selectedItems()
        if len(sel) and isinstance(sel[0], Task):
            self.__highlighted_task = sel[0]
        else:
            self.__highlighted_task = None
        if self.__highlighted_task is not None and self.__scene is not self.__highlighted_task.scene():
            self.__highlighted_task = None
            self.__buttons = None
        if self.__highlighted_task is None:
            return

        task: Task = self.__highlighted_task
        pos = task.scenePos()
        path = QPainterPath()
        mark_path = QPainterPath()

        size = 24
        path.addEllipse(pos.x() - 0.5 * size, pos.y() - 0.5 * size, size, size)
        path.moveTo(pos.x(), pos.y() - 0.5 * size)
        dot_counts = {}
        already_visited_nodes = set()
        task_node_id = task.node().get_id()
        self.__buttons = {}

        for i, (inv_id, node_id, log_meta) in enumerate(reversed(task.invocation_logs())):
            if node_id not in already_visited_nodes:
                already_visited_nodes.add(node_id)
                pos = self.__scene.get_node(node_id).scenePos()
                bbox = self.__scene.get_node(node_id).boundingRect()
                target_pos = pos + bbox.bottomLeft() + self.__node_offset
                if not rect.contains(target_pos) and not rect.contains(path.currentPosition()):
                    path.moveTo(bbox.topLeft() + pos + self.__node_offset)
                    continue
                if i != 0 or node_id != task_node_id:
                    path.cubicTo(path.currentPosition() + self.__tangent,
                                 target_pos - self.__tangent,
                                 target_pos)
                    path.lineTo(bbox.topLeft() + pos + self.__node_offset)
                mark_path.moveTo(bbox.bottomLeft() + pos + self.__node_mark_offset_bottom)
                mark_path.lineTo(bbox.topLeft() + pos + self.__node_mark_offset_top)

            dot_i = dot_counts.setdefault(node_id, 0)
            dot_counts[node_id] += 1
            dot_start = bbox.topLeft() + pos + self.__node_mark_offset_top + self.__dot_start_offset
            # painter.drawRect(QRectF(dot_start + QPointF(-5, -5), dot_start + QPointF(5, 5)))

            if rect.contains(dot_start):
                buttons = self.__buttons.setdefault(node_id, (dot_start, bbox.height(), []))
                buttons[2].append((inv_id, log_meta))

        painter.setPen(self.__pen_line)
        painter.drawPath(path)
        painter.setPen(self.__pen_mark)
        painter.drawPath(mark_path)

    def draw_imgui_foreground(self, viewer: QGraphicsView):
        if self.__highlighted_task is None or self.__buttons is None:
            return

        for node_id, (pos, height, logs) in self.__buttons.items():
            screen_pos = viewer.mapFromScene(pos)
            screen_height = abs(viewer.mapFromScene(0, height).y() - viewer.mapFromScene(0, 0).y())
            texts = []
            for invoc_id, log_meta in logs:

                invoc_good = log_meta.return_code == 0

                status = "ok" if invoc_good \
                         else ("..." if log_meta.return_code is None else f'fail({log_meta.return_code})')
                runtime_text = str(timedelta(seconds=round(log_meta.invocation_runtime))) if log_meta.invocation_runtime is not None else "N/A"
                status_text = f'{status}:{runtime_text}'
                texts.append((invoc_id, invoc_good, status_text))

            # status_text_height = 0
            status_text_width = 0
            for _, _, text in texts:
                estimated_size = imgui.calc_text_size(text)
                status_text_width = max(status_text_width, estimated_size[0] + 12)  # 16 for scrollbar
                # status_text_height += estimated_size[1]
            window_width = 50 + status_text_width

            imgui.push_style_var(imgui.STYLE_WINDOW_PADDING, (6.0, 6.0))
            imgui.set_next_window_position(screen_pos.x() - window_width, screen_pos.y() - 15)
            #imgui.set_next_window_size(window_width, 12 + status_text_height + 4 * len(texts))
            imgui.set_next_window_size_constraints((window_width, 30),
                                                   (window_width, max(30, screen_height)))
            imgui.begin(f"smth##forground_interface_{node_id}", False,
                        imgui.WINDOW_NO_MOVE | imgui.WINDOW_NO_TITLE_BAR |
                        imgui.WINDOW_NO_COLLAPSE | imgui.WINDOW_NO_RESIZE |
                        imgui.WINDOW_NO_SAVED_SETTINGS | imgui.WINDOW_NO_FOCUS_ON_APPEARING |
                        imgui.WINDOW_NO_NAV_FOCUS | imgui.WINDOW_NO_BRING_TO_FRONT_ON_FOCUS |
                        imgui.WINDOW_ALWAYS_AUTO_RESIZE)

            for invoc_id, invoc_good, status_text in texts:
                if imgui.button(f'log##forground_interface_log_{invoc_id}'):
                    self.__scene.fetch_and_open_log(invoc_id, self._open_log_viewer, viewer)
                imgui.same_line()
                clr = (0.55, 0.9, 0.55) if invoc_good else (0.9, 0.55, 0.55)
                imgui.push_style_color(imgui.COLOR_TEXT, *clr)
                imgui.text(status_text)
                imgui.pop_style_color()
            imgui.end()
            imgui.pop_style_var()

    def _open_log_viewer(self, log, parent):
        hl = StringParameterEditor.SyntaxHighlight.LOG
        wgt = StringParameterEditor(syntax_highlight=hl, parent=parent)
        wgt.set_text(log.stdout)
        wgt.set_readonly(True)
        wgt.set_title(f'Log: task {log.task_id}, invocation {log.invocation_id}')
        wgt.show()
