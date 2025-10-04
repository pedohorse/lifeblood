from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import QWidget
from .code_editor.editor import StringParameterEditor
from .scene_data_controller import SceneDataController
from lifeblood.enums import InvocationState
from lifeblood.ui_protocol_data import InvocationLogData

from typing import Optional, Tuple


def fetch_and_open_log_viewer(scene_data_controller: SceneDataController, invoc_id: int, parent_widget: QWidget, *, update_interval: Optional[float] = None):
    if update_interval is None:
        scene_data_controller.fetch_log_run_callback(invoc_id, _open_log_viewer, parent_widget)
    else:
        scene_data_controller.fetch_log_run_callback(invoc_id, _open_log_viewer_with_update, (parent_widget, update_interval, invoc_id, scene_data_controller))


def _open_log_viewer(log:  InvocationLogData, parent: QWidget):
    hl = StringParameterEditor.SyntaxHighlight.LOG
    wgt = StringParameterEditor(syntax_highlight=hl, parent=parent)
    wgt.setAttribute(Qt.WA_DeleteOnClose, True)
    wgt.set_text(log.stdout)
    wgt.set_readonly(True)
    wgt.set_title(f'Log: task {log.task_id}, invocation {log.invocation_id}')
    wgt.show()


def _open_log_viewer_with_update(log, callback_data: Tuple[QWidget, float, int, SceneDataController]):
    parent, update_interval, invoc_id, scene_data_controller = callback_data

    hl = StringParameterEditor.SyntaxHighlight.LOG
    wgt = StringParameterEditor(syntax_highlight=hl, parent=parent)
    wgt.setAttribute(Qt.WA_DeleteOnClose, True)
    wgt.set_readonly(True)
    wgt.set_title(f'Live Log: task {log.task_id}, invocation {log.invocation_id}')

    update_timer = QTimer(wgt)
    update_timer.setInterval(int(update_interval * 1000))
    update_timer.setSingleShot(True)  # we will restart timer every time log is received, since that func is async

    # there is time between log request and log fetch - IF widget is closed and destroyed at that time - we get an error
    #  that internal C++ qt object was destroyed, unless we make appropriate checks
    def _on_log_fetched(new_log: InvocationLogData, _):
        if wgt.is_closed():
            # do nothing as widget is closed, and it's c++ part destroyed
            return
        wgt.set_text(new_log.stdout, stick_to_bottom=True)
        if new_log.invocation_state != InvocationState.FINISHED:
            update_timer.start()  # restart timer

    update_timer.timeout.connect(
        lambda: scene_data_controller.fetch_log_run_callback(
            invoc_id,
            _on_log_fetched
        )
    )
    wgt._update_timer = update_timer  # we need to keep reference, or pyside will delete underlying qt object

    wgt.show()
    update_timer.start()

    wgt.set_text(log.stdout, stick_to_bottom=True)
