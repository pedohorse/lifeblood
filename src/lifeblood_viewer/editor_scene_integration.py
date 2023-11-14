from PySide2.QtCore import Qt, QTimer
from PySide2.QtWidgets import QWidget
from .code_editor.editor import StringParameterEditor

from typing import Optional, TYPE_CHECKING
if TYPE_CHECKING:
    from .graphics_scene import QGraphicsImguiScene


def fetch_and_open_log_viewer(scene: "QGraphicsImguiScene", invoc_id: int, parent_widget: QWidget, *, update_interval: Optional[float] = None):
    if update_interval is None:
        scene.fetch_log_run_callback(invoc_id, _open_log_viewer, parent_widget)
    else:
        scene.fetch_log_run_callback(invoc_id, _open_log_viewer_with_update, (parent_widget, update_interval, invoc_id, scene))


def _open_log_viewer(log, parent):
    hl = StringParameterEditor.SyntaxHighlight.LOG
    wgt = StringParameterEditor(syntax_highlight=hl, parent=parent)
    wgt.setAttribute(Qt.WA_DeleteOnClose, True)
    wgt.set_text(log.stdout)
    wgt.set_readonly(True)
    wgt.set_title(f'Log: task {log.task_id}, invocation {log.invocation_id}')
    wgt.show()


def _open_log_viewer_with_update(log, callback_data):
    parent, update_interval, invoc_id, scene = callback_data

    hl = StringParameterEditor.SyntaxHighlight.LOG
    wgt = StringParameterEditor(syntax_highlight=hl, parent=parent)
    wgt.setAttribute(Qt.WA_DeleteOnClose, True)
    wgt.set_readonly(True)
    wgt.set_title(f'Live Log: task {log.task_id}, invocation {log.invocation_id}')

    update_timer = QTimer(wgt)
    update_timer.setInterval(int(update_interval * 1000))
    update_timer.setSingleShot(True)  # we will restart timer every time log is received, since that func is async
    update_timer.timeout.connect(
        lambda: scene.fetch_log_run_callback(
            invoc_id,
            lambda new_log, _:
                (wgt.set_text(new_log.stdout, stick_to_bottom=True),
                 update_timer.start(),
                 print('DEBUG ME, TIMER IS OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOn'))
        )
    )
    wgt._update_timer = update_timer  # we need to keep reference, or pyside will delete underlying qt object

    wgt.show()
    update_timer.start()

    wgt.set_text(log.stdout, stick_to_bottom=True)
