from pathlib import Path
from itertools import chain, repeat
from types import MappingProxyType
from enum import Enum
from .graphics_items import Task, Node, NodeConnection, NetworkItem, NetworkItemWithUI
from .graphics_scene import QGraphicsImguiScene
from .long_op import LongOperation
from .flashy_label import FlashyLabel
from .ui_snippets import UiNodeSnippetData
from lifeblood.misc import timeit, performance_measurer
from lifeblood.enums import TaskState, NodeParameterType, TaskGroupArchivedState
from lifeblood.config import get_config
from lifeblood import logging
from lifeblood import paths
from lifeblood.net_classes import NodeTypeMetadata
from lifeblood.taskspawn import NewTask
from lifeblood.invocationjob import InvocationJob
from lifeblood.snippets import NodeSnippetData, NodeSnippetDataPlaceholder
from lifeblood.environment_resolver import EnvironmentResolverArguments

from lifeblood.text import generate_name

import PySide2.QtCore
import PySide2.QtGui
from PySide2.QtWidgets import *
from PySide2.QtCore import QObject, Qt, Slot, Signal, QThread, QRectF, QPointF, QEvent, QSize
from PySide2.QtGui import QKeyEvent, QSurfaceFormat, QPainter, QTransform, QKeySequence, QCursor, QShortcutEvent

from .dialogs import MessageWithSelectableText
from .create_task_dialog import CreateTaskDialog
from .save_node_settings_dialog import SaveNodeSettingsDialog

import imgui
from imgui.integrations.opengl import ProgrammablePipelineRenderer

from typing import Optional, List, Mapping, Tuple, Dict, Set, Callable, Generator, Iterable, Union, Any

logger = logging.get_logger('viewer')


def call_later(callable, *args, **kwargs):
    if len(args) == 0 and len(kwargs) == 0:
        PySide2.QtCore.QTimer.singleShot(0, callable)
    else:
        PySide2.QtCore.QTimer.singleShot(0, lambda: callable(*args, **kwargs))


class QOpenGLWidgetWithSomeShit(QOpenGLWidget):
    def __init__(self, *args, **kwargs):
        super(QOpenGLWidgetWithSomeShit, self).__init__(*args, **kwargs)
        fmt = QSurfaceFormat()
        fmt.setSamples(4)
        self.setFormat(fmt)

    def initializeGL(self) -> None:
        super(QOpenGLWidgetWithSomeShit, self).initializeGL()
        logger.debug('init')


class Clipboard:
    class ClipboardContentsType(Enum):
        NOTHING = 0
        NODES = 1

    def __init__(self):
        self.__contents: Dict[Clipboard.ClipboardContentsType, Tuple[int, Any]] = {}
        self.__copy_operation_id = 0

    def set_contents(self, ctype: ClipboardContentsType, contents: Any):
        self.__contents[ctype] = (self.__copy_operation_id, contents)

    def contents(self, ctype: ClipboardContentsType) -> Tuple[Optional[int], Optional[Any]]:
        return self.__contents.get(ctype, (None, None))


class Shortcutable:
    def __init__(self, config_name):
        assert isinstance(self, QObject)
        self.__shortcuts: Dict[str, QShortcut] = {}
        self.__shortcut_contexts: Dict[str, Set[str]] = {}
        config = get_config(config_name)
        defaults = self.default_shortcuts()
        self.__context_name = 'main'

        for action, meth in self.shortcutable_methods().items():
            shortcut = config.get_option_noasync(f'shortcuts.{action}', defaults.get(action, None))
            if shortcut is None:
                continue
            self.__shortcuts[action] = QShortcut(QKeySequence(shortcut), self, shortcutContext=Qt.WidgetShortcut)
            self.__shortcut_contexts[action] = {'main'}  # TODO: make a way to define shortcut context per shortcut or per action, dunno
            self.__shortcuts[action].activated.connect(meth)

    def change_shortcut_context(self, new_context_name: str) -> None:
        self.disable_shortcuts()
        self.__context_name = new_context_name
        self.enable_shortcuts()

    def reset_shortcut_context(self) -> None:
        return self.change_shortcut_context('main')

    def current_shortcut_context(self) -> str:
        return self.__context_name

    def shortcuts(self):
        return MappingProxyType(self.__shortcuts)

    def shortcutable_methods(self) -> Dict[str, Callable]:
        return {}

    def default_shortcuts(self) -> Dict[str, str]:
        return {}

    def disable_shortcuts(self):
        """
        disable shortcuts for current context

        :return:
        """
        for action, shortcut in self.__shortcuts.items():
            if self.__context_name in self.__shortcut_contexts.get(action, {}):
                shortcut.setEnabled(False)

    def enable_shortcuts(self):
        """
        enable shortcuts for current context

        :return:
        """
        for action, shortcut in self.__shortcuts.items():
            if self.__context_name in self.__shortcut_contexts.get(action, {}):
                shortcut.setEnabled(True)


class NodeEditor(QGraphicsView, Shortcutable):
    def __init__(self, db_path: str = None, worker=None, parent=None):
        super(NodeEditor, self).__init__(parent=parent)
        # PySide's QWidget does not call super, so we call explicitly
        Shortcutable.__init__(self, 'viewer')

        self.__overlay_message = FlashyLabel(parent=self)

        self.__oglwidget = QOpenGLWidgetWithSomeShit()
        self.setViewport(self.__oglwidget)
        self.setRenderHints(QPainter.Antialiasing | QPainter.SmoothPixmapTransform)
        self.setMouseTracking(True)
        self.setDragMode(self.RubberBandDrag)

        self.setViewportUpdateMode(QGraphicsView.FullViewportUpdate)
        self.setCacheMode(QGraphicsView.CacheBackground)
        self.__view_scale = 0.0

        self.__ui_panning_lastpos = None

        self.__ui_focused_item = None

        self.__scene = QGraphicsImguiScene(db_path, worker)
        self.setScene(self.__scene)
        #self.__update_timer = PySide2.QtCore.QTimer(self)
        #self.__update_timer.timeout.connect(lambda: self.__scene.invalidate(layers=QGraphicsScene.ForegroundLayer))
        #self.__update_timer.setInterval(50)
        #self.__update_timer.start()
        self.__editor_clipboard = Clipboard()

        #self.__shortcut_layout = QShortcut(QKeySequence('ctrl+l'), self)
        #self.__shortcut_layout.activated.connect(self.layout_selected_nodes)

        self.__create_menu_popup_toopen = False
        self.__node_type_input = ''
        self.__menu_popup_selection_id = 0
        self.__menu_popup_selection_name = ''
        self.__menu_popup_arrow_down = False
        self.__node_types: Dict[str, NodeTypeMetadata] = {}
        self.__viewer_presets: Dict[str, NodeSnippetData] = {}  # viewer side presets
        self.__scheduler_presets: Dict[str, Dict[str, Union[NodeSnippetData, NodeSnippetDataPlaceholder]]] = {}  # scheduler side presets

        self.__preset_scan_paths: List[Path] = [paths.config_path('presets', 'viewer')]

        # connec
        self.__scene.nodetypes_updated.connect(self._nodetypes_updated)
        self.__scene.nodepresets_updated.connect(self._nodepresets_updated)
        self.__scene.task_invocation_job_fetched.connect(self._popup_show_invocation_info)
        self.__scene.operation_progress_updated.connect(self._scene_operation_progress_updated)

        self.__scene.request_node_types_update()

        self.__imgui_input_blocked = False

        self.__imgui_init = False
        self.__imgui_config_path = get_config('viewer').get_option_noasync('imgui.ini_file', str(paths.config_path('imgui.ini', 'viewer'))).encode('UTF-8')
        self.rescan_presets()
        self.update()

    def shortcutable_methods(self):
        return {'nodeeditor.layout_graph': self.layout_selected_nodes,
                'nodeeditor.copy': self.copy_selected_nodes,
                'nodeeditor.paste': self.paste_copied_nodes,
                'nodeeditor.focus_selected': self.focus_on_selected,
                'nodeeditor.undo': self.undo,
                'nodeeditor.delete': self.delete_selected}

    def default_shortcuts(self) -> Dict[str, str]:
        return {'nodeeditor.layout_graph': 'Ctrl+l',
                'nodeeditor.copy': 'Ctrl+c',
                'nodeeditor.paste': 'Ctrl+v',
                'nodeeditor.focus_selected': 'f',
                'nodeeditor.undo': 'Ctrl+z',
                'nodeeditor.delete': 'delete'}

    def show_message(self, message: str, duration: float):
        self.__overlay_message.show_label(message[:100], duration)
        self.__overlay_message.move(self.width()//2 - self.__overlay_message.width()//2, self.height()*5//6)
        self.__overlay_message.raise_()

    def rescan_presets(self):
        self.__viewer_presets = {}
        for preset_base_path in self.__preset_scan_paths:
            if not preset_base_path.exists():
                logger.debug(f'skipped non-existing preset scan path: {preset_base_path}')
                continue
            for preset_path in preset_base_path.iterdir():
                with open(preset_path, 'rb') as f:
                    snippet = NodeSnippetData.deserialize(f.read())
                if not snippet.label:  # skip presets with bad label
                    logger.debug(f'skipped preset: {preset_path}')
                    continue
                self.__viewer_presets[snippet.label] = snippet
                logger.info(f'loaded preset: {snippet.label}')

    #
    # get/set settings
    #
    def dead_shown(self) -> bool:
        return not self.__scene.skip_dead()

    @Slot(bool)
    def set_dead_shown(self, show: bool):
        self.__scene.set_skip_dead(not show)
        self.__scene.request_graph_and_tasks_update()

    def archived_groups_shown(self) -> bool:
        return not self.__scene.skip_archived_groups()

    @Slot(bool)
    def set_archived_groups_shown(self, show: bool):
        self.__scene.set_skip_archived_groups(not show)
        self.__scene.request_task_groups_update()

    #
    # Actions
    #
    @Slot()
    def layout_selected_nodes(self):
        nodes = [n for n in self.__scene.selectedItems() if isinstance(n, Node)]
        if not nodes:
            return
        self.__scene.layout_nodes(nodes, center=self.sceneRect().center())
        self.show_message('Nodes auto aligned', 2)

    @Slot()
    def copy_selected_nodes(self):
        """
        we save a structure that remembers all selected nodes' names, types and all parameters' values
        and all connections
        later on "paste" event these will be used to create all new nodes

        :return:
        """
        snippet = UiNodeSnippetData.from_viewer_nodes([x for x in self.__scene.selectedItems() if isinstance(x, Node)])
        for node in snippet.nodes_data:
            node.name += ' copy'
        self.__editor_clipboard.set_contents(Clipboard.ClipboardContentsType.NODES, snippet)
        self.show_message('Nodes copied', 2)

    @Slot()
    def preset_from_selected_nodes(self, preset_label: Optional[str] = None, file_path: Optional[str] = None):
        """
        saves selected nodes as a preset
        if path where preset is saved is one of preset scan paths - the preset will be loaded

        :param file_path: where to save. if None - file dialog will be displayed
        :param preset_label: label for the preset. if None - dialog will be displayed

        :return:
        """
        if preset_label is None:
            preset_label, good = QInputDialog.getText(self, 'pick a label for this preset', 'label:', QLineEdit.Normal)
            if not good:
                return

        snippet = UiNodeSnippetData.from_viewer_nodes([x for x in self.__scene.selectedItems() if isinstance(x, Node)], preset_label)

        user_presets_path = paths.config_path('presets', 'viewer')
        if file_path is None:
            if not user_presets_path.exists():
                user_presets_path.mkdir(parents=True, exist_ok=True)
            file_path, _ = QFileDialog.getSaveFileName(self, 'save preset', str(user_presets_path), 'node presets (*.lbp)')
        if not file_path:
            return
        with open(file_path, 'wb') as f:
            f.write(snippet.serialize(ascii=True))
        if Path(file_path).parent in self.__preset_scan_paths:
            self.__viewer_presets[preset_label] = snippet

    @Slot(QPointF)
    def paste_copied_nodes(self, pos: Optional[QPointF] = None):
        if pos is None:
            pos = self.mapToScene(self.mapFromGlobal(QCursor.pos()))
        clipdata = self.__editor_clipboard.contents(self.__editor_clipboard.ClipboardContentsType.NODES)
        if clipdata is None:
            return
        self.__scene.nodes_from_snippet(clipdata[1], pos)
        self.show_message('Nodes pasted', 2)

    @Slot()
    def undo(self):
        for op in self.__scene.undo():
            self.show_message(f'undo: {op}', 1)

    @Slot()
    def delete_selected(self):
        self.__scene.delete_selected_nodes()

    @Slot(str, str, QPointF)
    def get_snippet_from_scheduler_and_create_nodes(self, package: str, preset_name: str, pos: QPointF):
        def todoop(longop):
            self.__scene.request_node_preset(package, preset_name, longop.new_op_data())
            _, _, snippet = yield
            self.__scheduler_presets.setdefault(package, {})[preset_name] = snippet

            self.__scene.nodes_from_snippet(snippet, pos)

        self.__scene.add_long_operation(todoop)

    @Slot(QPointF)
    def duplicate_selected_nodes(self, pos: QPointF):
        contents = self.__scene.selectedItems()
        if not contents:
            return
        node_ids = []
        avg_old_pos = QPointF()
        for item in contents:
            if not isinstance(item, Node):
                continue
            node_ids.append(item.get_id())
            avg_old_pos += item.pos()
        if len(node_ids) == 0:
            return
        avg_old_pos /= len(node_ids)
        print(node_ids, pos, avg_old_pos)
        self.__scene.request_duplicate_nodes(node_ids, pos - avg_old_pos)

    @Slot()
    def focus_on_selected(self):
        if self.__ui_panning_lastpos:  # if we are panning right now
            return
        numitems = len(self.__scene.selectedItems())
        if numitems == 0:
            center = self.__scene.itemsBoundingRect().center()
        else:
            center = QPointF()
            for item in self.__scene.selectedItems():
                center += item.mapToScene(item.boundingRect().center())
            center /= numitems

        rect = self.sceneRect()
        rect.setSize(QSize(1, 1))
        rect.moveCenter(center)
        self.setSceneRect(rect)
        #self.setSceneRect(rect.translated(*((self.__ui_panning_lastpos - event.screenPos()) * (2 ** self.__view_scale)).toTuple()))
    #
    #
    def show_task_menu(self, task):
        menu = QMenu(self)
        menu.addAction(f'task {task.get_id()}').setEnabled(False)
        menu.addSeparator()
        menu.addAction(f'{task.state().name}').setEnabled(False)
        if task.state_details() is None:
            menu.addAction('no state message').setEnabled(False)
        else:
            menu.addAction('state message').triggered.connect(lambda _=False, x=task: self.show_task_details(x))
        menu.addAction('-paused-' if task.paused() else 'active').setEnabled(False)

        menu.addAction('show invocation info').triggered.connect(lambda ckeched=False, x=task.get_id(): self.__scene.request_invocation_job(x))

        menu.addSeparator()
        menu.addAction('change attribute').triggered.connect(lambda checked=False, x=task: self._update_attribs_and_popup_modify_task_widget(x))
        menu.addSeparator()

        if task.paused():
            menu.addAction('resume').triggered.connect(lambda checked=False, x=task.get_id(): self.__scene.set_tasks_paused([x], False))
        else:
            menu.addAction('pause').triggered.connect(lambda checked=False, x=task.get_id(): self.__scene.set_tasks_paused([x], True))

        if task.state() == TaskState.IN_PROGRESS:
            menu.addAction('cancel').triggered.connect(lambda checked=False, x=task.get_id(): self.__scene.request_task_cancel(x))
        state_submenu = menu.addMenu('force state')
        for state in TaskState:
            if state in (TaskState.GENERATING, TaskState.INVOKING, TaskState.IN_PROGRESS, TaskState.POST_GENERATING):
                continue
            state_submenu.addAction(state.name).triggered.connect(lambda checked=False, x=task.get_id(), state=state: self.__scene.set_task_state([x], state))

        pos = self.mapToGlobal(self.mapFromScene(task.scenePos()))
        menu.aboutToHide.connect(menu.deleteLater)
        menu.popup(pos)

    def show_task_details(self, task: Task):
        details = task.state_details()
        if details is None:
            return
        dialog = MessageWithSelectableText(details.get('message', ''), parent=self)
        dialog.show()

    def show_node_menu(self, node: Node, pos=None):
        menu = QMenu(self)
        menu.addAction(f'node {node.node_name()}').setEnabled(False)
        menu.addSeparator()
        menu.addAction('rename').triggered.connect(lambda checked=False, x=node: self._popup_node_rename_widget(x))
        menu.addSeparator()
        settings_names = self.__scene.node_types()[node.node_type()].settings_names
        settings_menu = menu.addMenu('apply settings >')
        settings_menu.setEnabled(len(settings_names) > 0)
        for name in settings_names:
            settings_menu.addAction(name).triggered.connect(lambda checked=False, x=node, sett=name: x.apply_settings(sett))
        settings_actions_menu = menu.addMenu('modify settings >')
        settings_actions_menu.addAction('save settings').triggered.connect(lambda checked=False, x=node: self._popup_save_settings_dialog(x))
        settings_defaults_menu = settings_actions_menu.addMenu('set defaults')
        for name in (None, *settings_names):
            settings_defaults_menu.addAction(name or '<unset>').triggered.connect(lambda checked=False, x=node, sett=name: self._popup_set_settings_default(node, sett))

        menu.addSeparator()
        menu.addAction('pause all tasks').triggered.connect(node.pause_all_tasks)
        menu.addAction('resume all tasks').triggered.connect(node.resume_all_tasks)
        menu.addSeparator()

        if len(self.__scene.selectedItems()) > 0:
            menu.addAction(f'layout selected nodes ({self.shortcuts()["nodeeditor.layout_graph"].key().toString()})').triggered.connect(self.layout_selected_nodes)
            menu.addSeparator()

        menu.addAction('create new task').triggered.connect(lambda checked=False, x=node: self._popup_create_task(x))

        menu.addSeparator()
        del_submenu = menu.addMenu('extra')

        def _action(checked=False, nid=node.get_id()):
            self.__scene.request_wipe_node(nid)
            node = self.__scene.get_node(nid)
            if node is not None:
                node.setSelected(False)

        del_submenu.addAction('reset node to default state').triggered.connect(_action)

        if pos is None:
            pos = self.mapToGlobal(self.mapFromScene(node.mapToScene(node.boundingRect().topRight())))
        menu.aboutToHide.connect(menu.deleteLater)
        menu.popup(pos)

    def show_general_menu(self, pos):
        menu = QMenu(self)
        menu.addAction(f'layout selected nodes ({self.shortcuts()["nodeeditor.layout_graph"].key().toString()})').triggered.connect(self.layout_selected_nodes)
        menu.addAction('duplicate selected nodes here').triggered.connect(lambda c=False, p=self.mapToScene(self.mapFromGlobal(pos)): self.duplicate_selected_nodes(p))
        menu.addSeparator()
        menu.addAction('copy selected').triggered.connect(self.copy_selected_nodes)
        menu.addAction('paste').triggered.connect(lambda c=False, p=self.mapToScene(self.mapFromGlobal(pos)): self.paste_copied_nodes(p))
        menu.addSeparator()
        menu.addAction('save preset').triggered.connect(self.preset_from_selected_nodes)
        menu.aboutToHide.connect(menu.deleteLater)
        menu.popup(pos)

    def _popup_node_rename_widget(self, node: Node):
        assert node.scene() == self.__scene
        lpos = self.mapFromScene(node.mapToScene(node.boundingRect().topLeft()))
        wgt = QLineEdit(self)
        wgt.setMinimumWidth(256)  # TODO: user-befriend this shit
        wgt.move(lpos)
        self.__imgui_input_blocked = True
        wgt.editingFinished.connect(lambda i=node.get_id(), w=wgt: self.__scene.rename_node(i, w.text()))
        wgt.editingFinished.connect(wgt.deleteLater)
        wgt.editingFinished.connect(lambda: PySide2.QtCore.QTimer.singleShot(0, self.__unblock_imgui_input))  # polish trick to make this be called after current events are processed, events where keypress might be that we need to skip

        wgt.textChanged.connect(lambda x: logger.debug(f'sh {self.sizeHint()}'))
        wgt.setText(node.node_name())
        wgt.show()
        wgt.setFocus()

    def _popup_create_task_callback(self, node_id: int, wgt: CreateTaskDialog):
        new_task = NewTask(wgt.get_task_name(), node_id, task_attributes=wgt.get_task_attributes())
        new_task.add_extra_group_names(wgt.get_task_groups())
        res_name, res_args = wgt.get_task_environment_resolver_and_arguments()
        new_task.set_environment_resolver(res_name, res_args)
        self.__scene.request_add_task(new_task)

    def _popup_create_task(self, node: Node):
        wgt = CreateTaskDialog(self)
        wgt.accepted.connect(lambda i=node.get_id(), w=wgt: self._popup_create_task_callback(i, w))
        wgt.finished.connect(wgt.deleteLater)
        wgt.show()

    def _update_attribs_and_popup_modify_task_widget(self, task: Task):
        def operation(longop: LongOperation):
            self.__scene.request_attributes(task.get_id(), longop.new_op_data())
            attribs = yield  # we don't need to use them tho
            self._popup_modify_task_widget(task)

        self.__scene.add_long_operation(operation)

    def _popup_modify_task_widget(self, task: Task):
        if task.scene() is not self.__scene:  # if task was removed from scene while we were waiting for this function to be called
            return  # then do nothing
        wgt = CreateTaskDialog(self, task)
        wgt.accepted.connect(lambda i=task.get_id(), w=wgt: self._popup_modify_task_callback(i, w))
        wgt.finished.connect(wgt.deleteLater)
        wgt.show()

    def _popup_modify_task_callback(self, task_id, wgt: CreateTaskDialog):
        name, groups, changes, deletes, resolver_name, res_changed, res_deletes = wgt.get_task_changes()
        if len(changes) > 0 or len(deletes) > 0:
            self.__scene.request_update_task_attributes(task_id, changes, deletes)
        if name is not None:
            self.__scene.request_rename_task(task_id, name)
        if groups is not None:
            self.__scene.request_set_task_groups(task_id, set(groups))

        if resolver_name is not None or res_changed or res_deletes:
            if resolver_name == '':
                self.__scene.request_unset_environment_resolver_arguments(task_id)
            else:
                task = self.__scene.get_task(task_id)
                env_args = task.environment_attributes() or EnvironmentResolverArguments()
                args = dict(env_args.arguments())
                if res_changed:
                    args.update(res_changed)
                for name in res_deletes:
                    del args[name]
                env_args = EnvironmentResolverArguments(resolver_name or env_args.name(), args)
                self.__scene.request_set_environment_resolver_arguments(task_id, env_args)

        # TODO: this works only because connection worker CURRENTLY executes requests sequentially
        #  so first request to update task goes through, then request to update attributes.
        #  if connection worker is improoved to be multithreaded - this has to be enforced with smth like longops
        self.__scene.request_attributes(task_id)  # request updated task from scheduler

    def _popup_save_settings_dialog(self, node: Node):
        names = [x.name() for x in node.get_nodeui().parameters()]
        wgt = SaveNodeSettingsDialog(names, self)
        wgt.accepted.connect(lambda nid=node.get_id(), w=wgt: self._popup_save_settings_dialog_accepted_callback(nid, w))
        wgt.finished.connect(wgt.deleteLater)
        wgt.show()

    @Slot()
    def _popup_save_settings_dialog_accepted_callback(self, node_id: int, wgt: SaveNodeSettingsDialog):
        assert isinstance(wgt, SaveNodeSettingsDialog)
        parameter_names = wgt.selected_names()
        settings_name = wgt.settings_name()
        assert settings_name
        node = self.__scene.get_node(node_id)
        ui = node.get_nodeui()

        settings = {}
        for pname in parameter_names:
            param = ui.parameter(pname)
            if param.is_readonly():
                continue
            if param.has_expression():
                settings[pname] = {'value': param.unexpanded_value(),
                                   'expression': param.expression()}
            else:
                settings[pname] = param.unexpanded_value()
        self.__scene.save_nodetype_settings(node.node_type(), settings_name, settings)

    def _popup_set_settings_default(self, node, settings_name: Optional[str]):
        node_type = node.node_type()
        self.__scene.request_set_settings_default(node_type, settings_name)

    @Slot(object, object)
    def _popup_show_invocation_info(self, task_id: int, invjob: InvocationJob):
        popup = QDialog(parent=self)
        layout = QVBoxLayout(popup)
        edit = QTextEdit()
        edit.setReadOnly(True)
        layout.addWidget(edit)
        #popup = QMessageBox(QMessageBox.Information, f'invocation job information for task #{task_id}', 'see details', parent=self)
        popup.finished.connect(popup.deleteLater)
        popup.setModal(False)
        popup.setSizeGripEnabled(True)
        popup.setWindowTitle(f'invocation job information for task #{task_id}')

        env = 'Extra environment:\n' + '\n'.join(f'\t{k}={v}' for k, v in invjob.env().resolve().items()) if invjob.env() is not None else 'none'
        argv = f'Command line:\n\t{repr(invjob.args())}'
        extra_files = 'Extra Files:\n' + '\n'.join(f'\t{name}: {len(data):,d}B' for name, data in invjob.extra_files().items())

        #popup.setDetailedText('\n\n'.join((argv, env, extra_files)))
        edit.setPlainText('\n\n'.join((argv, env, extra_files)))

        popup.show()

    @Slot(str, float)
    def _scene_operation_progress_updated(self, op_name: str, progress_normalized: float):
        self.show_message(f'{op_name}: {round(100*progress_normalized):3g}%', 3)

    @Slot()
    def __unblock_imgui_input(self):
        self.__imgui_input_blocked = False

    @Slot()
    def _nodetypes_updated(self, nodetypes):
        self.__node_types = nodetypes

    @Slot()
    def _nodepresets_updated(self, nodepresets):
        self.__scheduler_presets = nodepresets  # TODO: we keep a LIVE copy of scene's cached presets here. that might be a problem later

    def _set_clipboard(self, text: str):
        QApplication.clipboard().setText(text)

    def _get_clipboard(self) -> str:
        return QApplication.clipboard().text()

    @timeit(0.05)
    def drawItems(self, *args, **kwargs):
        return super(NodeEditor, self).drawItems(*args, **kwargs)

    def drawForeground(self, painter: PySide2.QtGui.QPainter, rect: QRectF) -> None:
        painter.beginNativePainting()
        if not self.__imgui_init:
            logger.debug('initializing imgui')
            self.__imgui_init = True
            imgui.create_context()
            self.__imimpl = ProgrammablePipelineRenderer()
            imguio = imgui.get_io()
            # note that as of imgui 1.3.0 ini_file_name seem to have a bug of not increasing refcount,
            # so there HAS to be some other python variable, like self.__imgui_config_path, to ensure
            # that path is not garbage collected
            imguio.ini_file_name = self.__imgui_config_path
            imguio.display_size = 400, 400
            imguio.set_clipboard_text_fn = self._set_clipboard
            imguio.get_clipboard_text_fn = self._get_clipboard
            self._map_keys()

        imgui.get_io().display_size = self.rect().size().toTuple()  # rect.size().toTuple()
        # start new frame context
        imgui.new_frame()


        imgui.core.show_metrics_window()

        # open new window context
        imgui.set_next_window_size(561, 697, imgui.FIRST_USE_EVER)
        imgui.set_next_window_position(1065, 32, imgui.FIRST_USE_EVER)
        imgui.begin("Parameters")

        # draw text label inside of current window
        iitem = self.__scene.get_inspected_item()
        if iitem and isinstance(iitem, NetworkItemWithUI):
            iitem.draw_imgui_elements(self)

        # close current window context
        imgui.end()

        # undo window
        imgui.set_next_window_size(256, 300, imgui.FIRST_USE_EVER)
        imgui.set_next_window_position(32, 32, imgui.FIRST_USE_EVER)
        imgui.begin('op history stack')
        for name in reversed(self.__scene.undo_stack_names()):
            if len(name) > 40:
                name = name[:40] + '...'
            imgui.bullet_text(name)
            if not imgui.is_item_visible():
                break
        imgui.end()

        # tab menu
        if self.__create_menu_popup_toopen:
            imgui.open_popup('create node')
            self.__node_type_input = ''
            self.__menu_popup_selection_id = 0
            self.__menu_popup_selection_name = ()
            self.__menu_popup_arrow_down = False

            self.change_shortcut_context('create_node')

        if imgui.begin_popup('create node'):
            changed, self.__node_type_input = imgui.input_text('', self.__node_type_input, 256)
            if not imgui.is_item_active() and not imgui.is_mouse_down():
                # if text input is always focused - selectable items do not work
                imgui.set_keyboard_focus_here(-1)
            if changed:
                # reset selected item if filter line changed
                self.__menu_popup_selection_id = 0
                self.__menu_popup_selection_name = ()
            item_number = 0
            max_items = 32
            for (entity_type, entity_type_label, package), (type_name, type_meta) in chain(zip(repeat(('node', None, None)), self.__node_types.items()),
                                                                        zip(repeat(('vpreset', 'preset', None)), self.__viewer_presets.items()),
                                                                        *(zip(repeat(('spreset', 'preset', pkg)), pkgdata.items()) for pkg, pkgdata in self.__scheduler_presets.items())):

                inparts = [x.strip() for x in self.__node_type_input.split(' ')]
                label = type_meta.label
                tags = type_meta.tags
                if all(x in type_name
                       or any(t.startswith(x) for t in tags)
                       or x in label for x in inparts):  # TODO: this can be cached
                    selected = self.__menu_popup_selection_id == item_number
                    if entity_type_label is not None:
                        label += f' ({entity_type_label})'
                    _, selected = imgui.selectable(f'{label}##popup_selectable',  selected=selected, flags=imgui.SELECTABLE_DONT_CLOSE_POPUPS)
                    if selected:
                        self.__menu_popup_selection_id = item_number
                        self.__menu_popup_selection_name = (package, type_name, label, entity_type)
                    item_number += 1
                    if item_number > max_items:
                        break

            imguio: imgui.core._IO = imgui.get_io()
            if imguio.keys_down[imgui.KEY_DOWN_ARROW]:
                if not self.__menu_popup_arrow_down:
                    self.__menu_popup_selection_id += 1
                    self.__menu_popup_selection_id = self.__menu_popup_selection_id % max(1, item_number)
                    self.__menu_popup_arrow_down = True
            elif imguio.keys_down[imgui.KEY_UP_ARROW]:
                if not self.__menu_popup_arrow_down:
                    self.__menu_popup_selection_id -= 1
                    self.__menu_popup_selection_id = self.__menu_popup_selection_id % max(1, item_number)
                    self.__menu_popup_arrow_down = True
            if imguio.keys_down[imgui.KEY_ENTER] or imgui.is_mouse_double_clicked():
                imgui.close_current_popup()
                self.reset_shortcut_context()
                # for type_name, type_meta in self.__node_types.items():
                #     if self.__node_type_input in type_name \
                #             or self.__node_type_input in type_meta.tags \
                #             or self.__node_type_input in type_meta.label:
                #         self.__node_type_input = type_name
                #         break
                # else:
                #     self.__node_type_input = ''
                if self.__menu_popup_selection_name:
                    package, entity_name, label, entity_type = self.__menu_popup_selection_name
                    if entity_type == 'node':
                        self.__scene.create_node(entity_name, f'{label} {generate_name(5, 7)}', self.mapToScene(imguio.mouse_pos.x, imguio.mouse_pos.y))
                    elif entity_type == 'vpreset':
                        self.__scene.nodes_from_snippet(self.__viewer_presets[entity_name], self.mapToScene(self.mapFromGlobal(QCursor.pos())))
                    elif entity_type == 'spreset':
                        if isinstance(self.__scheduler_presets[package][entity_name], NodeSnippetDataPlaceholder):
                            self.get_snippet_from_scheduler_and_create_nodes(package, entity_name, pos=self.mapToScene(self.mapFromGlobal(QCursor.pos())))
                        else:  # if already fetched
                            self.__scene.nodes_from_snippet(self.__scheduler_presets[package][entity_name], self.mapToScene(self.mapFromGlobal(QCursor.pos())))
            elif self.__menu_popup_arrow_down:
                self.__menu_popup_arrow_down = False

            elif imguio.keys_down[imgui.KEY_ESCAPE]:
                imgui.close_current_popup()
                self.reset_shortcut_context()
                self.__node_type_input = ''
                self.__menu_popup_selection_id = 0
            imgui.end_popup()

        self.__create_menu_popup_toopen = False
        # pass all drawing comands to the rendering pipeline
        # and close frame context
        imgui.render()
        # imgui.end_frame()
        self.__imimpl.render(imgui.get_draw_data())
        painter.endNativePainting()

    def imguiProcessEvents(self, event: PySide2.QtGui.QInputEvent, do_recache=True):
        if self.__imgui_input_blocked:
            return
        if not self.__imgui_init:
            return
        io = imgui.get_io()
        if isinstance(event, PySide2.QtGui.QMouseEvent):
            io.mouse_pos = event.pos().toTuple()
        elif isinstance(event, PySide2.QtGui.QWheelEvent):
            io.mouse_wheel = event.angleDelta().y() / 100
        elif isinstance(event, PySide2.QtGui.QKeyEvent):
            #print('pressed', event.key(), event.nativeScanCode(), event.nativeVirtualKey(), event.text(), imgui.KEY_A)
            if event.key() in imgui_key_map:
                if event.type() == QEvent.KeyPress:
                    io.keys_down[imgui_key_map[event.key()]] = True  # TODO: figure this out
                    #io.keys_down[event.key()] = True
                elif event.type() == QEvent.KeyRelease:
                    io.keys_down[imgui_key_map[event.key()]] = False
            elif event.key() == Qt.Key_Control:
                io.key_ctrl = event.type() == QEvent.KeyPress

            if event.type() == QEvent.KeyPress and len(event.text()) > 0:
                io.add_input_character(ord(event.text()))

        if isinstance(event, (PySide2.QtGui.QMouseEvent, PySide2.QtGui.QWheelEvent)):
            io.mouse_down[0] = event.buttons() & Qt.LeftButton
            io.mouse_down[1] = event.buttons() & Qt.MiddleButton
            io.mouse_down[2] = event.buttons() & Qt.RightButton
        if do_recache:
            self.resetCachedContent()

    def focusInEvent(self, event):
        # just in case we will drop all imgui extra keys
        event.accept()
        if self.__imgui_input_blocked:
            return
        if not self.__imgui_init:
            return
        io = imgui.get_io()
        for key in imgui_key_map.values():
            io.keys_down[key] = False
        io.key_ctrl = False

    # def _map_keys(self):
    #     key_map = imgui.get_io().key_map
    #
    #     key_map[imgui.KEY_TAB] = Qt.Key_Tab
    #     key_map[imgui.KEY_LEFT_ARROW] = Qt.Key_Left
    #     key_map[imgui.KEY_RIGHT_ARROW] = Qt.Key_Right
    #     key_map[imgui.KEY_UP_ARROW] = Qt.Key_Up
    #     key_map[imgui.KEY_DOWN_ARROW] = Qt.Key_Down
    #     key_map[imgui.KEY_PAGE_UP] = Qt.Key_PageUp
    #     key_map[imgui.KEY_PAGE_DOWN] = Qt.Key_PageDown
    #     key_map[imgui.KEY_HOME] = Qt.Key_Home
    #     key_map[imgui.KEY_END] = Qt.Key_End
    #     key_map[imgui.KEY_DELETE] = Qt.Key_Delete
    #     key_map[imgui.KEY_BACKSPACE] = Qt.Key_Backspace
    #     key_map[imgui.KEY_ENTER] = Qt.Key_Enter
    #     key_map[imgui.KEY_ESCAPE] = Qt.Key_Escape
    #     key_map[imgui.KEY_A] = Qt.Key_A
    #     key_map[imgui.KEY_C] = Qt.Key_C
    #     key_map[imgui.KEY_V] = Qt.Key_V
    #     key_map[imgui.KEY_X] = Qt.Key_X
    #     key_map[imgui.KEY_Y] = Qt.Key_Y
    #     key_map[imgui.KEY_Z] = Qt.Key_Z

    def _map_keys(self):
        key_map = imgui.get_io().key_map

        key_map[imgui.KEY_TAB] = imgui.KEY_TAB
        key_map[imgui.KEY_LEFT_ARROW] = imgui.KEY_LEFT_ARROW
        key_map[imgui.KEY_RIGHT_ARROW] = imgui.KEY_RIGHT_ARROW
        key_map[imgui.KEY_UP_ARROW] = imgui.KEY_UP_ARROW
        key_map[imgui.KEY_DOWN_ARROW] = imgui.KEY_DOWN_ARROW
        key_map[imgui.KEY_PAGE_UP] = imgui.KEY_PAGE_UP
        key_map[imgui.KEY_PAGE_DOWN] = imgui.KEY_PAGE_DOWN
        key_map[imgui.KEY_HOME] = imgui.KEY_HOME
        key_map[imgui.KEY_END] = imgui.KEY_END
        key_map[imgui.KEY_DELETE] = imgui.KEY_DELETE
        key_map[imgui.KEY_BACKSPACE] = imgui.KEY_BACKSPACE
        key_map[imgui.KEY_ENTER] = imgui.KEY_ENTER
        key_map[imgui.KEY_ESCAPE] = imgui.KEY_ESCAPE
        key_map[imgui.KEY_A] = imgui.KEY_A
        key_map[imgui.KEY_C] = imgui.KEY_C
        key_map[imgui.KEY_V] = imgui.KEY_V
        key_map[imgui.KEY_X] = imgui.KEY_X
        key_map[imgui.KEY_Y] = imgui.KEY_Y
        key_map[imgui.KEY_Z] = imgui.KEY_Z

    def request_ui_focus(self, item: NetworkItem):
        if self.__ui_focused_item is not None and self.__ui_focused_item.scene() != self.__scene:
            self.__ui_focused_item = None

        if self.__ui_focused_item is not None:
            return False
        self.__ui_focused_item = item
        return True

    def release_ui_focus(self, item: NetworkItem):
        assert item == self.__ui_focused_item, "ui focus was released by not the item that got focus"
        self.__ui_focused_item = None
        return True

    def mouseDoubleClickEvent(self, event: PySide2.QtGui.QMouseEvent):
        self.imguiProcessEvents(event)
        if imgui.get_io().want_capture_mouse:
            event.accept()
        else:
            super(NodeEditor, self).mouseDoubleClickEvent(event)

    def mouseMoveEvent(self, event: PySide2.QtGui.QMouseEvent):
        self.imguiProcessEvents(event)
        if imgui.get_io().want_capture_mouse:
            event.accept()
        else:
            if self.__ui_panning_lastpos is not None:
                rect = self.sceneRect()
                rect.setSize(QSize(1, 1))
                self.setSceneRect(rect.translated(*((self.__ui_panning_lastpos - event.screenPos()) * (2 ** self.__view_scale)).toTuple()))
                #self.translate(*(event.screenPos() - self.__ui_panning_lastpos).toTuple())
                self.__ui_panning_lastpos = event.screenPos()
            else:
                super(NodeEditor, self).mouseMoveEvent(event)

    def mousePressEvent(self, event: PySide2.QtGui.QMouseEvent):
        self.imguiProcessEvents(event)
        if imgui.get_io().want_capture_mouse:
            event.accept()
        else:
            if event.buttons() & Qt.MiddleButton:
                self.__ui_panning_lastpos = event.screenPos()
            elif event.buttons() & Qt.RightButton and self.itemAt(event.pos()) is None:
                event.accept()
                self.show_general_menu(event.globalPos())
            else:
                super(NodeEditor, self).mousePressEvent(event)

    def mouseReleaseEvent(self, event: PySide2.QtGui.QMouseEvent):
        self.imguiProcessEvents(event)
        if imgui.get_io().want_capture_mouse:
            event.accept()
        else:
            super(NodeEditor, self).mouseReleaseEvent(event)
            if not (event.buttons() & Qt.MiddleButton):
                self.__ui_panning_lastpos = None
        PySide2.QtCore.QTimer.singleShot(50, self.resetCachedContent)

    def wheelEvent(self, event: PySide2.QtGui.QWheelEvent):
        self.imguiProcessEvents(event)
        if imgui.get_io().want_capture_mouse:
            event.accept()
        else:
            event.accept()
            self.__view_scale = max(0, self.__view_scale - event.angleDelta().y()*0.001)

            iz = 2**(-self.__view_scale)
            self.setTransform(QTransform.fromScale(iz, iz))
            super(NodeEditor, self).wheelEvent(event)

    def keyPressEvent(self, event: PySide2.QtGui.QKeyEvent):
        self.imguiProcessEvents(event)
        if imgui.get_io().want_capture_keyboard:
            event.accept()
        else:
            if event.key() == Qt.Key_Tab:
                # in case enter or escape is pressed at this time - force unpress it
                self.imguiProcessEvents(PySide2.QtGui.QKeyEvent(QEvent.KeyRelease, Qt.Key_Return, Qt.NoModifier))
                self.imguiProcessEvents(PySide2.QtGui.QKeyEvent(QEvent.KeyRelease, Qt.Key_Escape, Qt.NoModifier))

                self.__create_menu_popup_toopen = True
                self.__scene.request_node_types_update()
                self.__scene.request_node_presets_update()
                PySide2.QtCore.QTimer.singleShot(0, self.resetCachedContent)
            super(NodeEditor, self).keyPressEvent(event)

    def keyReleaseEvent(self, event: PySide2.QtGui.QKeyEvent):
        self.imguiProcessEvents(event)
        if imgui.get_io().want_capture_keyboard:
            event.accept()
        else:
            super(NodeEditor, self).keyReleaseEvent(event)

    def closeEvent(self, event: PySide2.QtGui.QCloseEvent) -> None:
        self.stop()
        super(NodeEditor, self).closeEvent(event)

    def event(self, event):
        if event.type() == QEvent.ShortcutOverride:
            if imgui.get_io().want_capture_keyboard:
                event.accept()
                return True
        return super(NodeEditor, self).event(event)

    def start(self):
        self.__scene.start()

    def stop(self):
        self.__scene.stop()
        self.__scene.save_node_layout()


imgui_key_map = {
    Qt.Key_Tab: imgui.KEY_TAB,
    Qt.Key_Left: imgui.KEY_LEFT_ARROW,
    Qt.Key_Right: imgui.KEY_RIGHT_ARROW,
    Qt.Key_Up: imgui.KEY_UP_ARROW,
    Qt.Key_Down: imgui.KEY_DOWN_ARROW,
    Qt.Key_PageUp: imgui.KEY_PAGE_UP,
    Qt.Key_PageDown: imgui.KEY_PAGE_DOWN,
    Qt.Key_Home: imgui.KEY_HOME,
    Qt.Key_End: imgui.KEY_END,
    Qt.Key_Delete: imgui.KEY_DELETE,
    Qt.Key_Backspace: imgui.KEY_BACKSPACE,
    Qt.Key_Return: imgui.KEY_ENTER,
    Qt.Key_Escape: imgui.KEY_ESCAPE,
    Qt.Key_A: imgui.KEY_A,
    Qt.Key_C: imgui.KEY_C,
    Qt.Key_V: imgui.KEY_V,
    Qt.Key_X: imgui.KEY_X,
    Qt.Key_Y: imgui.KEY_Y,
    Qt.Key_Z: imgui.KEY_Z,
}
