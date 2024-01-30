import sys

from math import log2
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from enum import Enum
from .graphics_items import Task, Node, NetworkItem
from .graphics_scene import QGraphicsImguiScene
from .long_op import LongOperation
from .widgets.flashy_label import FlashyLabel
from .ui_snippets import UiNodeSnippetData
from .ui_elements_base import ImguiWindow
from .menu_entry_base import MainMenuLocation
from .utils import BetterOrderedDict
from lifeblood.base import TypeMetadata
from lifeblood.misc import timeit
from lifeblood.enums import TaskState
from lifeblood.config import get_config
from lifeblood import logging
from lifeblood import paths
from lifeblood.node_type_metadata import NodeTypeMetadata
from lifeblood.taskspawn import NewTask
from lifeblood.invocationjob import InvocationJob
from lifeblood.snippets import NodeSnippetData, NodeSnippetDataPlaceholder
from lifeblood.environment_resolver import EnvironmentResolverArguments

import PySide2.QtCore
import PySide2.QtGui
from PySide2.QtWidgets import *
from PySide2.QtCore import QObject, Qt, Slot, QRectF, QPointF, QEvent, QSize
from PySide2.QtGui import QSurfaceFormat, QPainter, QTransform, QKeySequence, QCursor, QPen, QColor, QClipboard

from .dialogs import MessageWithSelectableText
from .create_task_dialog import CreateTaskDialog
from .save_node_settings_dialog import SaveNodeSettingsDialog
from .nodeeditor_overlays.overlay_base import NodeEditorOverlayBase
from .undo_stack import OperationCompletionDetails, OperationCompletionStatus

import imgui
from .imgui_opengl_hotfix import AdjustedProgrammablePipelineRenderer as ProgrammablePipelineRenderer

from typing import Any, Optional, List, Tuple, Dict, Set, Callable, Union

MenuStructure = BetterOrderedDict

logger = logging.get_logger('viewer')

_in_debug_mode = logger.isEnabledFor(logging.DEBUG)


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


@dataclass
class MenuItem:
    label: str
    shortcut: Optional[str]

    def __hash__(self):
        return hash((self.label, self.shortcut))


class MenuSeparatorItem:
    """
    class representing menu separator for viewer main menu definition
    """
    __cnt = 0

    def __init__(self):
        self.__num = MenuSeparatorItem.__cnt
        MenuSeparatorItem.__cnt += 1

    def __hash__(self):
        return self.__num


class Shortcutable:
    def __init__(self, config_name):
        assert isinstance(self, QObject)
        self.__shortcuts: Dict[str, QShortcut] = {}
        self.__shortcut_contexts: Dict[str, Set[str]] = {}
        self.__config = get_config(config_name)

        self.__context_name = 'main'

    def add_shortcut(self, action: str, context: str, shortcut: str, callback: Callable):
        """
        add shortcut
        """
        if action in self.__shortcuts:
            logger.error(f'action "{action}" is already defined, ignoring')
            return

        shortcut = self.__config.get_option_noasync(f'shortcuts.{action}', shortcut)

        self.__shortcuts[action] = QShortcut(QKeySequence(shortcut), self, shortcutContext=Qt.WidgetShortcut)
        logger.debug(f'adding shortcut: {self.__shortcuts[action]}, {shortcut}')
        self.__shortcut_contexts.setdefault(action, set()).add(context)
        self.__shortcuts[action].activated.connect(callback)
        if self.current_shortcut_context() != context:
            self.__shortcuts[action].setEnabled(False)

    def change_shortcut_context(self, new_context_name: str) -> None:
        if self.__context_name == new_context_name:
            return
        logger.debug(f'changed shortcut context to "{new_context_name}"')
        self.disable_shortcuts()
        self.__context_name = new_context_name
        self.enable_shortcuts()

    def reset_shortcut_context(self) -> None:
        return self.change_shortcut_context('main')

    def current_shortcut_context(self) -> str:
        return self.__context_name

    def shortcuts(self):
        return MappingProxyType(self.__shortcuts)

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
        self.__opened_windows: Set[ImguiWindow] = set()
        self.__overlays: List[NodeEditorOverlayBase] = []
        self.__actions: Dict[str, Callable[[], None]] = {}
        self.__menu_actions: MenuStructure = MenuStructure()

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

        # initialize standard actions
        for action, (callback, shortcut) in self._default_actions().items():
            self.add_action(action, callback, shortcut)

        for action, menu_entry in self._default_menu():
            menu = self.__menu_actions
            shortcut = None
            if qtshortcut := self.shortcuts().get(action):
                shortcut = qtshortcut.key().toString()
            for submenu in menu_entry.location:
                menu = menu.setdefault(submenu, MenuStructure())
            if menu_entry.label is ...:
                menu[MenuSeparatorItem()] = None
            else:
                menu[MenuItem(menu_entry.label, shortcut)] = self.__actions.get(action, lambda _: ())

    def _default_menu(self) -> Tuple[Tuple[Optional[str], MainMenuLocation], ...]:
        return (
            ('nodeeditor.undo', MainMenuLocation(('Edit',), 'Undo')),
            (None, MainMenuLocation(('Edit',), ...)),
            ('nodeeditor.copy', MainMenuLocation(('Edit',), 'Copy')),
            ('nodeeditor.paste', MainMenuLocation(('Edit',), 'Paste')),
            (None, MainMenuLocation(('Edit',), ...)),
            ('nodeeditor.copy_as_code', MainMenuLocation(('Edit',), 'Copy as code')),
            ('nodeeditor.paste_from_code', MainMenuLocation(('Edit',), 'Paste from code')),
            (None, MainMenuLocation(('Edit',), ...)),
        )

    def _default_actions(self) -> Dict[str, Tuple[Callable, str]]:
        return {
            'nodeeditor.undo': (self.undo, 'Ctrl+z'),
            'nodeeditor.copy': (self.copy_selected_nodes, 'Ctrl+c'),
            'nodeeditor.paste': (self.paste_copied_nodes, 'Ctrl+v'),
            'nodeeditor.copy_as_code': (self.copy_selected_nodes_to_clipboard, 'Ctrl+Shift+c'),
            'nodeeditor.paste_from_code': (self.paste_nodes_from_clipboard, 'Ctrl+Shift+v'),
            'nodeeditor.layout_graph': (self.layout_selected_nodes, 'Ctrl+l'),
            'nodeeditor.focus_selected': (self.focus_on_selected, 'f'),
            'nodeeditor.delete': (self.delete_selected, 'delete'),
        }

    def add_action(self, action_name: str, action_callback: Callable, shortcut: Optional[str], menu_entry: Optional[MainMenuLocation] = None, *, insert_menu_after_label: Optional[str] = None):
        logger.info(f'registering action "{action_name}"')
        if action_name in self.__actions:
            raise RuntimeError(f'action "{action_name}" is already registered')
        self.__actions[action_name] = action_callback
        if menu_entry is not None:
            menu = self.__menu_actions
            for submenu in menu_entry.location:
                menu = menu.setdefault(submenu, MenuStructure())

            if insert_menu_after_label:
                try:
                    index = menu.index(insert_menu_after_label, key_func=lambda x: x.label if isinstance(x, MenuItem) else x) + 1
                except ValueError:
                    index = len(menu)

                menu.insert_at(MenuItem(menu_entry.label, shortcut), action_callback, index)
            else:
                menu[MenuItem(menu_entry.label, shortcut)] = action_callback

        if shortcut:
            self.add_shortcut(action_name, 'main', shortcut, action_callback)

    def perform_action(self, action_name: str):
        """
        perform action named action_name
        """
        if action_name not in self.__actions:
            logger.error(f'no action named "{action_name}"')
            return
        self.__actions[action_name]()

    def add_overlay(self, overlay: NodeEditorOverlayBase):
        """
        add new graphical overlay to this viewer
        """
        self.__overlays.append(overlay)
        self.__menu_actions.setdefault('View', MenuStructure()).setdefault('Overlays', MenuStructure()).setdefault(
            lambda: f'{"[x]" if overlay.enabled() else "[ ]"} {overlay.name()}',
            lambda: overlay.toggle()
        )

    def overlays(self):
        """
        get graphical overlays attached to this viewer
        """
        return tuple(self.__overlays)

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

    # popup related

    def _window_opened(self, window: ImguiWindow):
        self.__opened_windows.add(window)
        PySide2.QtCore.QTimer.singleShot(0, self.resetCachedContent)  # this ensures foreground is redrawn, so window's draw is actually called

    def _window_closed(self, window: ImguiWindow):
        if window not in self.__opened_windows:
            logger.error(f'a window reported being closed, but it wasn\'t even opened! {window}')
            return

        # this trick below is to keep iterated __opened_windows valid
        logger.debug(f'closed window {window}')
        new_set = self.__opened_windows.copy()
        new_set.remove(window)
        self.__opened_windows = new_set

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
    def copy_selected_nodes_to_clipboard(self):
        """
        Serializes selected nodes to internal ascii representation and sets it to be to contents of the system clipboard
        Internal clipboard is not affected by this operation
        Nodes are not additionally renamed, as they are with standard copy operation
        """
        snippet = UiNodeSnippetData.from_viewer_nodes([x for x in self.__scene.selectedItems() if isinstance(x, Node)])
        QClipboard().setText(snippet.serialize(ascii=True).decode('latin1'), QClipboard.Clipboard)
        self.show_message('Nodes copied to clipboard', 2)

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

    @Slot(QPointF)
    def paste_nodes_from_clipboard(self, pos: Optional[QPointF] = None):
        """
        this function expects system clipboard to hold valid node snippet
        given snippet from clipboard is created.
        """
        if pos is None:
            pos = self.mapToScene(self.mapFromGlobal(QCursor.pos()))
        clipdata = QClipboard().text(QClipboard.Clipboard)
        if clipdata is None:
            return
        try:
            data = NodeSnippetData.deserialize(clipdata.encode('latin1'))
        except Exception as e:
            self.show_message("Pasting failed: incorrect clipboard contents", 4)
            return
        self.__scene.nodes_from_snippet(data, pos)
        self.show_message('Nodes pasted', 2)

    @Slot()
    def undo(self):
        for op in self.__scene.undo():
            self.show_message(f'undo: {op}', 1)

    @Slot()
    def delete_selected(self):
        def _on_finished(_, result: OperationCompletionDetails):
            if result.status == OperationCompletionStatus.PartialSuccess:
                self.show_message('::warning::some nodes were not deleted', 1)
            elif result.status == OperationCompletionStatus.NotPerformed:
                self.show_message('::error::no nodes were deleted', 2)
        self.__scene.delete_selected_nodes(callback=_on_finished)

    @Slot(str, str, QPointF)
    def get_snippet_from_scheduler_and_create_nodes(self, package: str, preset_name: str, pos: QPointF):
        def todoop(longop):
            self.__scene.request_node_preset(package, preset_name, longop.new_op_data())
            _, _, snippet = yield
            self.__scheduler_presets.setdefault(package, {})[preset_name] = snippet

            self.__scene.nodes_from_snippet(snippet, pos)

        self.__scene.add_long_operation(todoop)

    def create_from_viewer_preset(self, preset_name: str, pos: QPointF):
        self.scene().nodes_from_snippet(self.__viewer_presets[preset_name], pos)

    def create_from_scheduler_preset(self, package_name: str, preset_name: str, pos: QPointF):
        if isinstance(self.__scheduler_presets[package_name][preset_name], NodeSnippetDataPlaceholder):
            self.get_snippet_from_scheduler_and_create_nodes(package_name, preset_name, pos=pos)
        else:  # if already fetched
            self.__scene.nodes_from_snippet(self.__scheduler_presets[package_name][preset_name], pos)

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

    def selected_nodes(self) -> Tuple[Node, ...]:
        return tuple(node for node in self.__scene.selectedItems() if isinstance(node, Node))

    def node_types(self) -> MappingProxyType[str, TypeMetadata]:
        return MappingProxyType(self.__node_types)

    def viewer_presets_metadata(self) -> MappingProxyType[str, TypeMetadata]:
        return MappingProxyType(self.__viewer_presets)

    def scheduler_presets_metadata(self) -> MappingProxyType[str, Dict[str, TypeMetadata]]:  # TODO: make inner dict immutable
        return MappingProxyType(self.__scheduler_presets)

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
            menu.addAction('pause further processing').triggered.connect(lambda checked=False, x=task.get_id(): self.__scene.set_tasks_paused([x], True))

        if task.state() == TaskState.IN_PROGRESS:
            action_text = 'cancel' if task.paused() else 'cancel and reschedule'
            menu.addAction(action_text).triggered.connect(lambda checked=False, x=task.get_id(): self.__scene.request_task_cancel(x))
        elif task.state() in (TaskState.READY, TaskState.ERROR):
            action_text = 'retry' if task.state() == TaskState.ERROR else 'regenerate'
            menu.addAction(action_text).triggered.connect(lambda checked=False, x=task.get_id(): self.__scene.set_task_state([x], TaskState.WAITING))
        elif task.state() == TaskState.DONE and task.paused():
            menu.addAction('retry').triggered.connect(
                lambda checked=False, x=task.get_id(): (
                    self.__scene.set_task_state([x], TaskState.WAITING),
                    self.__scene.set_tasks_paused([x], False)
                )
            )

        if _in_debug_mode:
            state_submenu = menu.addMenu('force state (debug)')
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
        menu.addAction('show task list').triggered.connect(lambda: (
            node.set_selected(True, unselect_others=True),
            self.perform_action('nodeeditor.task_list_for_selected_node')
        ))
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
        menu.addAction('regenerate all ready tasks').triggered.connect(node.regenerate_all_ready_tasks)
        menu.addAction('retry all error tasks').triggered.connect(node.retry_all_error_tasks)
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
    def _scene_operation_progress_updated(self, op_id: int, op_name: str, progress_normalized: float):
        if progress_normalized >= 0:
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

    def drawBackground(self, painter, rect):
        pen = QPen()
        pen2 = QPen()
        pen.setStyle(Qt.DotLine)
        pen2.setStyle(Qt.DotLine)
        pen.setColor(QColor.fromRgbF(0.65, 0.65, 0.75, 0.05))

        spacing = 150
        limit_per_width = 10

        top = rect.top()
        bottom = rect.bottom()
        left = rect.left()
        right = rect.right()

        spacing_scaling_exp = max(1.0, log2((right - left) / limit_per_width / spacing))
        width_scale = min(1.0, max(0.0, 1.5 * (1 - pow(spacing_scaling_exp % 1, 0.75))))

        width_base = 1.85 * max(1.0, pow((right - left) / limit_per_width / spacing, 0.95))
        pen.setWidthF(width_base)

        spacing *= 2 ** int(spacing_scaling_exp)
        if width_scale < 1.0:
            pen2.setColor(QColor.fromRgbF(0.65 * width_scale, 0.65 * width_scale, 0.75 * width_scale, 0.05 * width_scale))
            pen2.setWidthF(width_base * width_scale)
            spacing2 = 2 * spacing
            painter.setPen(pen)
            for x in range(int(rect.left() / spacing2) * spacing2, int(rect.right()) + 1, spacing2):
                painter.drawLine(x, bottom, x, top)
            for y in range(int(rect.top() / spacing2) * spacing2, int(rect.bottom()) + 1, spacing2):
                painter.drawLine(left, y, right, y)
            painter.setPen(pen2)
            for x in range(int(rect.left() / spacing2 - 1) * spacing2, int(rect.right()) + 1, spacing2):
                painter.drawLine(x + spacing, bottom, x + spacing, top)
            for y in range(int(rect.top() / spacing2 - 1) * spacing2, int(rect.bottom()) + 1, spacing2):
                painter.drawLine(left, y + spacing, right, y + spacing)
        else:
            painter.setPen(pen)
            for x in range(int(rect.left()/spacing)*spacing, int(rect.right())+1, spacing):
                painter.drawLine(x, bottom, x, top)
            for y in range(int(rect.top()/spacing)*spacing, int(rect.bottom())+1, spacing):
                painter.drawLine(left, y, right, y)

    def drawForeground(self, painter: PySide2.QtGui.QPainter, rect: QRectF) -> None:
        for overlay in self.__overlays:
            overlay.wrapped_draw_scene_foreground(painter, rect)

        painter.beginNativePainting()
        if not self.__imgui_init:
            try:
                logger.debug('initializing imgui')
                self.__imgui_init = True
                imgui.create_context()
                self.__imimpl = ProgrammablePipelineRenderer()
                imguio = imgui.get_io()
            except Exception as e:
                logger.exception(f'Failed to initialized opengl context for imgui: {e}')
                logger.critical('viewer cannot work without opengl context, shutting down')
                sys.exit(1)
            # note that as of imgui 1.3.0 ini_file_name seem to have a bug of not increasing refcount,
            # so there HAS to be some other python variable, like self.__imgui_config_path, to ensure
            # that path is not garbage collected
            imguio.ini_file_name = self.__imgui_config_path
            imguio.display_size = 400, 400
            imguio.set_clipboard_text_fn = self._set_clipboard
            imguio.get_clipboard_text_fn = self._get_clipboard
            self._map_keys()

            style = imgui.get_style()
            style.scrollbar_size = 12
            style.window_rounding = 4
            style.frame_rounding = 4
            style.tab_rounding = 4

        imgui.get_io().display_size = self.rect().size().toTuple()  # rect.size().toTuple()
        # start new frame context
        imgui.new_frame()

        # draw main menu
        def _draw_one_level(submenu):
            for item, something in submenu.items():
                if isinstance(something, MenuStructure):
                    assert isinstance(item, str)
                    if imgui.begin_menu(item):
                        _draw_one_level(something)
                        imgui.end_menu()
                else:
                    shortcut = None
                    if isinstance(item, MenuSeparatorItem):
                        imgui.separator()
                        continue
                    elif isinstance(item, MenuItem):
                        label = item.label
                        if callable(label):
                            label = label()
                        shortcut = item.shortcut
                    elif isinstance(item, str):
                        label = item
                    elif callable(item):
                        label = item()
                    else:
                        raise RuntimeError(f'unknown menu item type: {item}')

                    clicked, _ = imgui.menu_item(label, shortcut)
                    if clicked:
                        something()

        imgui.begin_main_menu_bar()
        _draw_one_level(self.__menu_actions)
        imgui.end_main_menu_bar()
        #

        # imgui.core.show_metrics_window()

        # general window draw
        any_window_focused = False
        for window in self.__opened_windows:
            window.draw()
            if window.is_focused():
                ctx = window.shortcut_context_id()
                if ctx is not None:
                    if ctx != self.current_shortcut_context():
                        self.change_shortcut_context(ctx)
                    any_window_focused = True
        if not any_window_focused:
            self.reset_shortcut_context()

        # finally draw foreground's imgui shit # TODO: maybe move this imgui shit to __foreground too?
        for overlay in self.__overlays:
            overlay.wrapped_draw_imgui_foreground(self)

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

    def scene(self) -> QGraphicsImguiScene:  # this function is here just for typing
        return super().scene()

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
            if event.buttons() & Qt.MiddleButton or (event.buttons() & Qt.LeftButton and event.modifiers() & Qt.AltModifier):
                self.__ui_panning_lastpos = event.screenPos()
            elif event.buttons() & Qt.RightButton and self.itemAt(event.pos()) is None:
                event.accept()
                self.show_general_menu(event.globalPos())
            else:
                for overlay in self.__overlays:
                    if overlay.wrapped_handle_mouse_press(event):
                        break
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
