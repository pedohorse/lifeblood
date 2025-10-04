from datetime import timedelta
import imgui
from lifeblood import logging
from lifeblood.enums import InvocationState, TaskState
from lifeblood.ui_protocol_data import TaskData, IncompleteInvocationLogData, InvocationLogData
from .scene_task_preview import SceneTaskPreview
from ..drawable_task import DrawableTask
from ...graphics_items import Node
from ...graphics_scene_container import GraphicsSceneWithNodesAndTasks
from ...network_item_watchers import NetworkItemWatcher

from ...utils import call_later

from lifeblood_viewer.editor_scene_integration import fetch_and_open_log_viewer
from lifeblood_viewer.scene_data_controller import SceneDataController
from lifeblood_viewer.graphics_scene_viewing_widget import GraphicsSceneViewingWidgetBase

from PySide6.QtCore import Qt, QPointF
from PySide6.QtWidgets import QGraphicsItem, QGraphicsSceneMouseEvent

from typing import Optional, Set


logger = logging.get_logger('viewer')


class SceneTask(DrawableTask):
    def __init__(self, scene: GraphicsSceneWithNodesAndTasks, task_data: TaskData, data_controller: SceneDataController):
        super().__init__(scene, task_data)
        self.__scene_container = scene
        self.__data_controller = data_controller
        self.__meta_needs_to_be_requested = False
        self.setAcceptHoverEvents(True)
        # self.setFlags(QGraphicsItem.ItemIsSelectable)

        self.__ui_interactor: Optional[SceneTaskPreview] = None
        self.__press_pos: Optional[QPointF] = None

        self.__requested_invocs_while_selected = set()

    def add_item_watcher(self, watcher: "NetworkItemWatcher"):
        super().add_item_watcher(watcher)
        # additionally refresh ui if we are not being watched
        if len(self.item_watchers()) == 1:  # it's a first watcher
            self.refresh_ui()

    def set_name(self, name: str):
        super().set_name(name)
        self.refresh_ui()

    def set_groups(self, groups: Set[str]):
        super().set_groups(groups)
        self.refresh_ui()

    def set_state(self, state: Optional[TaskState], paused: Optional[bool]):
        super().set_state(state, paused)
        self.refresh_ui()

    def set_state_details(self, state_details: Optional[str] = None):
        super().set_state_details(state_details)
        self.refresh_ui()

    def set_progress(self, progress: float):
        super().set_progress(progress)
        self.refresh_ui()

    def refresh_ui(self):
        """
        unlike update - this method actually queries new task ui status
        if task is not selected or not watched - does nothing
        :return:
        """
        if not self.isSelected() and len(self.item_watchers()) == 0:
            return
        self.__meta_needs_to_be_requested = True  # actual request will happen when DRAWN

        for invoc_id, nid, invoc_dict in self.invocation_logs():
            if invoc_dict is None:
                continue
            if (isinstance(invoc_dict, IncompleteInvocationLogData)
                    or invoc_dict.invocation_state != InvocationState.FINISHED) and invoc_id in self.__requested_invocs_while_selected:
                self.__requested_invocs_while_selected.remove(invoc_id)

    def itemChange(self, change, value):
        if change == QGraphicsItem.GraphicsItemChange.ItemSelectedHasChanged:
            if value and self.node() is not None:   # item was just selected
                self.refresh_ui()
            elif not value:
                self.setFlag(QGraphicsItem.GraphicsItemFlag.ItemIsSelectable, False)  # we are not selectable any more by band selection until directly clicked
                pass
        return super().itemChange(change, value)

    def mousePressEvent(self, event: QGraphicsSceneMouseEvent) -> None:
        if not self._get_selectshapepath().contains(event.pos()):
            event.ignore()
            return
        self.setFlag(QGraphicsItem.GraphicsItemFlag.ItemIsSelectable, True)  # if we are clicked - we are now selectable until unselected. This is to avoid band selection
        super().mousePressEvent(event)
        self.__press_pos = event.scenePos()

        if event.button() == Qt.MouseButton.RightButton:
            # context menu time
            view = event.widget().parent()
            assert isinstance(view, GraphicsSceneViewingWidgetBase)
            view.item_requests_context_menu(self)
        event.accept()

    def mouseMoveEvent(self, event: QGraphicsSceneMouseEvent) -> None:
        if self.__ui_interactor is None:
            movedist = event.scenePos() - self.__press_pos
            if QPointF.dotProduct(movedist, movedist) > 2500:  # TODO: config this rad squared
                self.__ui_interactor = SceneTaskPreview(self)
                self.scene().addItem(self.__ui_interactor)
        if self.__ui_interactor:
            self.__ui_interactor.mouseMoveEvent(event)
        else:
            super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event: QGraphicsSceneMouseEvent) -> None:
        if self.__ui_interactor:
            self.__ui_interactor.mouseReleaseEvent(event)
            nodes = [x for x in self.scene().items(event.scenePos(), Qt.ItemSelectionMode.IntersectsItemBoundingRect) if isinstance(x, Node)]  # TODO: dirty, implement such method in one of scene subclasses
            if len(nodes) > 0:
                logger.debug(f'moving item {self} to node {nodes[0]}')
                self.__data_controller.request_set_task_node(self.get_id(), nodes[0].get_id())
            call_later(self.__ui_interactor.scene().removeItem, self.__ui_interactor)
            self.__ui_interactor = None

        else:
            super().mouseReleaseEvent(event)

    @staticmethod
    def _draw_dict_table(attributes: dict, table_name: str):
        imgui.columns(2, table_name)
        imgui.separator()
        imgui.text('name')
        imgui.next_column()
        imgui.text('value')
        imgui.next_column()
        imgui.separator()
        for key, val in attributes.items():
            imgui.text(key)
            imgui.next_column()
            imgui.text(repr(val))
            imgui.next_column()
        imgui.columns(1)

    def request_update_meta_if_needed(self):
        """
        note, this is just a request, actual update will come later
        """
        if not self.__meta_needs_to_be_requested:
            return
        self.__meta_needs_to_be_requested = False
        self.__data_controller.request_log_meta(self.get_id())  # update all task metadata: which nodes it ran on and invocation numbers only
        self.__data_controller.request_attributes(self.get_id())

    #
    # interface
    def draw_imgui_elements(self, drawing_widget):
        self.request_update_meta_if_needed()

        imgui.text(f'Task {self.get_id()} {self.name()}')
        imgui.text(f'state: {self.state().name}')
        imgui.text(f'groups: {", ".join(self.groups())}')
        imgui.text(f'parent id: {self.parent_task_id()}')
        imgui.text(f'children count: {self.children_tasks_count()}')
        imgui.text(f'split level: {self.split_level()}')
        imgui.text(f'invocation attempts: {self.latest_invocation_attempt()}')

        # first draw attributes
        if self.attributes():
            self._draw_dict_table(self.attributes(), 'node_task_attributes')

        if env_res_args := self.environment_attributes():
            tab_expanded, _ = imgui.collapsing_header(f'environment resolver attributes##collapsing_node_task_environment_resolver_attributes')
            if tab_expanded:
                imgui.text(f'environment resolver: "{env_res_args.name()}"')
                if env_res_args.arguments():
                    self._draw_dict_table(env_res_args.arguments(), 'node_task_environment_resolver_attributes')

        # now draw log
        imgui.text('Logs:')
        for node_id, invocs in self.invocation_logs_mapping().items():
            node: Node = self.__scene_container.get_node(node_id)
            if node is None:
                logger.warning(f'node for task {self.get_id()} does not exist')
                continue
            node_name: str = node.node_name()
            node_expanded, _ = imgui.collapsing_header(f'node {node_id}' + (f' "{node_name}"' if node_name else ''))
            if not node_expanded:  # or invocs is None:
                continue
            for invoc_id, invoc_log in invocs.items():
                # TODO: pyimgui is not covering a bunch of fancy functions... watch when it's done
                imgui.indent(10)
                invoc_expanded, _ = imgui.collapsing_header(f'invocation {invoc_id}' +
                                                            (f', worker {invoc_log.worker_id}' if isinstance(invoc_log, InvocationLogData) is not None else '') +
                                                            f', time: {timedelta(seconds=round(invoc_log.invocation_runtime)) if invoc_log.invocation_runtime is not None else "N/A"}' +
                                                            f'###logentry_{invoc_id}')
                if not invoc_expanded:
                    imgui.unindent(10)
                    continue
                if invoc_id not in self.__requested_invocs_while_selected:
                    self.__requested_invocs_while_selected.add(invoc_id)
                    self.__data_controller.request_log(invoc_id)
                if isinstance(invoc_log, IncompleteInvocationLogData):
                    imgui.text('...fetching...')
                else:
                    if invoc_log.stdout:
                        if imgui.button(f'open in viewer##{invoc_id}'):
                            fetch_and_open_log_viewer(self.__data_controller, invoc_id, drawing_widget, update_interval=None if invoc_log.invocation_state == InvocationState.FINISHED else 5)

                        imgui.text_unformatted(invoc_log.stdout or '...nothing here...')
                    if invoc_log.invocation_state == InvocationState.IN_PROGRESS:
                        if imgui.button('update'):
                            logger.debug('clicked')
                            if invoc_id in self.__requested_invocs_while_selected:
                                self.__requested_invocs_while_selected.remove(invoc_id)
                imgui.unindent(10)
