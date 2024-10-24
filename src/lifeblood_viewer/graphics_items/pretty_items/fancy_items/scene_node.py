import imgui
from lifeblood import logging
from lifeblood.config import get_config
from lifeblood.enums import NodeParameterType
from lifeblood.node_parameters import CollapsableVerticalGroup, OneLineParametersLayout, Parameter, ParameterExpressionError, ParametersLayoutBase, Separator
from lifeblood.node_ui import NodeUi
from lifeblood_viewer.graphics_items import Node
from ...utils import call_later
from ..decorated_node import DecoratedNode
from ..drawable_node_with_snap_points import DrawableNodeWithSnapPoints
from ..node_connection_create_preview import NodeConnectionCreatePreview
from ..node_connection_snap_point import NodeConnSnapPoint
from ...graphics_scene_container import GraphicsSceneWithNodesAndTasks

from lifeblood_viewer.scene_data_controller import SceneDataController
from lifeblood_viewer.code_editor.editor import StringParameterEditor
from lifeblood_viewer.graphics_scene_viewing_widget import GraphicsSceneViewingWidgetBase
from ..node_decorator_base import NodeDecoratorFactoryBase

from PySide6.QtCore import Qt, Slot, QPointF
from PySide6.QtGui import QDesktopServices, QPainter
from PySide6.QtWidgets import QGraphicsItem, QStyleOptionGraphicsItem, QGraphicsSceneMouseEvent, QWidget

from typing import Iterable, Optional


logger = logging.get_logger('viewer')


class SceneNode(DecoratedNode):
    base_height = 100
    base_width = 150

    def __init__(self, scene: GraphicsSceneWithNodesAndTasks, id: int, type: str, name: str, data_controller: SceneDataController, node_decorator_factories: Iterable[NodeDecoratorFactoryBase] = ()):
        super().__init__(scene, id, type, name, node_decorator_factories)
        self.__scene_container = scene
        self.__data_controller: SceneDataController = data_controller

        # display
        self.setFlags(QGraphicsItem.GraphicsItemFlag.ItemIsMovable | QGraphicsItem.GraphicsItemFlag.ItemIsSelectable | QGraphicsItem.GraphicsItemFlag.ItemSendsGeometryChanges)
        self.setAcceptHoverEvents(True)
        self.__nodeui_menucache = {}
        self.__ui_selected_tab = 0

        self.__ui_interactor = None
        self.__ui_grabbed_conn = None
        self.__ui_widget: Optional[GraphicsSceneViewingWidgetBase] = None

        self.__move_start_position = None
        self.__move_start_selection = None

        self.__node_ui_for_io_requested = False

        # misc
        self.__manual_url_base = get_config('viewer').get_option_noasync('manual_base_url', 'https://pedohorse.github.io/lifeblood')

    def apply_settings(self, settings_name: str):
        self.__data_controller.request_apply_node_settings(self.get_id(), settings_name)

    def pause_all_tasks(self):
        self.__data_controller.set_tasks_paused([x.get_id() for x in self.tasks_iter()], True)

    def resume_all_tasks(self):
        self.__data_controller.set_tasks_paused([x.get_id() for x in self.tasks_iter()], False)

    def update_nodeui(self, nodeui: NodeUi):
        super().update_nodeui(nodeui)
        self.__nodeui_menucache = {}

    def paint(self, painter: QPainter, option: QStyleOptionGraphicsItem, widget: Optional[QWidget] = None) -> None:
        # TODO: this request from paint here is SUS, guess it's needed to ensure inputs-outputs and color are displayed properly
        if not self.__node_ui_for_io_requested:
            self.__node_ui_for_io_requested = True
            self.__data_controller.request_node_ui(self.get_id())

        super().paint(painter, option, widget)

    #
    # interface

    # helper
    def __draw_single_item(self, item, size=(1.0, 1.0), drawing_widget=None):
        if isinstance(item, Parameter):
            if not item.visible():
                return
            param_name = item.name()
            param_label = item.label() or ''
            parent_layout = item.parent()
            idstr = f'_{self.get_id()}'
            assert isinstance(parent_layout, ParametersLayoutBase)
            imgui.push_item_width(imgui.get_window_width() * parent_layout.relative_size_for_child(item)[0] * 2 / 3)

            changed = False
            expr_changed = False

            new_item_val = None
            new_item_expression = None

            try:
                if item.has_expression():
                    with imgui.colored(imgui.COLOR_FRAME_BACKGROUND, 0.1, 0.4, 0.1):
                        expr_changed, newval = imgui.input_text('##'.join((param_label, param_name, idstr)), item.expression(), 256, flags=imgui.INPUT_TEXT_ENTER_RETURNS_TRUE)
                    if expr_changed:
                        new_item_expression = newval
                elif item.has_menu():
                    menu_order, menu_items = item.get_menu_items()

                    if param_name not in self.__nodeui_menucache:
                        self.__nodeui_menucache[param_name] = {'menu_items_inv': {v: k for k, v in menu_items.items()},
                                                               'menu_order_inv': {v: i for i, v in enumerate(menu_order)}}

                    menu_items_inv = self.__nodeui_menucache[param_name]['menu_items_inv']
                    menu_order_inv = self.__nodeui_menucache[param_name]['menu_order_inv']
                    if item.is_readonly() or item.is_locked():  # TODO: treat locked items somehow different, but for now it's fine
                        imgui.text(menu_items_inv[item.value()])
                        return
                    else:
                        changed, val = imgui.combo('##'.join((param_label, param_name, idstr)), menu_order_inv[menu_items_inv[item.value()]], menu_order)
                        if changed:
                            new_item_val = menu_items[menu_order[val]]
                else:
                    if item.is_readonly() or item.is_locked():  # TODO: treat locked items somehow different, but for now it's fine
                        imgui.text(f'{item.value()}')
                        if item.label():
                            imgui.same_line()
                            imgui.text(f'{item.label()}')
                        return
                    param_type = item.type()
                    if param_type == NodeParameterType.BOOL:
                        changed, newval = imgui.checkbox('##'.join((param_label, param_name, idstr)), item.value())
                    elif param_type == NodeParameterType.INT:
                        #changed, newval = imgui.slider_int('##'.join((param_label, param_name, idstr)), item.value(), 0, 10)
                        slider_limits = item.display_value_limits()
                        if slider_limits[0] is not None:
                            changed, newval = imgui.slider_int('##'.join((param_label, param_name, idstr)), item.value(), *slider_limits)
                        else:
                            changed, newval = imgui.input_int('##'.join((param_label, param_name, idstr)), item.value(), flags=imgui.INPUT_TEXT_ENTER_RETURNS_TRUE)
                        if imgui.begin_popup_context_item(f'item context menu##{param_name}', 2):
                            imgui.selectable('toggle expression')
                            imgui.end_popup()
                    elif param_type == NodeParameterType.FLOAT:
                        #changed, newval = imgui.slider_float('##'.join((param_label, param_name, idstr)), item.value(), 0, 10)
                        slider_limits = item.display_value_limits()
                        if slider_limits[0] is not None and slider_limits[1] is not None:
                            changed, newval = imgui.slider_float('##'.join((param_label, param_name, idstr)), item.value(), *slider_limits)
                        else:
                            changed, newval = imgui.input_float('##'.join((param_label, param_name, idstr)), item.value(), flags=imgui.INPUT_TEXT_ENTER_RETURNS_TRUE)
                    elif param_type == NodeParameterType.STRING:
                        if item.is_text_multiline():
                            # TODO: this below is a temporary solution. it only gives 8192 extra symbols for editing, but currently there is no proper way around with current pyimgui version
                            imgui.begin_group()
                            ed_butt_pressed = imgui.small_button(f'open in external window##{param_name}')
                            changed, newval = imgui.input_text_multiline('##'.join((param_label, param_name, idstr)), item.unexpanded_value(), len(item.unexpanded_value()) + 1024*8, flags=imgui.INPUT_TEXT_ALLOW_TAB_INPUT | imgui.INPUT_TEXT_ENTER_RETURNS_TRUE | imgui.INPUT_TEXT_CTRL_ENTER_FOR_NEW_LINE)
                            imgui.end_group()
                            if ed_butt_pressed:
                                hl = StringParameterEditor.SyntaxHighlight.NO_HIGHLIGHT
                                if item.syntax_hint() == 'python':
                                    hl = StringParameterEditor.SyntaxHighlight.PYTHON
                                wgt = StringParameterEditor(syntax_highlight=hl, parent=drawing_widget)
                                wgt.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
                                wgt.set_text(item.unexpanded_value())
                                wgt.edit_done.connect(lambda x, sc=self.scene(), id=self.get_id(), it=item: sc.change_node_parameter(id, item, x))
                                wgt.set_title(f'editing parameter "{param_name}"')
                                wgt.show()
                        else:
                            changed, newval = imgui.input_text('##'.join((param_label, param_name, idstr)), item.unexpanded_value(), 256, flags=imgui.INPUT_TEXT_ENTER_RETURNS_TRUE)
                    else:
                        raise NotImplementedError()
                    if changed:
                        new_item_val = newval

                # item context menu popup
                popupid = '##'.join((param_label, param_name, idstr))  # just to make sure no names will collide with full param imgui lables
                if imgui.begin_popup_context_item(f'Item Context Menu##{popupid}', 2):
                    if item.can_have_expressions() and not item.has_expression():
                        if imgui.selectable(f'enable expression##{popupid}')[0]:
                            expr_changed = True
                            # try to turn backtick expressions into normal one
                            if item.type() == NodeParameterType.STRING:
                                new_item_expression = item.python_from_expandable_string(item.unexpanded_value())
                            else:
                                new_item_expression = str(item.value())
                    if item.has_expression():
                        if imgui.selectable(f'delete expression##{popupid}')[0]:
                            try:
                                value = item.value()
                            except ParameterExpressionError as e:
                                value = item.default_value()
                            expr_changed = True
                            changed = True
                            new_item_val = value
                            new_item_expression = None
                    imgui.end_popup()
            finally:
                imgui.pop_item_width()

            if changed or expr_changed:
                # TODO: op below may fail, so callback to display error should be provided
                self.__data_controller.change_node_parameter(self.get_id(), item,
                                                             new_item_val if changed else ...,
                                                             new_item_expression if expr_changed else ...)

        elif isinstance(item, Separator):
            imgui.separator()
        elif isinstance(item, OneLineParametersLayout):
            first_time = True
            for child in item.items(recursive=False):
                h, w = item.relative_size_for_child(child)
                if isinstance(child, Parameter):
                    if not child.visible():
                        continue
                if first_time:
                    first_time = False
                else:
                    imgui.same_line()
                self.__draw_single_item(child, (h*size[0], w*size[1]), drawing_widget=drawing_widget)
        elif isinstance(item, CollapsableVerticalGroup):
            expanded, _ = imgui.collapsing_header(f'{item.label()}##{item.name()}')
            if expanded:
                imgui.indent(5)
                for child in item.items(recursive=False):
                    h, w = item.relative_size_for_child(child)
                    self.__draw_single_item(child, (h*size[0], w*size[1]), drawing_widget=drawing_widget)
                imgui.unindent(5)
                imgui.separator()
        elif isinstance(item, ParametersLayoutBase):
            imgui.indent(5)
            for child in item.items(recursive=False):
                h, w = item.relative_size_for_child(child)
                if isinstance(child, Parameter):
                    if not child.visible():
                        continue
                self.__draw_single_item(child, (h*size[0], w*size[1]), drawing_widget=drawing_widget)
            imgui.unindent(5)
        elif isinstance(item, ParametersLayoutBase):
            for child in item.items(recursive=False):
                h, w = item.relative_size_for_child(child)
                if isinstance(child, Parameter):
                    if not child.visible():
                        continue
                self.__draw_single_item(child, (h*size[0], w*size[1]), drawing_widget=drawing_widget)
        else:
            raise NotImplementedError(f'unknown parameter hierarchy item to display {type(item)}')

    # main dude
    def draw_imgui_elements(self, drawing_widget):
        imgui.text(f'Node {self.get_id()}, type "{self.node_type()}", name {self.node_name()}')

        if imgui.selectable(f'parameters##{self.node_name()}', self.__ui_selected_tab == 0, width=imgui.get_window_width() * 0.5 * 0.7)[1]:
            self.__ui_selected_tab = 0
        imgui.same_line()
        if imgui.selectable(f'description##{self.node_name()}', self.__ui_selected_tab == 1, width=imgui.get_window_width() * 0.5 * 0.7)[1]:
            self.__ui_selected_tab = 1
        imgui.separator()

        if self.__ui_selected_tab == 0:
            if (nodeui := self.get_nodeui()) is not None:
                self.__draw_single_item(nodeui.main_parameter_layout(), drawing_widget=drawing_widget)
        elif self.__ui_selected_tab == 1:

            if (node_type := self.node_type()) in self.__data_controller.node_types() and imgui.button('open manual page'):
                plugin_info = self.__data_controller.node_types()[node_type].plugin_info
                category = plugin_info.category
                package = plugin_info.package_name
                QDesktopServices.openUrl(self.__manual_url_base + f'/nodes/{category}{f"/{package}" if package else ""}/{self.node_type()}.html')
            imgui.text(self.__data_controller.node_types()[self.node_type()].description if self.node_type() in self.__data_controller.node_types() else 'error')

    def itemChange(self, change, value):
        if change == QGraphicsItem.GraphicsItemChange.ItemSelectedHasChanged:
            if value and self.graphics_scene().get_inspected_item() == self:   # item was just selected, And is the first selected
                self.__data_controller.request_node_ui(self.get_id())
        elif change == QGraphicsItem.GraphicsItemChange.ItemPositionChange:
            if self.__move_start_position is None:
                self.__move_start_position = self.pos()
            for connection in self.all_connections():
                connection.prepareGeometryChange()

        return super().itemChange(change, value)

    def mousePressEvent(self, event: QGraphicsSceneMouseEvent):
        if event.button() == Qt.MouseButton.LeftButton and self.__ui_interactor is None:
            wgt = event.widget().parent()
            assert isinstance(wgt, GraphicsSceneViewingWidgetBase)
            pos = event.scenePos()
            r2 = (self._input_radius() + 0.5*self._line_width())**2

            # check expand button
            expand_button_shape = self._get_expandbutton_shape()
            if expand_button_shape.contains(event.pos()):
                self.set_expanded(not self.is_expanded())
                event.ignore()
                return

            for input in self.input_names():
                inpos = self.get_input_position(input)
                if QPointF.dotProduct(inpos - pos, inpos - pos) <= r2 and wgt.request_ui_focus(self):
                    snap_points = [y for x in self.__scene_container.nodes() if x != self and isinstance(x, DrawableNodeWithSnapPoints) for y in x.output_snap_points()]
                    displayer = NodeConnectionCreatePreview(None, self, '', input, snap_points, 15, self._ui_interactor_finished)
                    self.scene().addItem(displayer)
                    self.__ui_interactor = displayer
                    self.__ui_grabbed_conn = input
                    self.__ui_widget = wgt
                    event.accept()
                    self.__ui_interactor.mousePressEvent(event)
                    return

            for output in self.output_names():
                outpos = self.get_output_position(output)
                if QPointF.dotProduct(outpos - pos, outpos - pos) <= r2 and wgt.request_ui_focus(self):
                    snap_points = [y for x in self.__scene_container.nodes() if x != self and isinstance(x, DrawableNodeWithSnapPoints) for y in x.input_snap_points()]
                    displayer = NodeConnectionCreatePreview(self, None, output, '', snap_points, 15, self._ui_interactor_finished)
                    self.scene().addItem(displayer)
                    self.__ui_interactor = displayer
                    self.__ui_grabbed_conn = output
                    self.__ui_widget = wgt
                    event.accept()
                    self.__ui_interactor.mousePressEvent(event)
                    return

            if not self._get_nodeshape().contains(event.pos()):
                event.ignore()
                return

        super().mousePressEvent(event)
        self.__move_start_selection = {self}
        self.__move_start_position = None

        # check for special picking: shift+move should move all upper connected nodes
        if event.modifiers() & Qt.KeyboardModifier.ShiftModifier or event.modifiers() & Qt.KeyboardModifier.ControlModifier:
            selecting_inputs = event.modifiers() & Qt.KeyboardModifier.ShiftModifier
            selecting_outputs = event.modifiers() & Qt.KeyboardModifier.ControlModifier
            extra_selected_nodes = set()
            if selecting_inputs:
                extra_selected_nodes.update(self.input_nodes())
            if selecting_outputs:
                extra_selected_nodes.update(self.output_nodes())

            extra_selected_nodes_ordered = list(extra_selected_nodes)
            for relnode in extra_selected_nodes_ordered:
                relnode.setSelected(True)
                relrelnodes = set()
                if selecting_inputs:
                    relrelnodes.update(node for node in relnode.input_nodes() if node not in extra_selected_nodes)
                if selecting_outputs:
                    relrelnodes.update(node for node in relnode.output_nodes() if node not in extra_selected_nodes)
                extra_selected_nodes_ordered.extend(relrelnodes)
                extra_selected_nodes.update(relrelnodes)
            self.setSelected(True)
        for item in self.scene().selectedItems():
            if isinstance(item, Node):
                self.__move_start_selection.add(item)
                item.__move_start_position = None

        if event.button() == Qt.MouseButton.RightButton:
            # context menu time
            view = event.widget().parent()
            assert isinstance(view, GraphicsSceneViewingWidgetBase)
            view.item_requests_context_menu(self)
            event.accept()

    def mouseMoveEvent(self, event: QGraphicsSceneMouseEvent):
        # if self.__ui_interactor is not None:
        #     event.accept()
        #     self.__ui_interactor.mouseMoveEvent(event)
        #     return
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event: QGraphicsSceneMouseEvent):
        # if self.__ui_interactor is not None:
        #     event.accept()
        #     self.__ui_interactor.mouseReleaseEvent(event)
        #     return
        super().mouseReleaseEvent(event)
        if self.__move_start_position is not None:
            # calc final pos if snapping is involved,
            # then reset nodes to orig position and call scene's move_nodes so that proper op is generated
            nodes_final_positions = []
            for node in self.__move_start_selection:
                pos = node.pos()
                if self.__scene_container.node_snapping_enabled():
                    snapx = node.base_width / 4
                    snapy = node.base_height / 4
                    pos = QPointF(round(pos.x() / snapx) * snapx,
                                  round(pos.y() / snapy) * snapy)
                nodes_final_positions.append((node, pos))
                node.setPos(node.__move_start_position)

            self.__scene_container.move_nodes(nodes_final_positions)
            #self.scene()._nodes_were_moved([(node, node.__move_start_position) for node in self.__move_start_selection])
            for node in self.__move_start_selection:
                node.__move_start_position = None

    @Slot(object)
    def _ui_interactor_finished(self, snap_point: Optional[NodeConnSnapPoint]):
        assert self.__ui_interactor is not None
        call_later(lambda x: logger.debug(f'later removing {x}') or x.scene().removeItem(x), self.__ui_interactor)
        if self.scene() is None:  # if scheduler deleted us while interacting
            return
        if self.__ui_widget is None:
            raise RuntimeError('interaction finalizer called, but ui widget is not set')

        grabbed_conn = self.__ui_grabbed_conn
        self.__ui_widget.release_ui_focus(self)
        self.__ui_widget = None
        self.__ui_interactor = None
        self.__ui_grabbed_conn = None

        # actual node reconection
        if snap_point is None:
            logger.debug('no change')
            return

        setting_out = not snap_point.connection_is_input()
        self.__data_controller.add_connection(snap_point.node().get_id() if setting_out else self.get_id(),
                                              snap_point.connection_name() if setting_out else grabbed_conn,
                                              snap_point.node().get_id() if not setting_out else self.get_id(),
                                              snap_point.connection_name() if not setting_out else grabbed_conn)

