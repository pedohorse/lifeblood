import imgui
from itertools import chain, repeat
from lifeblood.text import generate_name
from PySide2.QtGui import QCursor
from lifeblood_viewer.nodeeditor import NodeEditor
from lifeblood_viewer.ui_scene_elements import ImguiViewPopup


class CreateNodePopup(ImguiViewPopup):
    def __init__(self, editor_widget: NodeEditor):
        super().__init__(editor_widget, 'Create Node')
        self.__node_type_input = ''
        self.__menu_popup_selection_id = 0
        self.__menu_popup_selection_name = ()
        self.__menu_popup_arrow_down = False

    def shortcut_context_id(self) -> str:
        return 'create_node'

    def popup(self):
        super().popup()
        self.__node_type_input = ''
        self.__menu_popup_selection_id = 0
        self.__menu_popup_selection_name = ()
        self.__menu_popup_arrow_down = False

        self.scene().request_node_types_update()
        self.scene().request_node_presets_update()

    def draw_window_elements(self):
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
        for (entity_type, entity_type_label, package), (type_name, type_meta) in chain(zip(repeat(('node', None, None)), self.editor_widget().node_types().items()),
                                                                                       zip(repeat(('vpreset', 'preset', None)), self.editor_widget().viewer_presets_metadata().items()),
                                                                                       *(zip(repeat(('spreset', 'preset', pkg)), pkgdata.items()) for pkg, pkgdata in self.editor_widget().scheduler_presets_metadata().items())):

            inparts = [x.strip() for x in self.__node_type_input.split(' ')]
            label = type_meta.label
            tags = type_meta.tags
            if all(x in type_name
                   or any(t.startswith(x) for t in tags)
                   or x in label for x in inparts):  # TODO: this can be cached
                selected = self.__menu_popup_selection_id == item_number
                if entity_type_label is not None:
                    label += f' ({entity_type_label})'
                _, selected = imgui.selectable(f'{label}##popup_selectable', selected=selected, flags=imgui.SELECTABLE_DONT_CLOSE_POPUPS)
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
            self._close()

            if self.__menu_popup_selection_name:
                package, entity_name, label, entity_type = self.__menu_popup_selection_name
                if entity_type == 'node':
                    self.scene().create_node(entity_name, f'{label} {generate_name(5, 7)}', self.editor_widget().mapToScene(imguio.mouse_pos.x, imguio.mouse_pos.y))
                elif entity_type == 'vpreset':
                    self.editor_widget().create_from_viewer_preset(entity_name, self.editor_widget().mapToScene(self.editor_widget().mapFromGlobal(QCursor.pos())))
                elif entity_type == 'spreset':
                    self.editor_widget().create_from_scheduler_preset(package, entity_name, self.editor_widget().mapToScene(self.editor_widget().mapFromGlobal(QCursor.pos())))
        elif self.__menu_popup_arrow_down:
            self.__menu_popup_arrow_down = False

        elif imguio.keys_down[imgui.KEY_ESCAPE]:
            self._close()
            self.__node_type_input = ''
            self.__menu_popup_selection_id = 0
