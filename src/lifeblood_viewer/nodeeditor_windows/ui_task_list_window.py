from imgui_bundle import imgui
from datetime import timedelta

from lifeblood.enums import TaskState
from lifeblood.logging import get_logger
from lifeblood_viewer.nodeeditor import NodeEditor
from lifeblood_viewer.ui_scene_elements import ImguiViewWindow
from ..graphics_items.pretty_items.fancy_items.scene_task import SceneTask
from ..graphics_items import Node, Task, NetworkItemWatcher
from PySide6.QtGui import QCursor

from typing import Optional


logger = get_logger('viewer.windows.task_list')


class TaskListWindow(ImguiViewWindow, NetworkItemWatcher):
    def __init__(self, editor_widget: NodeEditor):
        super().__init__(editor_widget, 'Task List')
        self.__displayed_node: Optional[Node] = None
        self.__pinned = False

    def set_display_node(self, display_node: Optional[Node]):
        if display_node == self.__displayed_node:
            return
        if self.__displayed_node:
            self.__displayed_node.remove_item_watcher(self)
        self.__displayed_node = display_node
        if self.__displayed_node:
            self.__displayed_node.add_item_watcher(self)
        self._update_title()

    def on_closed(self):
        self.set_display_node(None)

    def pin(self, pin: bool = True):
        self.__pinned = pin
        self._update_title()

    def _update_title(self):
        self.set_title(f'Task List "{self.__displayed_node.node_name() if self.__displayed_node else ""}"'
                       f'{" pinned" if self.__pinned else ""}')

    def draw_window_elements(self):
        iitem = self.scene().get_inspected_item()
        if iitem and isinstance(iitem, Node) and not self.__pinned:
            self.set_display_node(iitem)

        if self.__displayed_node is not None:
            imgui.text(f'node: {self.__displayed_node.node_name()}')
            base_name = f'table_{self._imgui_key_name()}'
            if imgui.begin_table(f'tasks##{base_name}', 6, imgui.TableFlags_.sizing_stretch_prop |
                                                             imgui.TableFlags_.borders_inner_v |
                                                             imgui.TableFlags_.row_bg |
                                                             imgui.TableFlags_.sortable
                                   ):
                imgui.table_setup_column('ID', imgui.TableColumnFlags_.default_sort)
                imgui.table_setup_column('frame(s)')
                imgui.table_setup_column('name')
                imgui.table_setup_column('total runtime')
                imgui.table_setup_column('paused', imgui.TableColumnFlags_.width_fixed | imgui.TableColumnFlags_.no_sort, 64)
                imgui.table_setup_column('state', imgui.TableColumnFlags_.width_fixed | imgui.TableColumnFlags_.no_sort, 128.0)
                imgui.table_headers_row()

                imgui.table_next_row()
                imgui.table_next_column()

                # pick sorting order


                sort_spec = imgui.table_get_sort_specs()
                sort_order = self.__displayed_node.TaskSortOrder.ID
                if sort_spec is None:
                    logger.warning('task sorting internal error')
                elif sort_spec.specs_count:
                    spec = sort_spec.get_specs(0)
                    if spec.column_index == 0:
                        if spec.sort_direction == imgui.SortDirection.ascending:
                            sort_order = self.__displayed_node.TaskSortOrder.ID
                        else:
                            sort_order = self.__displayed_node.TaskSortOrder.ID_REV
                    elif spec.column_index == 1:
                        if spec.sort_direction == imgui.SortDirection.ascending:
                            sort_order = self.__displayed_node.TaskSortOrder.FRAMES
                        else:
                            sort_order = self.__displayed_node.TaskSortOrder.FRAMES_REV
                    elif spec.column_index == 2:
                        if spec.sort_direction == imgui.SortDirection.ascending:
                            sort_order = self.__displayed_node.TaskSortOrder.NAME
                        else:
                            sort_order = self.__displayed_node.TaskSortOrder.NAME_REV
                    elif spec.column_index == 3:
                        if spec.sort_direction == imgui.SortDirection.ascending:
                            sort_order = self.__displayed_node.TaskSortOrder.TOTAL_RUNTIME
                        else:
                            sort_order = self.__displayed_node.TaskSortOrder.TOTAL_RUNTIME_REV
                    else:
                        logger.error(f'sorting by column {spec.column_index} is not implemented')
                #

                prev_task = None
                select_next_task = False
                task_to_reselect = None
                for task in self.__displayed_node.tasks_iter(order=sort_order):
                    if isinstance(task, SceneTask):
                        task.request_update_meta_if_needed()  # note that this is async, so this will just do request, old data will be drawn in this call
                    if task.isSelected():
                        imgui.table_set_bg_color(imgui.TableBgTarget_.row_bg1, imgui.color_convert_float4_to_u32((0.5, 0.5, 0.375, 0.375)))
                        if imgui.is_window_focused():
                            if imgui.is_key_pressed(imgui.Key.up_arrow, False):
                                task_to_reselect = prev_task
                            elif imgui.is_key_pressed(imgui.Key.down_arrow, False):
                                select_next_task = True
                    else:
                        imgui.table_set_bg_color(imgui.TableBgTarget_.row_bg1, 0)
                        if select_next_task:
                            select_next_task = False
                            task_to_reselect = task

                    if imgui.selectable(str(task.get_id()), False, imgui.SelectableFlags_.span_all_columns)[0]:
                        task.set_selected(True)
                    if imgui.is_item_hovered() and imgui.is_mouse_clicked(imgui.MouseButton_.right):
                        self.editor_widget().show_task_menu(task, pos=QCursor.pos())

                    imgui.table_next_column()

                    if (frames := task.attributes().get('frames')) and isinstance(frames, list):
                        if len(frames) == 1:
                            imgui.text(str(frames[0]))
                        else:
                            imgui.text(f'{frames[0]}-{frames[-1]}')
                    else:
                        imgui.text('')
                    imgui.table_next_column()

                    imgui.text(str(task.name()))
                    imgui.table_next_column()

                    imgui.text(str(timedelta(seconds=int(task.invocations_total_time(only_last_per_node=True)))))
                    imgui.table_next_column()

                    if task.paused():
                        imgui.text('paused')
                    imgui.table_next_column()

                    if task.state() == TaskState.IN_PROGRESS:
                        imgui.push_item_width(-1)
                        imgui.progress_bar((task.get_progress() or 0) / 100, (0, 0), f'{task.get_progress() or 0}%')
                    else:
                        imgui.text(task.state().name)
                    imgui.table_next_row()
                    imgui.table_next_column()
                    prev_task = task

                # if keys were pressed during frame drawing
                if task_to_reselect is not None:
                    task_to_reselect.set_selected(True)
                imgui.end_table()

    def initial_geometry(self):
        return 512, 512, 550, 300

    def shortcut_context_id(self):
        return None
