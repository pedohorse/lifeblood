from lifeblood_viewer.task_group_actions import TaskGroupViewerActionPerformerBase, TaskGroupViewerAction
from lifeblood_viewer.scene_data_controller import SceneDataController
from lifeblood_viewer.long_op import LongOperationProcessor, LongOperationData, LongOperation
from lifeblood_viewer.widgets.dialogs.value_input import MultiInputDialog, IntListInputWidget
from lifeblood_viewer.task_group_actions_impl.submit_action import TaskGroupViewerSubmitAction
from PySide6.QtWidgets import QWidget


class SubmitViewerActionPerformer(TaskGroupViewerActionPerformerBase):
    def __init__(self, group_creator_name: str, data_controller: SceneDataController, op_processor: LongOperationProcessor, widget_parent: QWidget):
        self.__data_controller = data_controller
        self.__op_processor = op_processor
        self.__group_creator_name = group_creator_name
        self.__widget_parent = widget_parent

    def is_action_supported(self, action: TaskGroupViewerAction) -> bool:
        return action.type == 'submit'

    def perform_action(self, action: TaskGroupViewerAction):

        def opop(longop: LongOperation):
            assert isinstance(action, TaskGroupViewerSubmitAction)  # for ide analysis
            # mapping of requested to actual group names
            group_names_mapping = {}
            for group_def in action.groups:
                self.__data_controller.request_add_task_group(
                    group_def.name,
                    self.__group_creator_name,
                    allow_name_change_to_make_unique=True,
                    priority=group_def.priority,
                    user_data=group_def.user_data,
                    operation_data=LongOperationData(longop),
                )
                actual_task_group_name, success = yield
                if not success:
                    raise RuntimeError(f'failed to perform group creation for "{group_def.name}"')
                group_names_mapping[group_def.name] = actual_task_group_name

            for task in action.tasks.values():
                # replace group names with actual names
                task.set_extra_group_names((group_names_mapping.get(x, x) for x in task.extra_group_names()))
                # I guess no need to wait for spawn completion here...
                self.__data_controller.request_add_task(task)

        if not isinstance(action, TaskGroupViewerSubmitAction):
            raise RuntimeError('unexpected action type class')

        if action.attribute_substitutions:
            has_single_key = len(action.attribute_substitutions)
            wgt = MultiInputDialog(self.__widget_parent)
            order = []
            for task_key, overrides in action.attribute_substitutions.items():
                for attrib_name, override in overrides.items():
                    label = attrib_name if has_single_key else f'{task_key}:{attrib_name}'
                    if override.type == 'int_list':
                        wgt.add_input(label, IntListInputWidget(override.default))
                    else:
                        raise NotImplementedError(f'don\'t know how to present attribute of type "{override.type}"')
                    order.append((task_key, attrib_name))
            wgt.exec_()
            for (task_key, attrib_name), val in zip(order, wgt.get_values()):
                action.tasks[task_key].set_attribute(attrib_name, val)

        self.__op_processor.add_long_operation(opop)
