from lifeblood.taskspawn import NewTask
from lifeblood_viewer.task_group_actions import CannotParseData, TaskGroupDef, TaskGroupViewerAction, TaskGroupViewerActionAttributeDef
from dataclasses import dataclass

from typing import Any, Dict, List, Type


@dataclass
class TaskGroupViewerSubmitAction(TaskGroupViewerAction):
    tasks: Dict[str, NewTask]
    groups: List[TaskGroupDef]
    attribute_substitutions: Dict[str, Dict[str, TaskGroupViewerActionAttributeDef]]

    @classmethod
    def from_data_dict(cls, action_data: dict, *, original_user_data: bytes) -> "TaskGroupViewerAction":
        for entry_name, entry_type, elem_type in (
                ('tasks', dict, (str, str)), ('groups', list, dict)
        ):
            if entry_name not in action_data:
                raise CannotParseData(f'mandatory field "{entry_name}" not found')
            if not isinstance(action_data[entry_name], entry_type):
                raise CannotParseData(f'mandatory field "{entry_name}" is not of expected type "{entry_type}"')
            if elem_type is not None:
                if entry_type is list:
                    if not _check_list_type(action_data[entry_name], elem_type):
                        raise CannotParseData(f'mandatory field "{entry_name}" elements are not of expected type "{elem_type}"')
                elif entry_type is dict:
                    if not _check_dict_type(action_data[entry_name], *elem_type):
                        raise CannotParseData(f'mandatory field "{entry_name}" kvp elements are not of expected type "{elem_type}"')

        action_tasks = {k: NewTask.deserialize(v.encode('latin1')) for k, v in action_data['tasks'].items()}
        action_attribute_substitutions = {}
        action_groups = []
        for x in action_data['groups']:
            agroup = TaskGroupDef.deserialize_from_data(x)
            action_groups.append(agroup)
            # special recursion parameter check
            if x.get('set_self_as_user_data'):
                agroup.user_data = original_user_data

        if 'attribute_substitutions' in action_data:
            if not isinstance(action_data['attribute_substitutions'], dict):
                raise CannotParseData('optional field "attribute_substitutions" is not of expected type "dict"')
            for task_key, sub_data in action_data['attribute_substitutions'].items():
                if not isinstance(task_key, str) or not isinstance(sub_data, dict):
                    raise CannotParseData('optional field "attribute_substitutions" list elements are not of expected type')
                for key, val in sub_data.items():
                    if not isinstance(key, str) or not isinstance(val, dict):
                        raise CannotParseData('optional field "attribute_substitutions" list elements are not of expected type')
                    action_attr_def = TaskGroupViewerActionAttributeDef.deserialize_from_data(val)
                    action_attribute_substitutions.setdefault(task_key, {})[key] = action_attr_def

        return TaskGroupViewerSubmitAction(
                    'submit',
                    action_tasks,
                    action_groups,
                    action_attribute_substitutions,
                )


def _check_list_type(val: Any, elem_type: Type) -> bool:
    if not isinstance(val, list):
        return False
    for x in val:
        if not isinstance(x, elem_type):
            return False
    return True


def _check_dict_type(val: Any, key_type: Type, val_type: Type) -> bool:
    if not isinstance(val, dict):
        return False
    for key, elem in val.items():
        if not isinstance(key, key_type) or not isinstance(elem, val_type):
            return False
    return True
