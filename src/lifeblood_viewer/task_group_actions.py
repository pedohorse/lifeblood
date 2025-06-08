from dataclasses import dataclass
from lifeblood.taskspawn import NewTask
import json

from typing import Any, Dict, List, Optional, Type


class CannotParseData(RuntimeError):
    pass


@dataclass
class TaskGroupViewerActionAttributeDef:
    type: str
    default: Any

    @classmethod
    def deserialize_from_bytes(cls, data_bytes: bytes) -> "TaskGroupViewerActionAttributeDef":
        try:
            data = json.loads(data_bytes.decode('UTF-8'))
        except UnicodeDecodeError:
            raise CannotParseData('data is not unicode') from None
        except json.JSONDecodeError:
            raise CannotParseData('data is not json') from None
        return cls.deserialize_from_data(data)

    @classmethod
    def deserialize_from_data(cls, data: dict) -> "TaskGroupViewerActionAttributeDef":
        def_type = data.get('type')
        def_default = data.get('default')
        if def_type is None:
            raise CannotParseData('mandatory field "type" not found')
        if def_default is None:
            raise CannotParseData('mandatory field "default" not found')

        if def_type == 'int_list':
            if not isinstance(def_default, list) or any(not isinstance(x, int) for x in def_default):
                raise CannotParseData('type of default does not match type "int_list"')
        else:
            raise CannotParseData(f'unknown definition type "{def_type}')

        return TaskGroupViewerActionAttributeDef(
            def_type,
            def_default,
        )


@dataclass
class TaskGroupDef:
    name: str
    priority: float
    user_data: Optional[bytes]

    @classmethod
    def deserialize_from_data(cls, data: dict) -> "TaskGroupDef":
        for field_name, field_type in (('name', str), ('priority', float)):
            if field_name not in data:
                raise CannotParseData(f'group definition field "{field_name}" not found')
            if not isinstance(data[field_name], field_type):
                raise CannotParseData(f'group definition filed "{field_name}" is not of expected type "{field_type}"')
        if 'user_data' not in data:
            raise CannotParseData('group definition field "actions" not found')
        if data['user_data'] is not None and not isinstance(data['user_data'], str):
            raise CannotParseData('group definition filed "name" is not of expected type "str"')

        return TaskGroupDef(
            data['name'],
            float(data['priority']),
            data['user_data'].encode('latin') if data['user_data'] is not None else None,
        )

    def serialize_to_data(self) -> dict:
        return {
            'name': self.name,
            'priority': self.priority,
            'user_data': self.user_data.decode('latin1') if self.user_data else None,
        }


@dataclass
class TaskGroupViewerAction:
    type: str
    tasks: Dict[str, NewTask]
    groups: List[TaskGroupDef]
    attribute_substitutions: Dict[str, Dict[str, TaskGroupViewerActionAttributeDef]]

    @classmethod
    def from_user_data(cls, user_data: bytes) -> Dict[str, "TaskGroupViewerAction"]:
        try:
            data = json.loads(user_data.decode('UTF-8'))
        except UnicodeDecodeError:
            raise CannotParseData('data is not unicode') from None
        except json.JSONDecodeError:
            raise CannotParseData('data is not json') from None

        actions = {}
        if actions_data := data.get('actions', {}):
            if not isinstance(actions_data, dict):
                raise CannotParseData('bad data: actions is not a dict')
            for action_name, action_data in actions_data.items():
                # check mandatory fields
                if 'type' not in action_data:
                    raise CannotParseData(f'mandatory field "type" not found')
                action_type = action_data['type']

                if action_type == 'submit':
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
                            agroup.user_data = user_data

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
                elif action_type == 'noop':
                    action_tasks = []
                    action_groups = []
                    action_attribute_substitutions = {}
                else:
                    # TODO: log and skip
                    raise NotImplementedError(f'unknown action type "{action_type}"')

                actions[action_name] = TaskGroupViewerAction(
                    action_type,
                    action_tasks,
                    action_groups,
                    action_attribute_substitutions,
                )

        return actions


class ActionTypeNotSupported(RuntimeError):
    def __init__(self, action_type: str):
        self.action_type = action_type


class TaskGroupViewerActionPerformerBase:
    def is_action_supported(self, action: TaskGroupViewerAction) -> bool:
        raise NotImplementedError()

    def perform_action(self, action: TaskGroupViewerAction):
        """
        should raise ActionTypeNotSupported if action type is not supported
        """
        raise NotImplementedError()


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
