from dataclasses import dataclass
from lifeblood.taskspawn import NewTask
from lifeblood.logging import get_logger
import json

from typing import Any, Dict, Iterable, List, Optional, Type


logger = get_logger('viewer.task_group_actions')


class ActionDeserializationError(RuntimeError):
    pass


class CannotParseData(ActionDeserializationError):
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

    @classmethod
    def from_data_dict(cls, action_data: dict, *, original_user_data: bytes) -> "TaskGroupViewerAction":
        raise NotImplementedError()


class TaskGroupViewerActionRegistry:
    def __init__(self):
        self.__registered_task_group_viewer_actions: Dict[str, List[Type[TaskGroupViewerAction]]] = {}

    def register_action_type(self, supported_types: Iterable[str], action_type_class: Type[TaskGroupViewerAction]):
        for type_name in supported_types:
            self.__registered_task_group_viewer_actions.setdefault(type_name, []).append(action_type_class)

    def from_user_data(self, user_data: bytes) -> Dict[str, TaskGroupViewerAction]:
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
                if action_type not in self.__registered_task_group_viewer_actions:
                    logger.error(f'unsupported task group action type "{action_type}" is ignored')
                    continue
                for action_type_class in self.__registered_task_group_viewer_actions[action_type]:
                    try:
                        action = action_type_class.from_data_dict(action_data, original_user_data=user_data)
                        break
                    except ActionDeserializationError as e:
                        logger.error(f'failed to build action type "{action_type_class}", trying another type if any')
                        continue
                else:
                    logger.error(f'none of handlers registered for "{action_type}" were able to build action, action is ignored')
                    continue
                actions[action_name] = action

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
