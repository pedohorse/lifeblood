import json
from types import MappingProxyType

from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:
    from .basenode import BaseNode
    from .uidata import Parameter


class ProcessingContext:
    class TaskWrapper:
        def __init__(self, task_dict: dict):
            self.__attributes = json.loads(task_dict.get('attributes', '{}'))
            self.__stuff = task_dict

        def __getitem__(self, item):
            return self.__attributes[item]

        def __getattr__(self, item):
            if item in self.__stuff:
                return self.__stuff[item]
            raise AttributeError(f'task has no field {item}')

        def get(self, item, default):
            return self.__attributes.get(item, default)

    class NodeWrapper:
        def __init__(self, node: "BaseNode", context: "ProcessingContext"):
            self.__parameters: Dict[str, "Parameter"] = {x.name(): x for x in node.get_ui().parameters()}
            self.__attrs = {'name': node.name(), 'label': node.label()}
            self.__context = context

        def __getitem__(self, item):
            return self.__parameters[item].value(self.__context)

        def __getattr__(self, item):
            if item in self.__attrs:
                return self.__attrs[item]
            raise AttributeError(f'node has no field {item}')

    def __init__(self, node: "BaseNode", task_dict: dict):
        task_dict = dict(task_dict)
        self.__task_attributes = json.loads(task_dict.get('attributes', '{}'))
        self.__task_dict = task_dict
        self.__task_wrapper = ProcessingContext.TaskWrapper(task_dict)
        self.__node_wrapper = ProcessingContext.NodeWrapper(node, self)
        self.__node = node

    def param_value(self, param_name: str):
        return self.__node.get_ui().parameter(param_name).value(self)

    def locals(self):
        return {'task': self.__task_wrapper, 'node': self.__node_wrapper}

    def task_attribute(self, attrib_name: str):
        return self.__task_attributes[attrib_name]

    def task_has_attribute(self, attrib_name: str):
        return attrib_name in self.__task_attributes

    def task_attributes(self) -> MappingProxyType:
        return MappingProxyType(self.__task_attributes)

    def task_field(self, field_name: str, default_value=None):
        return self.__task_dict.get(field_name, default_value)

    def task_has_field(self, field_name: str):
        return field_name in self.__task_dict
