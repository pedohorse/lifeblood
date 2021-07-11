import json
from types import MappingProxyType

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .basenode import BaseNode


class ProcessingContext:
    def __init__(self, node: "BaseNode", task_dict: dict):
        task_dict = dict(task_dict)
        self.__task_attributes = json.loads(task_dict.get('attributes', '{}'))
        self.__task_dict = task_dict
        self.__node = node

    def param_value(self, param_name: str):
        return self.__node.get_ui().parameter(param_name).value(self)

    def locals(self):
        return {'task': self.__task_attributes}

    def task_attribute(self, attrib_name: str):
        return self.__task_attributes[attrib_name]

    def task_attributes(self) -> MappingProxyType:
        return MappingProxyType(self.__task_attributes)

    def task_field(self, field_name: str, default_value=None):
        return self.__task_dict.get(field_name, default_value)
