import asyncio
from enum import Enum
from typing import Dict


class AttributeType(Enum):
    INT = 0
    BOOL = 1
    FLOAT = 2
    STRING = 3


class BaseNode:
    def attribs(self) -> Dict[str, AttributeType]:
        return {}

    def attrib_value(self, attrib_name):
        raise NotImplementedError()

    def set_attrib_value(self, attrib_name, attrib_value):
        raise NotImplementedError()

    def process_task(self, task_dict):
        raise NotImplementedError()

    def postprocess_task(self, task_dict):
        raise NotImplementedError()

    def serialize(self):
        raise NotImplementedError()
