import asyncio
from enum import Enum
from typing import Dict, Optional, List, Any
from .nodethings import ProcessingResult



class AttributeType(Enum):
    INT = 0
    BOOL = 1
    FLOAT = 2
    STRING = 3


class BaseNode:
    def attribs(self) -> Dict[str, AttributeType]:
        return {}

    def attrib_value(self, attrib_name) -> Any:
        raise NotImplementedError()

    def set_attrib_value(self, attrib_name, attrib_value) -> None:
        raise NotImplementedError()

    def process_task(self, task_dict) -> ProcessingResult:
        raise NotImplementedError()

    def postprocess_task(self, task_dict) -> ProcessingResult:
        raise NotImplementedError()

    def serialize(self) -> bytes:
        raise NotImplementedError()
