import asyncio
from enum import Enum
from typing import TYPE_CHECKING, Dict, Optional, List, Any

if TYPE_CHECKING:
    from .taskspawn import TaskSpawn
    from .invocationjob import InvocationJob


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

    def process_task(self, task_dict) -> (InvocationJob, Optional[List[TaskSpawn]]):
        raise NotImplementedError()

    def postprocess_task(self, task_dict) -> (Dict[str, Any], Optional[List[TaskSpawn]]):
        raise NotImplementedError()

    def serialize(self) -> bytes:
        raise NotImplementedError()
