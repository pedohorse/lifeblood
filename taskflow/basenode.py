import asyncio
from enum import Enum
from typing import Dict, Optional, List, Any
from .nodethings import ProcessingResult
from .enums import NodeAttributeType
from .uidata import NodeUi


class BaseNode:
    def attribs(self) -> Dict[str, NodeAttributeType]:
        return {}

    def attrib_value(self, attrib_name) -> Any:
        raise NotImplementedError()

    def set_attrib_value(self, attrib_name, attrib_value) -> None:
        raise NotImplementedError()

    def get_nodeui(self):
        return NodeUi()

    def process_task(self, task_dict) -> ProcessingResult:
        raise NotImplementedError()

    def postprocess_task(self, task_dict) -> ProcessingResult:
        raise NotImplementedError()

    def serialize(self) -> bytes:
        raise NotImplementedError()
