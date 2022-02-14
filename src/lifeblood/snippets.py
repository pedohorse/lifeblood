import asyncio
import pickle
from dataclasses import dataclass

from .enums import NodeParameterType

from typing import Any, Optional, Tuple, Dict, Iterable


class NodeSnippetData:
    """
    class containing enough information to reproduce a certain snippet of nodes, with parameter values and connections ofc
    """
    @dataclass
    class ParamData:
        name: str
        type: NodeParameterType
        uvalue: Any
        expr: Optional[str]

    @dataclass
    class NodeData:
        tmpid: int
        type: str
        name: str
        parameters: Dict[str, "NodeSnippetData.ParamData"]
        pos: Tuple[float, float]

    @dataclass
    class ConnData:
        tmpout: int
        out_name: str
        tmpin: int
        in_name: str

    def __init__(self, nodes_data: Iterable[NodeData], connections_data: Iterable[ConnData]):
        self.nodes_data = list(nodes_data)
        self.connections_data = list(connections_data)

    def serialize(self) -> bytes:
        """
        serialize into bytes, not ascii-friendly
        """
        return pickle.dumps(self)

    async def serialize_async(self) -> bytes:
        return await asyncio.get_event_loop().run_in_executor(None, self.serialize)

    @classmethod
    def deserialize(cls, data: bytes) -> "NodeSnippetData":
        return pickle.loads(data)

    @classmethod
    def deserialize_async(cls, data: bytes) -> "NodeSnippetData":
        return await asyncio.get_event_loop().run_in_executor(None, cls.deserialize, data)
