import asyncio
from .basenode import BaseNode
from .node_dataprovider_base import NodeDataProvider
from .nodegraph_holder_base import NodeGraphHolderBase

from typing import Iterable, List, Optional, Tuple


class FailedToDeserialize(RuntimeError):
    pass


class IncompatibleDeserializationMethod(FailedToDeserialize):
    pass


class FailedToApplyNodeState(FailedToDeserialize):
    def __init__(self, wrapped_expection: Exception):
        self.wrapped_exception: Exception = wrapped_expection


class FailedToApplyParameters(FailedToDeserialize):
    def __init__(self, bad_parameters: Iterable[str]):
        self.bad_parameters: List[str] = list(bad_parameters)


class NodeSerializerBase:
    def serialize(self, node: BaseNode) -> Tuple[bytes, Optional[bytes]]:
        raise NotImplementedError()

    def serialize_state_only(self, node: BaseNode) -> Optional[bytes]:
        raise NotImplementedError()

    def deserialize(self, parent: NodeGraphHolderBase, node_id: int, node_data_provider: NodeDataProvider, data: bytes, state: Optional[bytes]) -> BaseNode:
        raise NotImplementedError()

    async def deserialize_async(self, parent: NodeGraphHolderBase, node_id: int, node_data_provider: NodeDataProvider, data: bytes, state: Optional[bytes]) -> BaseNode:
        return await asyncio.get_event_loop().run_in_executor(None, self.deserialize, parent, node_id, node_data_provider, data, state)

    async def serialize_async(self, node: BaseNode) -> Tuple[bytes, Optional[bytes]]:
        return await asyncio.get_event_loop().run_in_executor(None, self.serialize, node)
