import asyncio
from .basenode import BaseNode
from .node_dataprovider_base import NodeDataProvider
from .nodegraph_holder_base import NodeGraphHolderBase

from typing import Optional, Tuple


class FailedToDeserialize(RuntimeError):
    pass


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
