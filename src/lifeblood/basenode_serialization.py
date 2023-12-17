import asyncio
from .basenode import BaseNode
from .node_dataprovider_base import NodeDataProvider
from .nodegraph_holder_base import NodeGraphHolderBase

class FailedToDeserialize(RuntimeError):
    pass


class NodeSerializerBase:
    def serialize(self, node: BaseNode) -> bytes:
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, parent: NodeGraphHolderBase, node_id: int, node_data_provider: NodeDataProvider, data: bytes) -> BaseNode:
        raise NotImplementedError()

    @classmethod
    async def deserialize_async(cls, parent: NodeGraphHolderBase, node_id: int, node_data_provider: NodeDataProvider, data: bytes) -> BaseNode:
        return await asyncio.get_event_loop().run_in_executor(None, cls.deserialize, parent, node_id, node_data_provider, data)

    async def serialize_async(self, node: BaseNode) -> bytes:
        return await asyncio.get_event_loop().run_in_executor(None, self.serialize, node)
