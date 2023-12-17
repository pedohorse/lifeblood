import asyncio
from typing import Optional


class NodeGraphHolderBase:
    async def get_node_input_connections(self, node_id: int, input_name: Optional[str] = None):
        raise NotImplementedError()

    async def get_node_output_connections(self, node_id: int, output_name: Optional[str] = None):
        raise NotImplementedError()

    async def node_reports_changes_needs_saving(self, node_id):
        raise NotImplementedError()

    def get_event_loop(self) -> asyncio.AbstractEventLoop:
        raise NotImplementedError()
