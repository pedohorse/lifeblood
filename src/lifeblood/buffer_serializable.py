import asyncio
from io import BufferedIOBase
from .buffered_connection import BufferedReader


class IBufferSerializable:
    def serialize(self, stream: BufferedIOBase):
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, stream: BufferedReader):
        raise NotImplementedError()

    async def serialize_to_streamwriter(self, stream: asyncio.StreamWriter):
        await asyncio.get_event_loop().run_in_executor(None, self.serialize, stream)
