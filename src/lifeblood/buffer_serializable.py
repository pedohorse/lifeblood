import asyncio
from io import BufferedIOBase, BytesIO
from .buffered_connection import BufferedReader


class IBufferSerializable:
    def serialize(self, stream: BufferedIOBase):
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, stream: BufferedReader):
        raise NotImplementedError()

    async def serialize_to_streamwriter(self, stream: asyncio.StreamWriter):
        buff = BytesIO()
        await asyncio.get_event_loop().run_in_executor(None, self.serialize, buff)
        stream.write(buff.getbuffer())
