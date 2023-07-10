import asyncio
import struct
from io import BytesIO

from typing import Optional


class WriterMessageWrapper:
    """
    wraps writes into "messages" that have predictable borders, that can be redirected easily
    """
    def __init__(self, writer: asyncio.StreamWriter, expect_answer=False):
        self.__writer = writer
        self.__buffer: Optional[BytesIO] = None
        self.__size = 0
        self.__expect_answer = expect_answer

    def set_expect_answer(self, expect_answer):
        self.__expect_answer = expect_answer

    async def __aenter__(self) -> "WriterMessageWrapper":
        self.__buffer = BytesIO()
        self.__size = 0
        return self

    def write(self, data: bytes):
        self.__size += len(data)
        self.__buffer.write(data)

    def write_string(self, s: str):
        b = s.encode('UTF-8')
        self.write(struct.pack('>Q', len(b)))
        self.write(b)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.__writer.write(struct.pack('>Q?', self.__size, self.__expect_answer))
        self.__writer.write(self.__buffer.getbuffer())
        await self.__writer.drain()


class ReaderMessageWrapper:
    def __init__(self, reader: asyncio.StreamReader):
        self.__reader = reader
        self.__message_size: Optional[int] = None
        self.__expects_answer: Optional[bool] = None
        self.__already_read = 0

    def message_size(self):
        return self.__message_size

    def is_answer_expected(self):
        return self.__expects_answer

    async def __aenter__(self) -> "ReaderMessageWrapper":
        self.__message_size, self.__expects_answer = struct.unpack('>Q?', await self.__reader.readexactly(9))
        self.__already_read = 0
        return self

    async def readexactly(self, size: int):
        self.__already_read += size
        return await self.__reader.readexactly(size)

    async def readall(self):
        return await self.__reader.readexactly(self.__message_size - self.__already_read)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
