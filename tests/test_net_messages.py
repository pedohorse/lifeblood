import asyncio
import struct
import random
import string
from lifeblood.net_messages import ReaderMessageWrapper, WriterMessageWrapper
from unittest import IsolatedAsyncioTestCase
from io import BufferedReader, BufferedWriter, BytesIO

from typing import Optional


class DummyStream:
    def __init__(self, buffer: Optional[BytesIO] = None):
        super().__init__()
        self.__buffer = buffer or BytesIO()

    def write(self, data: bytes):
        self.__buffer.write(data)

    async def readexactly(self, size: int):
        await asyncio.sleep(0)
        return self.__buffer.read(size)

    async def drain(self):
        await asyncio.sleep(0)

    def get_buffer(self):
        return self.__buffer


class TestNetMessages(IsolatedAsyncioTestCase):
    async def test_simple_writer(self):
        stream = DummyStream()
        async with WriterMessageWrapper(stream) as writer:
            writer.write(b'test')

        buffer = stream.get_buffer()
        self.assertEqual(struct.pack('>Q?', 4, False) + b'test', buffer.getbuffer())

    async def test_empty_writer(self):
        stream = DummyStream()
        async with WriterMessageWrapper(stream, expect_answer=True) as writer:
            writer.write(b'')

        buffer = stream.get_buffer()
        self.assertEqual(struct.pack('>Q?', 0, True), buffer.getbuffer())

    async def test_random_writer(self):
        rng = random.Random(666666)
        for _ in range(666):
            data = rng.randbytes(rng.randint(0, 12345))
            reply = rng.random() > 0.5
            stream = DummyStream()
            async with WriterMessageWrapper(stream, expect_answer=reply) as writer:
                writer.write(data)

            buffer = stream.get_buffer()
            self.assertEqual(struct.pack('>Q?', len(data), reply) + data, buffer.getbuffer())

    async def test_simple_reader(self):
        stream = DummyStream(BytesIO(struct.pack('>Q?', 10, True) + b'doubletest'))
        async with ReaderMessageWrapper(stream) as reader:
            data = await reader.readexactly(10)

        self.assertEqual(b'doubletest', data)
        self.assertEqual(10, reader.message_size())
        self.assertEqual(True, reader.is_answer_expected())

    async def test_random_reader(self):
        rng = random.Random(666667)
        for _ in range(666):
            exp_data = rng.randbytes(rng.randint(0, 12345))
            reply = rng.random() > 0.5
            stream = DummyStream(BytesIO(struct.pack('>Q?', len(exp_data), reply) + exp_data))
            async with ReaderMessageWrapper(stream) as reader:
                data = await reader.readall()

            self.assertEqual(exp_data, data)
            self.assertEqual(len(exp_data), reader.message_size())
            self.assertEqual(reply, reader.is_answer_expected())
