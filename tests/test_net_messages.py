import asyncio
import struct
import random
import string
import uuid
from lifeblood.net_messages import ReaderStreamMessageWrapper, WriterStreamMessageWrapper, Message
from unittest import IsolatedAsyncioTestCase
from io import BufferedReader, BufferedWriter, BytesIO

from typing import Optional


class DummyStream:
    def __init__(self, buffer: Optional[BytesIO] = None):
        super().__init__()
        self.__buffer = buffer or BytesIO()

    def write(self, data: bytes):
        self.__buffer.write(data)

    def rewind(self):
        self.__buffer.seek(0)

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
        async with WriterStreamMessageWrapper(stream, destination='foo;бар') as writer:
            writer.write(b'test')

        buffer = stream.get_buffer()
        self.assertEqual(struct.pack('>QQ?', 23, 10, False) + b'foo;\xd0\xb1\xd0\xb0\xd1\x80' + b'test', bytes(buffer.getbuffer()))

    async def test_empty_writer(self):
        stream = DummyStream()
        async with WriterStreamMessageWrapper(stream, destination='bee;boo') as writer:
            writer.write(b'')

        buffer = stream.get_buffer()
        self.assertEqual(struct.pack('>QQ?', 16, 7, False) + b'bee;boo', bytes(buffer.getbuffer()))

    async def test_random_writer(self):
        rng = random.Random(666666)
        for _ in range(666):
            data = rng.randbytes(rng.randint(0, 12345))
            stream = DummyStream()
            dest = ''.join(rng.choice(string.ascii_letters) for _ in range(rng.randint(0, 100)))
            session = uuid.UUID(bytes=rng.randbytes(16)) if rng.random() > 0.5 else None
            async with WriterStreamMessageWrapper(stream, destination=dest, session=session) as writer:
                writer.write(data)

            stream.rewind()

            msg = await Message.from_stream_reader(stream)
            self.assertEqual(len(data), msg.message_size())
            self.assertEqual(data, msg.message_body())
            self.assertEqual(session, msg.message_session())
            self.assertEqual(dest, msg.message_destination())

    async def test_simple_reader(self):
        stream = DummyStream(BytesIO(struct.pack('>QQ?', 24, 5, False) + b'fooba' + b'doubletest'))
        async with ReaderStreamMessageWrapper(stream) as reader:
            data = await reader.readexactly(10)

        self.assertEqual(b'doubletest', data)
        self.assertEqual(10, reader.message_size())
        self.assertEqual('fooba', reader.message_destination())

    async def test_random_reader(self):
        rng = random.Random(666667)
        for _ in range(666):
            exp_data = rng.randbytes(rng.randint(0, 12345))
            dest = ''.join(rng.choice(string.ascii_letters) for _ in range(rng.randint(0, 100)))
            dest_data = dest.encode('UTF-8')
            session = uuid.UUID(bytes=rng.randbytes(16)) if rng.random() > 0.5 else None
            stream = DummyStream(BytesIO(struct.pack('>QQ?', len(exp_data) + 8 + len(dest_data) + 1 + (16 if session else 0), len(dest_data), session is not None) + dest_data + (session.bytes if session else b'') + exp_data))
            async with ReaderStreamMessageWrapper(stream) as reader:
                data_read = await reader.readexactly(min(11, len(exp_data)))
            data_all = reader.message_body()

            self.assertEqual(exp_data, data_all)
            self.assertEqual(exp_data[:11], data_read)
            self.assertEqual(len(exp_data), reader.message_size())
            self.assertEqual(dest, reader.message_destination())
            self.assertEqual(session, reader.message_session())
