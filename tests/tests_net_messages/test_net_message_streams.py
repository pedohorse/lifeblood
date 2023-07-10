import asyncio
import struct
import random
import string
import uuid
from lifeblood.net_messages.enums import MessageType
from lifeblood.net_messages.stream_wrappers import ReaderStreamRawMessageWrapper, WriterStreamRawMessageWrapper
from lifeblood.net_messages.messages import Message
from unittest import IsolatedAsyncioTestCase
from io import BufferedReader, BufferedWriter, BytesIO

from typing import Optional


class DummyStream:
    def __init__(self, buffer: Optional[BytesIO] = None):
        super().__init__()
        self.__buffer = buffer or BytesIO()

    @property
    def transport(self):
        class _DummySock:
            def get_extra_info(self, key, default=None) -> Optional[str]:
                if key == 'sockname':
                    return '127.0.0.1:1234'
                print(key)

        return _DummySock()

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


class TestNetMessageStreams(IsolatedAsyncioTestCase):
    async def test_simple_writer(self):
        stream = DummyStream()
        async with WriterStreamRawMessageWrapper(stream, destination='foo;бар', source='bob') as writer:
            writer.write(b'test')

        stream.rewind()
        async with ReaderStreamRawMessageWrapper(stream) as reader:
            pass
        self.assertEqual(b'test', reader.message_body())
        self.assertEqual('bob', reader.message_source())
        self.assertEqual('foo;бар', reader.message_destination())

    async def test_empty_writer(self):
        stream = DummyStream()
        async with WriterStreamRawMessageWrapper(stream, destination='bee;boo', source='bob2') as writer:
            writer.write(b'')

        stream.rewind()
        async with ReaderStreamRawMessageWrapper(stream) as reader:
            pass
        self.assertEqual(b'', reader.message_body())
        self.assertEqual('bob2', reader.message_source())
        self.assertEqual('bee;boo', reader.message_destination())

    async def test_random_writer(self):
        rng = random.Random(666666)
        for _ in range(666):
            data = rng.randbytes(rng.randint(0, 12345))
            stream = DummyStream()
            dest = ''.join(rng.choice(string.ascii_letters) for _ in range(rng.randint(0, 100)))
            src = ''.join(rng.choice(string.ascii_letters) for _ in range(rng.randint(0, 100)))
            session = uuid.UUID(bytes=rng.randbytes(16)) if rng.random() > 0.5 else None
            async with WriterStreamRawMessageWrapper(stream, destination=dest, source=src, session=session) as writer:
                writer.write(data)

            stream.rewind()

            async with ReaderStreamRawMessageWrapper(stream) as reader:
                pass
            msg = reader.to_message()
            self.assertEqual(len(data), msg.message_size())
            self.assertEqual(data, msg.message_body())
            self.assertEqual(session, msg.message_session())
            self.assertEqual(dest, msg.message_destination())
            self.assertEqual(src, msg.message_source())
