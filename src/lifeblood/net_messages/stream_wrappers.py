import asyncio
import uuid
import struct
from io import BytesIO
from .messages import MessageInterface, Message
from .enums import MessageType

from typing import Optional, Union, Tuple


class WriterStreamRawMessageWrapper(MessageInterface):
    """
    wraps writes into "messages" that have predictable borders, that can be redirected easily
    """
    def __init__(self, writer: asyncio.StreamWriter, *, message_type: Optional[MessageType] = None, source: str, destination: str, session: Optional[uuid.UUID] = None):
        """
        Note: source and destination must be provided:
            while destination is basically where reader/writer are connected,
            source is where reply should come, so it's NOT current socket's address

        """
        self.__writer = writer
        self.__buffer: Optional[BytesIO] = None
        self.__size = 0
        self.__source: str = source
        self.__destination: str = destination
        self.__session = session
        self.__message_type: MessageType = message_type or MessageType.STANDALONE_MESSAGE
        self.__initialized = False

    def initialized(self):
        return self.__initialized

    async def __aenter__(self) -> "WriterStreamRawMessageWrapper":
        if self.__initialized:
            raise RuntimeError('message was already initialized')
        self.__initialized = True
        self.__buffer = BytesIO()
        self.__size = 0
        return self

    def write(self, data: bytes):
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        self.__size += len(data)
        self.__buffer.write(data)

    def write_string(self, s: str):
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        b = s.encode('UTF-8')
        self.write(struct.pack('>Q', len(b)))
        self.write(b)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        Message._write_header(self.__writer, self.__size, self.__message_type, self.__source, self.__destination, self.__session)
        self.__writer.write(self.__buffer.getbuffer())
        await self.__writer.drain()

    def message_size(self) -> int:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__size

    def message_body(self) -> Union[bytes, memoryview]:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__buffer.getbuffer()

    def message_destination(self) -> str:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__destination

    def message_source(self) -> str:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__source

    def message_session(self) -> Optional[uuid.UUID]:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__session

    def message_type(self) -> MessageType:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__message_type

    def to_message(self) -> Message:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return Message(self.message_body(), self.message_type(), self.message_source(), self.message_destination(), self.message_session())


class ReaderStreamRawMessageWrapper(MessageInterface):
    def __init__(self, reader: asyncio.StreamReader, session: Optional[uuid.UUID] = None):
        self.__reader = reader
        self.__message_body_size: int = 0
        self.__already_read = 0
        self.__source: Optional[str] = None
        self.__destination: Optional[str] = None
        self.__message_type: Optional[MessageType] = None
        self.__initialized = False
        self.__buffer = BytesIO()
        self.__session = session

    def initialized(self):
        return self.__initialized

    def message_size(self):
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__message_body_size

    def message_body(self) -> Union[bytes, memoryview]:
        """
        message body so far. this ONLY guaranteed returns the whole message body after with block end
        """
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__buffer.getbuffer()

    def message_destination(self) -> str:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__destination

    def message_source(self) -> str:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__source

    def message_session(self) -> Optional[uuid.UUID]:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__session

    def message_type(self) -> MessageType:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__message_type

    async def __aenter__(self) -> "ReaderStreamRawMessageWrapper":
        if self.__initialized:
            raise RuntimeError('message was already initialized')
        self.__initialized = True
        self.__message_body_size, self.__message_type, self.__source, self.__destination, session = await Message._read_header(self.__reader)
        if self.__session is not None:  # if we expect specific session
            if session != self.__session:
                raise RuntimeError('received message has wrong session')
        else:
            self.__session = session
        self.__already_read = 0
        return self

    async def readexactly(self, size: int):
        if size <= 0:
            return
        self.__already_read += size
        data = await self.__reader.readexactly(size)
        self.__buffer.write(data)
        return data

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.readexactly(self.__message_body_size - self.__already_read)

    def to_message(self) -> Message:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return Message(self.message_body(), self.message_type(), self.message_source(), self.message_destination(), self.message_session())

#
# class MessageSession:
#     def __init__(self, writer: asyncio.StreamWriter, *, destination: Optional[str] = None, session: uuid.UUID):
#         self.__initialized = False
#         self.__finalized = False
#         self.__writer = writer
#         self.__destination = destination
#         self.__session = session
#
#     async def initialize(self):
#         if self.__initialized:
#             raise RuntimeError('message was already initialized')
#         self.__initialized = True
#         async with WriterStreamRawMessageWrapper(self.__writer,
#                                              message_type=MessageType.SESSION_START,
#                                              destination=self.__destination,
#                                              session=self.__session):
#             pass
#
#     async def finalize(self):
#         if not self.__initialized:
#             raise RuntimeError('message was not initialized')
#         if self.__finalized:
#             raise RuntimeError('message was already finalized')
#         self.__finalized = True
#         async with WriterStreamRawMessageWrapper(self.__writer,
#                                                  message_type=MessageType.SESSION_END,
#                                                  destination=self.__destination,
#                                                  session=self.__session):
#             pass
#
#     async def __aenter__(self) -> "MessageSession":
#         await self.initialize()
#         return self
#
#     async def __aexit__(self, exc_type, exc_val, exc_tb):
#         await self.finalize()


class MessageStream:
    def __init__(self, reader_stream: asyncio.StreamReader, writer_stream: asyncio.StreamWriter, reply_listening_address: Tuple[str, int]):
        self.__reader = reader_stream
        self.__writer = writer_stream
        self.__msg_source = ':'.join(str(x) for x in reply_listening_address)

    def start_new_message(self, destination: str, session: uuid.UUID) -> WriterStreamRawMessageWrapper:
        return WriterStreamRawMessageWrapper(self.__writer, source=self.__msg_source, destination=destination, session=session)

    def start_receiving_message(self):
        return ReaderStreamRawMessageWrapper(self.__reader)

    async def forward_message(self, message):
        """
        assume message prepared, just forwarding it
        """
        await message.serialize_to_stream_writer(self.__writer)
        await self.__writer.drain()

    async def send_data_message(self, data: bytes, destination: str, *, session: uuid.UUID) -> Message:
        async with self.start_new_message(destination, session=session) as stream:
            stream.write(data)
        return stream.to_message()

    async def receive_data_message(self) -> Message:
        async with self.start_receiving_message() as stream:
            pass  # will receive all

        return stream.to_message()

    def close(self):
        self.__writer.close()

    async def wait_closed(self):
        await self.__writer.wait_closed()
#
