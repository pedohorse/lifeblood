import asyncio
import struct
import uuid
from io import BytesIO

from typing import Optional, Tuple, Union


class MessageInterface:
    def message_size(self) -> int:
        raise NotImplementedError()

    def message_body(self) -> Union[bytes, memoryview]:
        raise NotImplementedError()

    def message_destination(self) -> str:
        raise NotImplementedError()

    def message_source(self) -> str:
        raise NotImplementedError()

    def message_session(self) -> Optional[uuid.UUID]:
        raise NotImplementedError()


class Message(MessageInterface):
    def __init__(self, data: bytes, source: str, destination: str, session: Optional[uuid.UUID]):
        self.__data = data
        self.__destination = destination
        self.__source = source
        self.__session = session

    @classmethod
    def _write_header(cls, stream: asyncio.StreamWriter, data_size: int, source: str, destination: str, session: Optional[uuid.UUID]):
        src_data = source.encode('UTF-8')
        src_data_size = len(src_data)
        dest_data = destination.encode('UTF-8')
        dest_data_size = len(dest_data)
        has_session = session is not None
        stream.write(struct.pack('>QQQ?', data_size + 16 + src_data_size + dest_data_size + 1 + (16 if has_session else 0),
                                 src_data_size,
                                 dest_data_size,
                                 session is not None))
        stream.write(src_data)
        stream.write(dest_data)
        if has_session:
            stream.write(session.bytes)

    @classmethod
    async def _read_header(cls, stream: asyncio.StreamReader) -> Tuple[int, str, str, Optional[uuid.UUID]]:
        """
        returns: (data_size, destination, session)
        """
        message_size, src_data_size, dest_data_size, has_session = struct.unpack('>QQQ?', await stream.readexactly(25))
        source = (await stream.readexactly(src_data_size)).decode('UTF-8')
        destination = (await stream.readexactly(dest_data_size)).decode('UTF-8')
        session = uuid.UUID(bytes=await stream.readexactly(16)) if has_session else None
        return message_size - 8-src_data_size - 8-dest_data_size - 1 - (16 if has_session else 0), \
               source, \
               destination, \
               session

    @classmethod
    async def from_stream_reader(cls, stream: asyncio.StreamReader):
        data_size, source, destination, session = await cls._read_header(stream)
        data = await stream.readexactly(data_size)
        return Message(data, source, destination, session)

    async def serialize_to_stream_writer(self, stream: asyncio.StreamWriter):
        self._write_header(stream, len(self.__data), self.__source, self.__destination, self.__session)
        stream.write(self.__data)
        await stream.drain()

    def message_size(self) -> int:
        return len(self.__data)

    def message_body(self) -> Union[bytes, memoryview]:
        return self.__data

    def message_destination(self) -> str:
        return self.__destination

    def message_session(self) -> Optional[uuid.UUID]:
        return self.__session

    def set_message_body(self, body: bytes):
        self.__data = body

    def create_reply_message(self, data: bytes = b''):
        return Message(data, self.__destination, self.__source, self.__session)


class WriterStreamMessageWrapper(MessageInterface):
    """
    wraps writes into "messages" that have predictable borders, that can be redirected easily
    """
    def __init__(self, writer: asyncio.StreamWriter, *, destination: str, session: Optional[uuid.UUID] = None):
        self.__writer = writer
        self.__buffer: Optional[BytesIO] = None
        self.__size = 0
        self.__source = writer.transport.get_extra_info('sockname')
        self.__destination = destination
        self.__session = session
        self.__initialized = False

    async def __aenter__(self) -> "WriterStreamMessageWrapper":
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
        Message._write_header(self.__writer, self.__size, self.__source, self.__destination, self.__session)
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


class ReaderStreamMessageWrapper(MessageInterface):
    def __init__(self, reader: asyncio.StreamReader):
        self.__reader = reader
        self.__message_body_size: int = 0
        self.__already_read = 0
        self.__source: Optional[str] = None
        self.__destination: Optional[str] = None
        self.__initialized = False
        self.__buffer = BytesIO()
        self.__session = None

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

    async def __aenter__(self) -> "ReaderStreamMessageWrapper":
        if self.__initialized:
            raise RuntimeError('message was already initialized')
        self.__initialized = True
        self.__message_body_size, self.__source, self.__destination, self.__session = await Message._read_header(self.__reader)
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


class MessageStream:
    def __init__(self, reader_stream: asyncio.StreamReader, writer_stream: asyncio.StreamWriter):
        self.__reader = reader_stream
        self.__writer = writer_stream
        self.__current_session: Optional[uuid.UUID] = None

    def new_message(self, destination: str, force_new_session=True) -> WriterStreamMessageWrapper:
        if self.__current_session is None or force_new_session:
            self.__current_session = uuid.uuid4()
        return WriterStreamMessageWrapper(self.__writer, destination=destination, session=self.__current_session)

    def start_receiving_message(self):
        pass
