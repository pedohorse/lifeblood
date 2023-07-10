import asyncio
import struct
import uuid
from .enums import MessageType
from .address import AddressChain

from typing import Optional, Tuple, Union


class MessageInterface:
    def message_size(self) -> int:
        raise NotImplementedError()

    def message_body(self) -> Union[bytes, memoryview]:
        raise NotImplementedError()

    def message_destination(self) -> AddressChain:
        raise NotImplementedError()

    def message_source(self) -> AddressChain:
        raise NotImplementedError()

    def message_session(self) -> Optional[uuid.UUID]:
        raise NotImplementedError()

    def message_type(self) -> MessageType:
        raise NotImplementedError()


class Message(MessageInterface):
    def __init__(self, data: bytes, message_type: MessageType, source: AddressChain, destination: AddressChain, session: Optional[uuid.UUID]):
        self.__data = data
        self.__destination = destination
        self.__source = source
        self.__session = session
        self.__message_type = message_type

    @classmethod
    def _write_header(cls, stream: asyncio.StreamWriter, data_size: int, message_type: MessageType, source: AddressChain, destination: AddressChain, session: Optional[uuid.UUID]):
        src_data = source.encode('UTF-8')
        src_data_size = len(src_data)
        dest_data = destination.encode('UTF-8')
        dest_data_size = len(dest_data)
        has_session = session is not None
        stream.write(struct.pack('>QQQ?', data_size + 16 + src_data_size + dest_data_size + 1 + message_type.message_type_size() + (16 if has_session else 0),
                                 src_data_size,
                                 dest_data_size,
                                 session is not None))
        stream.write(message_type.value)
        stream.write(src_data)
        stream.write(dest_data)
        if has_session:
            stream.write(session.bytes)

    @classmethod
    async def _read_header(cls, stream: asyncio.StreamReader) -> Tuple[int, MessageType, AddressChain, AddressChain, Optional[uuid.UUID]]:
        """
        returns: (data_size, destination, session)
        """
        message_size, src_data_size, dest_data_size, has_session = struct.unpack('>QQQ?', await stream.readexactly(25))
        message_type = MessageType(await stream.readexactly(MessageType.message_type_size()))
        source = (await stream.readexactly(src_data_size)).decode('UTF-8')
        destination = (await stream.readexactly(dest_data_size)).decode('UTF-8')
        session = uuid.UUID(bytes=await stream.readexactly(16)) if has_session else None
        return message_size - 8-src_data_size - 8-dest_data_size - 1 - MessageType.message_type_size() - (16 if has_session else 0), \
               message_type, \
               AddressChain(source), \
               AddressChain(destination), \
               session

    @classmethod
    async def from_stream_reader(cls, stream: asyncio.StreamReader):
        data_size, message_type, source, destination, session = await cls._read_header(stream)
        data = await stream.readexactly(data_size)
        return Message(data, message_type, source, destination, session)

    async def serialize_to_stream_writer(self, stream: asyncio.StreamWriter):
        self._write_header(stream, len(self.__data), self.__message_type, self.__source, self.__destination, self.__session)
        stream.write(self.__data)
        await stream.drain()

    def message_size(self) -> int:
        return len(self.__data)

    def message_body(self) -> Union[bytes, memoryview]:
        return self.__data

    def message_source(self) -> AddressChain:
        return self.__source

    def message_destination(self) -> AddressChain:
        return self.__destination

    def message_session(self) -> Optional[uuid.UUID]:
        return self.__session

    def set_message_body(self, body: bytes):
        self.__data = body

    def set_message_destination(self, destination: AddressChain):
        self.__destination = destination

    def set_message_source(self, source: AddressChain):
        self.__source = source

    def create_reply_message(self, data: bytes = b''):
        if self.__message_type in (MessageType.SESSION_START, MessageType.SESSION_MESSAGE):
            return_type = MessageType.SESSION_MESSAGE
        elif self.__message_type == MessageType.SESSION_END:
            raise RuntimeError('cannot reply to session end message')
        elif self.__message_type == MessageType.STANDALONE_MESSAGE:
            return_type = self.__message_type
        else:
            raise RuntimeError(f'unknown message type {self.__message_type}')
        return Message(data, return_type, self.__destination, self.__source, self.__session)

    def message_type(self) -> MessageType:
        return self.__message_type

