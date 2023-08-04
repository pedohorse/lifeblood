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
    def copy(cls, another_message: "Message"):
        return cls(another_message.__data, another_message.__message_type, another_message.__source, another_message.__destination, another_message.__session)

    def copy_from(self, another_message: "Message"):
        self.__data = another_message.__data
        self.__destination = another_message.__destination
        self.__source = another_message.__source
        self.__session = another_message.__session
        self.__message_type = another_message.__message_type

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
        elif self.__message_type == MessageType.DEFAULT_MESSAGE:
            return_type = self.__message_type
        else:
            raise RuntimeError(f'unknown message type {self.__message_type}')
        return Message(data, return_type, self.__destination, self.__source, self.__session)

    def message_type(self) -> MessageType:
        return self.__message_type

