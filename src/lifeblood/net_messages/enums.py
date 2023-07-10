from enum import Enum


class MessageType(Enum):
    @classmethod
    def message_type_size(cls) -> int:
        return 2

    STANDALONE_MESSAGE = b'\0\0'
    SESSION_START = b'\0\1'
    SESSION_END = b'\0\3'
    SESSION_MESSAGE = b'\0\2'
    SESSION_BROKEN = b'\0\4'
