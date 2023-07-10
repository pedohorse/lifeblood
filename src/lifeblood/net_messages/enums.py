from enum import Enum


class MessageType(Enum):
    @classmethod
    def message_type_size(cls) -> int:
        return 2

    STANDALONE_MESSAGE = b'\0\0'
