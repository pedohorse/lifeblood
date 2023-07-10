from enum import Enum


class MessageType(Enum):
    @classmethod
    def message_type_size(cls) -> int:
        return 2

    DEFAULT_MESSAGE = b'\0\0'
    SYSTEM_PING = b'\0\1'
    DELIVERY_ERROR = b'\1\0'
