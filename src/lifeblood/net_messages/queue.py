import asyncio
from dataclasses import dataclass
from uuid import UUID
from .messages import Message

from typing import Dict


@dataclass
class RefCountQueue:
    queue: asyncio.Queue
    ref_count: int = 0

    def __enter__(self):
        self.ref_count += 1
        return self.queue

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.ref_count -= 1


class MessageQueue:
    def __init__(self):
        self.__session_map: Dict[UUID, RefCountQueue] = {}

    async def put_message(self, message: Message):
        session = message.message_session()
        if session not in self.__session_map:
            self.__session_map[session] = RefCountQueue(asyncio.Queue())
        with self.__session_map[session] as queue:
            await queue.put(message)

    async def get_message(self, session):
        if session not in self.__session_map:
            self.__session_map[session] = RefCountQueue(asyncio.Queue())

        with self.__session_map[session] as queue:
            message = await queue.get()
            if self.__session_map[session].ref_count == 1:  # 1 means it's just us
                self.__session_map.pop(session)
            return message
