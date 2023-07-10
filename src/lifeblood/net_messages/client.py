import uuid
from .messages import Message
from .queue import MessageQueue
from .connections import open_message_connection
from .address import split_address

from typing import Optional, Tuple, Union


class MessageClient:
    def __init__(self, queue: MessageQueue, session: uuid.UUID, *, source: Union[str, Tuple[str, int]], destination: str):
        self.__message_queue = queue
        self.__last_sent_message: Optional[Message] = None
        self.__session: uuid.UUID = session
        if isinstance(source, str):
            self.__source = split_address(source)[0]
            self.__source_str = source
        else:
            self.__source = source
            self.__source_str = ':'.join(str(x) for x in source)
        self.__destination = split_address(destination)
        self.__destination_str = destination

    def session(self) -> uuid.UUID:
        return self.__session

    async def send_message(self, data: bytes) -> Message:
        stream = await open_message_connection(self.__destination, self.__source)
        do_close = True
        try:
            message = await stream.send_data_message(data, self.__destination_str, session=self.__session)
            self.__last_sent_message = message
            return message
        except ConnectionResetError:
            do_close = False
            raise
        finally:
            if do_close:
                stream.close()
                await stream.wait_closed()

    async def receive_message(self) -> Message:
        message = await self.__message_queue.get_message(self.__session)
        return message
