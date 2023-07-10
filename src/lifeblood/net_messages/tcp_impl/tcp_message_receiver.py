import asyncio
from .message_protocol import MessageProtocol
from ..interfaces import MessageReceiver, MessageStreamFactory
from ..messages import Message
from ..address import DirectAddress

from typing import Awaitable, Callable, Tuple


class TcpMessageReceiver(MessageReceiver):
    def __init__(self, address: Tuple[str, int],
                 message_received_callback: Callable[[Message], Awaitable[None]],
                 *, socket_backlog=4096):
        super().__init__(DirectAddress(address))
        self.__address = address
        self.__socket_backlog = socket_backlog
        self.__message_received_callback = message_received_callback
        self.__server = None

    async def start(self):
        self.__server = await asyncio.get_event_loop().create_server(lambda: MessageProtocol(self.__address,
                                                                                             self.__message_received_callback),
                                                                     self.__address[0],
                                                                     self.__address[1],
                                                                     backlog=self.__socket_backlog)

    def stop(self):
        self.__server.close()

    async def wait_till_stopped(self):
        await self.__server.wait_closed()
