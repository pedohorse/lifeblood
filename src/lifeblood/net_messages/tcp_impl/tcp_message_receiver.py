import asyncio
from .message_protocol import MessageProtocol, IProtocolInstanceCounter
from ..interfaces import MessageReceiver, MessageStreamFactory
from ..messages import Message
from ..address import DirectAddress

from typing import Awaitable, Callable, Tuple


class TcpMessageReceiver(MessageReceiver, IProtocolInstanceCounter):
    def __init__(self, address: Tuple[str, int],
                 message_received_callback: Callable[[Message], Awaitable[bool]],
                 *, socket_backlog=4096):
        super().__init__(DirectAddress(address))
        self.__address = address
        self.__socket_backlog = socket_backlog
        self.__message_received_callback = message_received_callback
        self.__server = None
        self.__active_protocol_instances_count = 0
        self.__no_more_instances = asyncio.Event()
        self.__no_more_instances.set()

    def _protocol_inc_count(self):
        if self.__active_protocol_instances_count == 0:
            self.__no_more_instances.clear()
        self.__active_protocol_instances_count += 1

    def _protocol_dec_count(self):
        if self.__active_protocol_instances_count == 0:
            raise RuntimeError('inconsistent protocol instance count!')
        self.__active_protocol_instances_count -= 1
        if self.__active_protocol_instances_count == 0:
            self.__no_more_instances.set()

    def _allowed_new_instances(self) -> bool:
        return self.__server.is_serving()

    async def start(self):
        self.__server = await asyncio.get_event_loop().create_server(lambda: MessageProtocol(self,
                                                                                             self.__address,
                                                                                             self.__message_received_callback),
                                                                     self.__address[0],
                                                                     self.__address[1],
                                                                     backlog=self.__socket_backlog)

    def stop(self):
        self.__server.close()

    async def wait_till_stopped(self):
        await self.__server.wait_closed()
        await self.__no_more_instances.wait()
