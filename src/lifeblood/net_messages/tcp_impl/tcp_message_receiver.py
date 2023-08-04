import asyncio
from .message_protocol import MessageProtocol, IProtocolInstanceCounter
from ..interfaces import MessageReceiver, MessageStreamFactory
from ..messages import Message
from ..address import DirectAddress
from ..logging import get_logger

from typing import Awaitable, Callable, List, Tuple


class TcpMessageReceiver(MessageReceiver, IProtocolInstanceCounter):
    def __init__(self, address: Tuple[str, int],
                 message_received_callback: Callable[[Message], Awaitable[bool]],
                 *, socket_backlog=4096):
        super().__init__(DirectAddress.from_host_port(*address))
        self.__address = address
        self.__socket_backlog = socket_backlog
        self.__message_received_callback = message_received_callback
        self.__server = None
        self.__active_protocol_instances_count = 0
        self.__protocol_instance_stash: List[MessageProtocol] = []
        self.__no_more_instances = asyncio.Event()
        self.__no_more_instances.set()
        self.__logger = get_logger(f'{self.__class__.__name__}')

    def _protocol_inc_count(self, instance):
        if self.__active_protocol_instances_count == 0:
            self.__no_more_instances.clear()
        self.__active_protocol_instances_count += 1
        self.__protocol_instance_stash.append(instance)

    def _protocol_dec_count(self, instance):
        if self.__active_protocol_instances_count == 0:
            raise RuntimeError('inconsistent protocol instance count!')
        self.__active_protocol_instances_count -= 1
        if self.__active_protocol_instances_count == 0:
            self.__no_more_instances.set()
        self.__protocol_instance_stash.remove(instance)

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
        self.__logger.debug('stopping tcp message receiver')
        self.__server.close()
        for instance in self.__protocol_instance_stash:
            instance.stop()

    async def wait_till_stopped(self):
        await self.__server.wait_closed()
        await self.__no_more_instances.wait()
