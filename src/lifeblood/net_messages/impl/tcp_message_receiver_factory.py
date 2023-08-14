from ..interfaces import MessageReceiverFactory
from ..messages import Message
from ..address import DirectAddress
from ..exceptions import AddressTypeNotSupportedError
from .tcp_message_receiver import TcpMessageReceiver

from typing import Callable, Awaitable


class TcpMessageReceiverFactory(MessageReceiverFactory):
    def __init__(self, backlog=4096):
        self.__backlog = backlog

    async def create_receiver(self, address: DirectAddress, message_callback: Callable[[Message], Awaitable[bool]]) -> TcpMessageReceiver:
        if address.count(':') != 1:
            raise AddressTypeNotSupportedError(f'address "{address}" is not of "<host>:<port>" format')
        host, sport = address.split(':')
        receiver = TcpMessageReceiver((host, int(sport)), message_callback, socket_backlog=self.__backlog)
        await receiver.start()
        return receiver
