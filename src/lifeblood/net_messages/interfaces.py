from .messages import Message
from .message_stream import MessageSendStreamBase, MessageReceiveStreamBase
from .address import DirectAddress, AddressChain

from typing import Callable, Awaitable


class MessageReceiverFactory:
    async def create_receiver(self, address: DirectAddress, message_callback: Callable[[Message], Awaitable[bool]]) -> "MessageReceiver":
        raise NotImplementedError()


class MessageReceiver:
    def __init__(self, this_address: DirectAddress):
        self.__this_address = this_address

    def this_address(self) -> DirectAddress:
        return self.__this_address

    def stop(self):
        """
        stop running receiver async task
        """
        raise NotImplementedError()

    async def wait_till_stopped(self):
        raise NotImplementedError()


class MessageStreamFactory:
    async def open_sending_stream(self, destination: DirectAddress, source: DirectAddress) -> MessageSendStreamBase:
        raise NotImplementedError()
