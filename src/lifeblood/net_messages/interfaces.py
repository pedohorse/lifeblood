from .messages import Message
from .stream_wrappers import MessageStream
from .address import DirectAddress, AddressChain

from typing import Callable, Awaitable


class MessageReceiverFactory:
    async def create_receiver(self, address: str, message_callback: Callable[[Message], Awaitable[None]]) -> "MessageReceiver":
        raise NotImplementedError()


class MessageReceiver:
    def stop(self):
        """
        stop running receiver async task
        """
        raise NotImplementedError()

    async def wait_till_stopped(self):
        raise NotImplementedError()


class MessageStreamFactory:
    async def open_message_connection(self, destination: DirectAddress, source: AddressChain) -> MessageStream:
        raise NotImplementedError()
