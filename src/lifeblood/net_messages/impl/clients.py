import asyncio
import json
import uuid
from ..client import MessageClient, MessageClientFactory
from ..queue import MessageQueue
from ..interfaces import MessageStreamFactory
from ..messages import Message, MessageType
from ..address import AddressChain

from typing import Optional


class JsonMessageWrapper(Message):
    def __init__(self, message: Message):
        super().__init__(b'', MessageType.DEFAULT_MESSAGE, AddressChain(''), AddressChain(''), None)
        self.copy_from(message)
        self.__wrapped_message = message
        self.__body_as_json = None

    def __getattr__(self, item):
        return getattr(self.__wrapped_message, item)

    async def message_body_as_json(self) -> dict:
        if self.__body_as_json is None:
            self.__body_as_json = await asyncio.get_event_loop().run_in_executor(None,
                                                                                 lambda s: json.loads(bytes(s).decode('utf-8')),
                                                                                 self.message_body())
        return self.__body_as_json

    async def set_message_body_as_json(self, body: dict):
        self.__body_as_json = body
        asyncio.get_event_loop().run_in_executor(None,
                                                 lambda d: self.set_message_body(json.dumps(d).encode('utf-8')),
                                                 body)


class JsonMessageClient(MessageClient):
    async def send_message_as_json(self, data: dict) -> JsonMessageWrapper:
        return JsonMessageWrapper(await self.send_message(json.dumps(data).encode('utf-8')))

    async def receive_message(self, timeout: Optional[float] = 90) -> JsonMessageWrapper:
        message = await super().receive_message(timeout)
        return JsonMessageWrapper(message)


class JsonMessageClientFactory(MessageClientFactory):
    def create_message_client(self, queue: MessageQueue, session: uuid.UUID, *,
                              source_address_chain: AddressChain,
                              destination_address_chain: AddressChain,
                              message_stream_factory: MessageStreamFactory,
                              send_retry_attempts: int = 2) -> MessageClient:
        return JsonMessageClient(queue, session,
                                 source_address_chain=source_address_chain,
                                 destination_address_chain=destination_address_chain,
                                 message_stream_factory=message_stream_factory,
                                 send_retry_attempts=send_retry_attempts
                                 )


class CommandJsonMessageClient(JsonMessageClient):
    async def send_command(self, command: str, arguments: dict):
        return await self.send_message_as_json({
            'command': {
                'name': command,
                'arguments': arguments
            }
        })


class CommandJsonMessageClientFactory(JsonMessageClientFactory):
    def create_message_client(self, queue: MessageQueue, session: uuid.UUID, *,
                              source_address_chain: AddressChain,
                              destination_address_chain: AddressChain,
                              message_stream_factory: MessageStreamFactory,
                              send_retry_attempts: int = 2) -> MessageClient:
        return CommandJsonMessageClient(queue, session,
                                        source_address_chain=source_address_chain,
                                        destination_address_chain=destination_address_chain,
                                        message_stream_factory=message_stream_factory,
                                        send_retry_attempts=send_retry_attempts
                                        )

