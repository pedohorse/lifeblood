import asyncio
import json
import uuid
from lifeblood.logging import get_logger
from ..message_processor import MessageProcessorBase
from ..messages import Message
from ..enums import MessageType
from ..client import MessageClient, MessageClientFactory
from ..address import AddressChain, DirectAddress
from ..queue import MessageQueue
from ..interfaces import MessageStreamFactory
from .tcp_message_processor import TcpMessageProcessor
from .tcp_message_receiver_factory import TcpMessageReceiverFactory
from .tcp_message_stream_factory import TcpMessageStreamFactory, TcpMessageStreamPooledFactory

from typing import Awaitable, Callable, Dict, Optional, Tuple


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


class TcpJsonMessageProcessor(TcpMessageProcessor):
    def __init__(self, listening_address: Tuple[str, int], *, backlog=4096, connection_pool_cache_time=300, message_client_factory: Optional[JsonMessageClientFactory] = None):
        super().__init__(listening_address,
                         backlog=backlog,
                         connection_pool_cache_time=connection_pool_cache_time,
                         message_client_factory=message_client_factory or JsonMessageClientFactory())

    async def process_message(self, message: Message, client: MessageClient):
        assert isinstance(client, JsonMessageClient)
        jmessage = JsonMessageWrapper(message)
        try:
            await jmessage.message_body_as_json()
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._logger.exception('failed to parse json')
            return await self.process_unknown_message(message, client)
        return await self.process_json_message(jmessage, client)

    async def process_json_message(self, message: JsonMessageWrapper, client: JsonMessageClient):
        raise NotImplementedError()

    async def process_unknown_message(self, message: Message, client: MessageClient):
        self._logger.warning(f'unexpected non-json message, ignoring {bytes(message.message_body()[:128])}')


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


class TcpCommandMessageProcessor(TcpJsonMessageProcessor):
    def __init__(self, listening_address: Tuple[str, int], *, backlog=4096, connection_pool_cache_time=300):
        super().__init__(listening_address,
                         backlog=backlog,
                         connection_pool_cache_time=connection_pool_cache_time,
                         message_client_factory=CommandJsonMessageClientFactory())

    def command_mapping(self) -> Dict[str, Callable[[dict, CommandJsonMessageClient, Message], Awaitable[None]]]:
        raise NotImplementedError()

    async def process_json_message(self, message: JsonMessageWrapper, client: CommandJsonMessageClient):
        if not await self.should_process(message):
            self._logger.debug(f'should not process, skipping message {message}')
            return

        body = await message.message_body_as_json()
        if 'command' in body:
            command_json = body['command']
            command = command_json['name']
            args = command_json.get('arguments', {})

            commands_map = self.command_mapping()
            if command not in commands_map:
                await self.process_unknown_command(command, args, client, message)
                return

            await commands_map[command](args, client, message)
        else:
            self._logger.error(f'unexpected message to the worker, "{message.message_body()}"')

    async def process_unknown_command(self, command: str, args: dict, client: CommandJsonMessageClient, original_message: JsonMessageWrapper):
        self._logger.error(f'unknown command {command}')
