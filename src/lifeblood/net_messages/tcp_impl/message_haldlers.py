import json
from .clients import JsonMessageClient, CommandJsonMessageClient, JsonMessageWrapper
from ..client import MessageClient
from ..message_handler import MessageHandlerBase
from ..messages import Message
from ..logging import get_logger

from typing import Awaitable, Callable, Dict


class JsonMessageHandlerBase(MessageHandlerBase):
    def __init__(self):
        super().__init__()
        self._logger = get_logger(self.__class__.__name__)

    async def process_message(self, message: Message, client: MessageClient) -> bool:
        if not isinstance(client, JsonMessageClient):
            # we expect client to be json client
            return False
        jmessage = JsonMessageWrapper(message)
        try:
            await jmessage.message_body_as_json()
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._logger.exception('failed to parse json')
            return False
        return await self.process_json_message(jmessage, client) or False

    async def process_json_message(self, message: JsonMessageWrapper, client: JsonMessageClient) -> bool:
        raise NotImplementedError()


class CommandMessageHandlerBase(JsonMessageHandlerBase):
    def command_mapping(self) -> Dict[str, Callable[[dict, CommandJsonMessageClient, Message], Awaitable[None]]]:
        raise NotImplementedError()

    async def process_json_message(self, message: JsonMessageWrapper, client: CommandJsonMessageClient) -> bool:
        if not isinstance(client, CommandJsonMessageClient):
            return False

        body = await message.message_body_as_json()
        if 'command' in body:
            command_json = body['command']
            command = command_json['name']
            args = command_json.get('arguments', {})

            commands_map = self.command_mapping()
            if command not in commands_map:
                return False

            await commands_map[command](args, client, message)
            return True
        else:
            self._logger.error(f'message is not a command, "{message}"')
            return False
