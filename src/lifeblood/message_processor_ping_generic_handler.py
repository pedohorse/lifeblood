import datetime
from contextlib import contextmanager
from .net_messages.address import AddressChain
from .net_messages.messages import Message
from .net_messages.impl.tcp_simple_command_message_processor import TcpCommandMessageProcessor
from .net_messages.impl.message_haldlers import CommandMessageHandlerBase
from .net_messages.impl.clients import CommandJsonMessageClient

from typing import Optional


class PingGenericHandler(CommandMessageHandlerBase):
    def command_mapping(self):
        return {
            'ping_generic': self._ping_handler
        }

    async def _ping_handler(self, args: dict, client: CommandJsonMessageClient, original_message: Message):
        data = args.get('data', {})
        reply = {
            'timestamp_utc': datetime.datetime.utcnow().timestamp(),
            'timestamp': datetime.datetime.now().timestamp(),
        }
        if data is not None:
            reply['data'] = await self.produce_reply(data)

        await client.send_message_as_json(reply)

    async def produce_reply(self, data: dict) -> dict:
        """
        override this for custom logic for ping reply generation
        """
        return {}


class PingGenericClient:
    def __init__(self, client: CommandJsonMessageClient):
        super().__init__()
        self.__client = client

    @classmethod
    @contextmanager
    def get_worker_control_client(cls, worker_address: AddressChain, processor: TcpCommandMessageProcessor) -> "PingGenericClient":
        with processor.message_client(worker_address) as message_client:
            yield PingGenericClient(message_client)

    async def ping(self, data: Optional[dict] = None) -> dict:
        req = {
            'timestamp': datetime.datetime.utcnow(),
        }
        if data is not None:
            req['data'] = data

        await self.__client.send_command(
            'ping_generic',
            data,
        )

        reply = await self.__client.receive_message()
        return await reply.message_body_as_json()
