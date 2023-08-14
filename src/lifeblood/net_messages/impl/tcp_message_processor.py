import asyncio
from ..message_processor import MessageProcessorBase
from ..message_handler import MessageHandlerBase
from ..messages import Message
from ..client import MessageClient, MessageClientFactory
from ..address import DirectAddress
from .tcp_message_receiver_factory import TcpMessageReceiverFactory
from .tcp_message_stream_factory import TcpMessageStreamFactory, TcpMessageStreamPooledFactory

from typing import Optional, Sequence, Tuple


class TcpMessageProcessor(MessageProcessorBase):
    def __init__(self, listening_address: Tuple[str, int], *,
                 backlog=4096,
                 connection_pool_cache_time=300,
                 stream_timeout: float = 90,
                 default_client_retry_attempts: Optional[int] = None,
                 message_client_factory: Optional[MessageClientFactory] = None,
                 message_handlers: Sequence[MessageHandlerBase] = ()):
        self.__pooled_factory = None
        if connection_pool_cache_time <= 0:
            stream_factory = TcpMessageStreamFactory(timeout=stream_timeout)
        else:
            stream_factory = TcpMessageStreamPooledFactory(connection_pool_cache_time, timeout=stream_timeout)
            self.__pooled_factory = stream_factory
        super().__init__(DirectAddress(':'.join(str(x) for x in listening_address)),
                         message_receiver_factory=TcpMessageReceiverFactory(backlog=backlog or 4096),
                         message_stream_factory=stream_factory,
                         default_client_retry_attempts=default_client_retry_attempts,
                         message_client_factory=message_client_factory,
                         message_handlers=message_handlers)

    async def _post_receiver_stop_waited(self):
        await super()._post_receiver_stop_waited()
        if self.__pooled_factory is not None:
            self.__pooled_factory.close_pool()

    async def wait_till_stops(self):
        await super().wait_till_stops()
        if self.__pooled_factory is not None:
            await self.__pooled_factory.wait_pool_closed()


class TcpMessageProxyProcessor(TcpMessageProcessor):
    async def process_message(self, message: Message, client: MessageClient):
        self._logger.warning('received a message addressed to me, though i\'m just a proxy. ignoring')
