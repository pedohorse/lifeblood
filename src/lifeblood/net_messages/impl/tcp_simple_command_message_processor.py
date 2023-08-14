from .clients import JsonMessageClientFactory, CommandJsonMessageClientFactory
from ..message_handler import MessageHandlerBase
from .tcp_message_processor import TcpMessageProcessor

from typing import Optional, Sequence, Tuple


class TcpJsonMessageProcessor(TcpMessageProcessor):
    def __init__(self, listening_address: Tuple[str, int], *,
                 backlog=4096,
                 connection_pool_cache_time=300,
                 message_client_factory: Optional[JsonMessageClientFactory] = None,
                 message_handlers: Sequence[MessageHandlerBase] = ()):
        super().__init__(listening_address,
                         backlog=backlog,
                         connection_pool_cache_time=connection_pool_cache_time,
                         message_handlers=message_handlers,
                         message_client_factory=message_client_factory or JsonMessageClientFactory())


class TcpCommandMessageProcessor(TcpJsonMessageProcessor):
    def __init__(self, listening_address: Tuple[str, int], *,
                 backlog=4096,
                 connection_pool_cache_time=300,
                 message_handlers: Sequence[MessageHandlerBase] = ()):
        super().__init__(listening_address,
                         backlog=backlog,
                         connection_pool_cache_time=connection_pool_cache_time,
                         message_handlers=message_handlers,
                         message_client_factory=CommandJsonMessageClientFactory())
