from ..message_processor import MessageProcessorBase
from ..messages import Message
from ..client import MessageClient
from ..address import DirectAddress
from .tcp_message_receiver_factory import TcpMessageReceiverFactory
from .tcp_message_stream_factory import TcpMessageStreamFactory

from typing import Tuple


class MessageProcessor(MessageProcessorBase):
    def __init__(self, listening_address: Tuple[str, int], *, backlog=4096):
        super().__init__(DirectAddress(':'.join(str(x) for x in listening_address)),
                         message_receiver_factory=TcpMessageReceiverFactory(backlog=backlog or 4096),
                         message_stream_factory=TcpMessageStreamFactory())


class MessageProxyProcessor(MessageProcessor):
    async def process_message(self, message: Message, client: MessageClient):
        self._logger.warning('received a message addressed to me, though i\'m just a proxy. ignoring')