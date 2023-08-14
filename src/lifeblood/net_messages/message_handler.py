from .messages import Message
from .client import MessageClient


class MessageHandlerBase:
    async def process_message(self, message: Message, client: MessageClient) -> bool:
        """
        should return True if message was processed.
        if message is not expected by this handler - the function should return False
        """
        raise NotImplementedError()
