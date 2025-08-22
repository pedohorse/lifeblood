from .messages import Message
from .client import MessageClient


class MessageHandlerBase:
    async def process_message(self, message: Message, client: MessageClient) -> bool:
        """
        should return True if message was processed.
        if message is not expected by this handler - the function should return False
        """
        raise NotImplementedError()

    async def clear_internal_state(self):
        """
        If a message handler implementation has an internal state -
         this function must clean it.
        This might be called when handler is de-initialized,
        or when some other clear slate reset is required
        """
        return
