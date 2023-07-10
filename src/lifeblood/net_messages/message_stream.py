import uuid
from .exceptions import MessageSendingError, MessageReceivingError, MessageRoutingError
from .messages import Message
from .address import AddressChain, DirectAddress
from .enums import MessageType

from typing import Optional


class MessageSendStreamBase:
    """
    Represents a direct channel between source and destination
    """
    def __init__(self, reply_address: DirectAddress, destination_address: DirectAddress):
        self.__source = reply_address
        self.__destination = destination_address

    def reply_address(self) -> DirectAddress:
        return self.__source

    def destination_address(self) -> DirectAddress:
        return self.__destination

    async def _send_message_implementation(self, message: Message):
        """
        override this with actual message sending implementation
        """
        raise NotImplementedError()

    async def send_raw_message(self, message: Message):
        """
        send message as is

        :raises MessageSendingError: wrapping actual error occured
        """
        if message.message_destination().split_address()[0] != self.destination_address():
            raise MessageRoutingError('message destination address must contain stream\'s destination')
        if message.message_source().split_address()[0] != self.reply_address():
            raise MessageRoutingError('message source address must contain stream\'s source')
        try:
            await self._send_message_implementation(message)
        except Exception as e:
            raise MessageSendingError(wrapped_exception=e) from None

    async def send_data_message(self, data: bytes, destination: AddressChain, *, session: uuid.UUID, source: Optional[AddressChain] = None) -> Message:
        """
        send message through stream

        :raises MessageSendingError: wrapping actual error occured
        :raises MessageRoutingError: if destination does not contain this stream's destination
        """
        source = source or self.reply_address()
        await self.send_raw_message(Message(data, MessageType.DEFAULT_MESSAGE, source, destination, session))

    def close(self):
        """
        start stream closing process
        if stream is already closing - do nothing
        """
        raise NotImplementedError()

    async def wait_closed(self):
        """
        wait till stream is closed
        if stream is already closed - return immediately
        """
        raise NotImplementedError()


class MessageReceiveStreamBase:
    def __init__(self, listening_address: DirectAddress, other_end_address: DirectAddress):
        self.__source = listening_address
        self.__destination = other_end_address
        self.__message_acked = True

    def listening_address(self) -> DirectAddress:
        return self.__source

    def other_end_address(self) -> DirectAddress:
        return self.__destination

    async def _receive_message_implementation(self) -> Message:
        """
        override this with actual message receiving implementation
        """
        raise NotImplementedError()

    async def _acknowledge_message_implementation(self, status: bool):
        """
        override this with actual message acknowledgement implementation
        """
        raise NotImplementedError()

    async def receive_data_message(self) -> Message:
        """
        receive message from stream

        :raises MessageReceivingError: wrapping actual error occured
        :returns: Message received from the stream
        """
        if not self.__message_acked:
            raise RuntimeError('previous message was not acknowledged')
        try:
            message = await self._receive_message_implementation()
        except Exception as e:
            raise MessageReceivingError(wrapped_exception=e) from None
        else:
            self.__message_acked = False
            return message

    async def acknowledge_received_message(self, success: bool):
        if self.__message_acked:
            raise RuntimeError('there was no message to acknowledge')
        await self._acknowledge_message_implementation(success)
        self.__message_acked = True

    def close(self):
        """
        start stream closing process
        if stream is already closing - do nothing
        """
        raise NotImplementedError()

    async def wait_closed(self):
        """
        wait till stream is closed
        if stream is already closed - return immediately
        """
        raise NotImplementedError()
