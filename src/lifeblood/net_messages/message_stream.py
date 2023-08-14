import uuid
from .exceptions import MessageSendingError, MessageReceivingError, MessageRoutingError, MessageTransferError
from .messages import Message
from .address import AddressChain, DirectAddress
from .enums import MessageType
from .defaults import default_stream_timeout

from typing import Optional


class MessageSendStreamBase:
    """
    Represents a direct channel between source and destination
    """
    def __init__(self, reply_address: DirectAddress, destination_address: DirectAddress, 
                 stream_timeout: Optional[float] = default_stream_timeout,
                 message_confirmation_timeout: Optional[float] = default_stream_timeout):
        self.__source = reply_address
        self.__destination = destination_address
        self.__timeout = stream_timeout if stream_timeout and stream_timeout > 0 else None
        self.__confirmation_timeout = message_confirmation_timeout
        self.__ping_timeout = message_confirmation_timeout

    def _stream_timeout(self) -> Optional[float]:
        return self.__timeout

    def _confirmation_timeout(self) -> Optional[float]:
        """
        how long to wait from message sending till confirmation receiving
        """
        return self.__confirmation_timeout

    def reply_address(self) -> DirectAddress:
        return self.__source

    def destination_address(self) -> DirectAddress:
        return self.__destination

    async def _send_message_implementation(self, message: Message, timeout: Optional[float]):
        """
        override this with actual message sending implementation

        on timeout should raise MessageTransferTimeoutError
        """
        raise NotImplementedError()

    async def _ack_message_implementation(self, message: Message, timeout: Optional[float]):
        """
        implementation-specific ack received after message is sent

        on timeout should raise MessageTransferTimeoutError
        """
        raise NotImplementedError()

    async def send_raw_message(self, message: Message, *, message_delivery_timeout_override: Optional[float] = ...):
        """
        send message as is

        :raises MessageSendingError: wrapping actual error occured
        """
        if message.message_destination().split_address()[0] != self.destination_address():
            raise MessageRoutingError('message destination address must contain stream\'s destination')
        if message.message_source().split_address()[0] != self.reply_address():
            raise MessageRoutingError('message source address must contain stream\'s source')
        try:
            await self._send_message_implementation(message, self._stream_timeout())
            await self._ack_message_implementation(message, self._confirmation_timeout() if message_delivery_timeout_override is ... else message_delivery_timeout_override)
        except MessageTransferError:
            raise
        except Exception as e:
            raise MessageSendingError(wrapped_exception=e) from None

    async def send_ping(self):
        await self.send_raw_message(Message(b'', MessageType.SYSTEM_PING, self.__source, self.__destination, None),
                                    message_delivery_timeout_override=self.__ping_timeout)

    async def send_data_message(self, data: bytes, destination: AddressChain, *, session: uuid.UUID, source: Optional[AddressChain] = None) -> Message:
        """
        send message through stream

        :raises MessageSendingError: wrapping actual error occured
        :raises MessageRoutingError: if destination does not contain this stream's destination
        """
        source = source or self.reply_address()
        message = Message(data, MessageType.DEFAULT_MESSAGE, source, destination, session)
        await self.send_raw_message(message)
        return message

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
    def __init__(self, listening_address: DirectAddress, other_end_address: DirectAddress, stream_timeout: Optional[float] = default_stream_timeout):
        self.__source = listening_address
        self.__destination = other_end_address
        self.__message_acked = True
        self.__timeout = stream_timeout if stream_timeout and stream_timeout > 0 else None

    def _stream_timeout(self) -> Optional[float]:
        return self.__timeout

    def listening_address(self) -> DirectAddress:
        return self.__source

    def other_end_address(self) -> DirectAddress:
        return self.__destination

    async def _receive_message_implementation(self) -> Message:
        """
        override this with actual message receiving implementation

        other implementation-specific exceptions will be wrapped into MessageReceivingError by receive_data_message

        :raises NoMessage: special case of timeout error when no data was read
        :raises MessageTransferTimeoutError: timeout error, when partial data was read
        """
        raise NotImplementedError()

    async def _acknowledge_message_implementation(self, status: bool):
        """
        override this with actual message acknowledgement implementation

        implementation-specific exceptions will be wrapped into MessageReceivingError by acknowledge_received_message
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
        except MessageTransferError:
            raise
        except Exception as e:
            raise MessageReceivingError(wrapped_exception=e) from None
        else:
            self.__message_acked = False
            return message

    async def acknowledge_received_message(self, success: bool):
        if self.__message_acked:
            raise RuntimeError('there was no message to acknowledge')
        try:
            await self._acknowledge_message_implementation(success)
        except MessageTransferError:
            raise
        except Exception as e:
            raise MessageTransferError(wrapped_exception=e) from None
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
