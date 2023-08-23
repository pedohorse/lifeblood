import asyncio
import uuid
from .messages import Message
from .queue import MessageQueue
from .address import AddressChain
from .interfaces import MessageStreamFactory
from .exceptions import MessageSendingError, MessageTransferTimeoutError, MessageReceiveTimeoutError
from .logging import get_logger

from typing import Optional


class MessageClient:
    logger = get_logger('message_client')

    def __init__(self, queue: MessageQueue, session: uuid.UUID, *,
                 source_address_chain: AddressChain,
                 destination_address_chain: AddressChain,
                 message_stream_factory: MessageStreamFactory,
                 send_retry_attempts: int = 2):
        self.__message_queue = queue
        self.__last_sent_message: Optional[Message] = None
        self.__session: uuid.UUID = session
        self.__message_stream_factory = message_stream_factory

        self.__source = source_address_chain.split_address()
        self.__source_str = source_address_chain

        self.__destination = destination_address_chain.split_address()
        self.__destination_str = destination_address_chain

        self.__attempts = send_retry_attempts
        self.__init_timeout = 1

    def set_base_attempt_timeout(self, timeout: float):
        if timeout <= 0:
            self.logger.warning('cannot set base attempt timeout to value <= 0, reset to default')
            timeout = 1
        self.__init_timeout = timeout

    def session(self) -> uuid.UUID:
        return self.__session

    async def send_message(self, data: bytes) -> Message:
        last_exception = None
        timeout = self.__init_timeout

        for attempt in range(self.__attempts):
            stream = None
            try:
                try:
                    stream = await self.__message_stream_factory.open_sending_stream(self.__destination[0], self.__source[0])
                except OSError as e:
                    raise MessageSendingError(wrapped_exception=e) from None
                message = await stream.send_data_message(data, self.__destination_str, source=self.__source_str, session=self.__session)
                self.__last_sent_message = message
                return message
            except MessageTransferTimeoutError as e:
                self.logger.warning('timeout while trying to send message')
                last_exception = e
                # TODO: should we allow more attempts in this case? kinda like unexpected multiplier for the timeout
            except MessageSendingError as e:
                self.logger.warning(f'failed to send message: {str(e.wrapped_exception())}')
                last_exception = e
            finally:
                if stream is None:
                    self.logger.warning(f'failed to initialize stream: {str(last_exception)}')
                else:
                    stream.close()
                    try:
                        await stream.wait_closed()
                    except Exception as e:
                        self.logger.warning(f'was unable to properly close stream: {str(e)}')
            if attempt < self.__attempts-1:
                self.logger.warning(f'unable to sent message, will retry in {timeout}')
                await asyncio.sleep(timeout)
                timeout *= 2
        else:
            self.logger.error(f'unable to sent message, out of attempts, failing.')
            raise last_exception

    async def receive_message(self, timeout: Optional[float] = 90) -> Message:
        task = self.__message_queue.get_message(self.__session)
        if timeout is None:
            message = await task
        else:
            try:
                message = await asyncio.wait_for(task, timeout=timeout)
            except asyncio.TimeoutError as e:
                raise MessageReceiveTimeoutError(wrapped_exception=e) from None
        return message


class MessageClientFactory:
    def create_message_client(self, queue: MessageQueue, session: uuid.UUID, *,
                              source_address_chain: AddressChain,
                              destination_address_chain: AddressChain,
                              message_stream_factory: MessageStreamFactory,
                              send_retry_attempts: int = 2) -> MessageClient:
        raise NotImplementedError()


class RawMessageClientFactory(MessageClientFactory):
    def create_message_client(self, queue: MessageQueue, session: uuid.UUID, *,
                              source_address_chain: AddressChain,
                              destination_address_chain: AddressChain,
                              message_stream_factory: MessageStreamFactory,
                              send_retry_attempts: int = 2) -> MessageClient:
        return MessageClient(queue, session,
                             source_address_chain=source_address_chain,
                             destination_address_chain=destination_address_chain,
                             message_stream_factory=message_stream_factory,
                             send_retry_attempts=send_retry_attempts)
