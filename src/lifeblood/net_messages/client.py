import asyncio
import uuid
from .messages import Message
from .queue import MessageQueue
from .address import AddressChain
from .interfaces import MessageStreamFactory
from .exceptions import MessageSendingError
from .logging import get_logger

from typing import Optional


class MessageClient:
    logger = get_logger('message_client')

    def __init__(self, queue: MessageQueue, session: uuid.UUID, *,
                 source_address_chain: AddressChain,
                 destination_address_chain: AddressChain,
                 message_stream_factory: MessageStreamFactory,
                 send_retry_attempts: int = 6):
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

    def session(self) -> uuid.UUID:
        return self.__session

    async def send_message(self, data: bytes) -> Message:
        try:
            stream = await self.__message_stream_factory.open_sending_stream(self.__destination[0], self.__source[0])
        except Exception as e:
            raise MessageSendingError('failed to open connection', wrapped_exception=e) from None

        last_exception = None
        timeout = self.__init_timeout
        for attempt in range(self.__attempts):
            try:
                message = await stream.send_data_message(data, self.__destination_str, source=self.__source_str, session=self.__session)
                self.__last_sent_message = message
                return message
            except MessageSendingError as e:
                self.logger.warning(f'failed to send message: {str(e.wrapped_exception())}')
                last_exception = e
            finally:
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

    async def receive_message(self) -> Message:
        message = await self.__message_queue.get_message(self.__session)
        return message
