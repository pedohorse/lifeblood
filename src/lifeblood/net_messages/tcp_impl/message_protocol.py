import asyncio
import uuid

from ..logging import get_logger
from ..stream_wrappers import MessageStream
from ..messages import Message
from ..queue import MessageQueue

from typing import Callable, Awaitable, Tuple


class MessageProtocol(asyncio.StreamReaderProtocol):
    def __init__(self, reply_address: Tuple[str, int], message_processor_callback: Callable[[Message], Awaitable[None]]):
        self.__reader = asyncio.StreamReader()
        self.__logger = get_logger('message_protocol')
        self.__callback = message_processor_callback
        self.__listening_address = reply_address
        self.__saved_references = []
        super(MessageProtocol, self).__init__(self.__reader, self.connection_callback)

    async def connection_callback(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        # there is a bug in py <=3.8, callback task can be GCd
        # see https://bugs.python.org/issue46309
        # so we HAVE to save a reference to self somewhere
        self.__saved_references.append(asyncio.current_task())

        message_stream = MessageStream(reader, writer, ':'.join(str(x) for x in self.__listening_address))

        while not reader.at_eof():
            try:
                message = await message_stream.receive_data_message()
                await self.__callback(message)
            except asyncio.exceptions.TimeoutError as e:
                self.__logger.warning(f'connection timeout happened')
            except ConnectionResetError as e:
                self.__logger.exception('connection was reset. disconnected %s', e)
            except ConnectionError as e:
                self.__logger.exception('connection error. disconnected %s', e)
            except Exception as e:
                self.__logger.exception('unknown error. disconnected %s', e)
                raise
            finally:
                writer.close()
                await writer.wait_closed()
                # according to the note in the beginning of the function - now reference can be cleared
                self.__saved_references.remove(asyncio.current_task())
