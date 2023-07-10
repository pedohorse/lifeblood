import asyncio
import uuid
import struct

from ..logging import get_logger
from ..stream_wrappers import MessageReceiveStream
from ..messages import Message
from ..queue import MessageQueue
from ..address import DirectAddress
from ..exceptions import MessageReceivingError
from ..interfaces import MessageStreamFactory

from typing import Callable, Awaitable, Tuple


class MessageProtocol(asyncio.StreamReaderProtocol):
    def __init__(self, reply_address: Tuple[str, int],
                 message_processor_callback: Callable[[Message], Awaitable[bool]]):
        self.__reader = asyncio.StreamReader()
        self.__logger = get_logger('message_protocol')
        self.__callback = message_processor_callback
        self.__listening_address = reply_address
        self.__saved_references = []
        super(MessageProtocol, self).__init__(self.__reader, self.connection_callback)

    @staticmethod
    async def read_string(reader: asyncio.StreamReader):
        data_size, = struct.unpack('>Q', await reader.readexactly(8))
        return (await reader.readexactly(data_size)).decode('UTF-8')

    async def connection_callback(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        # there is a bug in py <=3.8, callback task can be GCd
        # see https://bugs.python.org/issue46309
        # so we HAVE to save a reference to self somewhere
        self.__saved_references.append(asyncio.current_task())

        # first what's sent is return address
        try:
            other_stream_source = await self.read_string(reader)

            message_stream = MessageReceiveStream(reader, writer,
                                                  this_address=DirectAddress(':'.join(str(x) for x in self.__listening_address)),
                                                  other_end_address=DirectAddress(other_stream_source))

            while not reader.at_eof():
                message = await message_stream.receive_data_message()
                success = False
                try:
                    success = await self.__callback(message)
                finally:
                    await message_stream.acknowledge_received_message(success)
        except MessageReceivingError as mre:
            e = mre.wrapped_exception()
            if isinstance(e, asyncio.exceptions.IncompleteReadError):
                if len(e.partial) == 0:
                    self.__logger.debug('read 0 bytes, connection closed')
                else:
                    self.__logger.error(f'read incomplete {len(e.partial)} bytes')
            elif isinstance(e, asyncio.exceptions.TimeoutError):
                self.__logger.warning(f'connection timeout happened')
            elif isinstance(e, ConnectionResetError):
                self.__logger.exception('connection was reset. disconnected %s', e)
            elif isinstance(e, ConnectionError):
                self.__logger.exception('connection error. disconnected %s', e)
        except Exception as e:
            self.__logger.exception('unknown error. disconnected %s', e)
            raise
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception as e:
                self.__logger.warning(f'failed to close stream: {str(e)}')
            # according to the note in the beginning of the function - now reference can be cleared
            self.__saved_references.remove(asyncio.current_task())
