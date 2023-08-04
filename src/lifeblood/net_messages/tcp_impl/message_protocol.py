import asyncio
import uuid
import struct

from ..logging import get_logger
from ..stream_wrappers import MessageReceiveStream
from ..messages import Message
from ..queue import MessageQueue
from ..address import DirectAddress
from ..exceptions import MessageReceivingError, NoMessageError, MessageTransferError, MessageTransferTimeoutError
from ..interfaces import MessageStreamFactory

from typing import Callable, Awaitable, Tuple


class IProtocolInstanceCounter:
    def _protocol_inc_count(self, instance):
        raise NotImplementedError()

    def _protocol_dec_count(self, instance):
        raise NotImplementedError()

    def _allowed_new_instances(self) -> bool:
        raise NotImplementedError()


class MessageProtocol(asyncio.StreamReaderProtocol):
    def __init__(self,
                 instance_counter: IProtocolInstanceCounter,
                 reply_address: Tuple[str, int],
                 message_processor_callback: Callable[[Message], Awaitable[bool]]):
        self.__reader = asyncio.StreamReader()
        self.__logger = get_logger('message_protocol')
        self.__callback = message_processor_callback
        self.__listening_address = reply_address
        self.__saved_references = []
        self.__protocol_instance_counter = instance_counter
        self.__needs_to_stop = asyncio.Event()
        super(MessageProtocol, self).__init__(self.__reader, self.connection_callback)

    def stop(self):
        self.__needs_to_stop.set()

    @staticmethod
    async def read_string(reader: asyncio.StreamReader):
        data_size, = struct.unpack('>Q', await reader.readexactly(8))
        return (await reader.readexactly(data_size)).decode('UTF-8')

    async def connection_callback(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        # there is a bug in py <=3.8, callback task can be GCd
        # see https://bugs.python.org/issue46309
        # so we HAVE to save a reference to self somewhere
        self.__saved_references.append(asyncio.current_task())

        if not self.__protocol_instance_counter._allowed_new_instances():
            self.__logger.warning('new connection opened, but server closing, dropping connection')
            writer.close()
            await writer.wait_closed()
            return

        stop_waiter = asyncio.create_task(self.__needs_to_stop.wait())

        self.__protocol_instance_counter._protocol_inc_count(self)
        try:
            message_stream = None
            potentially_pending_tasks = []
            # first what's sent is return address
            try:
                other_stream_source = await self.read_string(reader)

                message_stream = MessageReceiveStream(reader, writer,
                                                      this_address=DirectAddress(':'.join(str(x) for x in self.__listening_address)),
                                                      other_end_address=DirectAddress(other_stream_source))

                while not reader.at_eof():
                    try:
                        message_waiter = asyncio.create_task(message_stream.receive_data_message())
                        potentially_pending_tasks = [message_waiter, stop_waiter]
                        done, pending = await asyncio.wait(potentially_pending_tasks, return_when=asyncio.FIRST_COMPLETED)
                        if stop_waiter in done:
                            self.__logger.debug('explicitly asked to stop')
                            message_waiter.cancel()
                            stop_waiter = None
                            break
                        message = await message_waiter
                    except NoMessageError:  # either reader is closed, or chain broke, so timeout happened
                        continue
                    except MessageTransferTimeoutError as e:  # partial message received. either reader is closed, or chain broke, so timeout happened
                        self.__logger.warning(f'partial message received, discarding, ignoring ({str(e)})')
                        # we cannot trust this stream anymore,
                        # next attempted read will most likely be some partial crap too
                        break

                    success = False
                    try:
                        success = await self.__callback(message)
                    finally:
                        await message_stream.acknowledge_received_message(success)
            except MessageTransferError as mre:
                e = mre.wrapped_exception()
                if isinstance(e, asyncio.exceptions.IncompleteReadError):
                    if len(e.partial) == 0:
                        self.__logger.debug('read 0 bytes, connection closed')
                    else:
                        self.__logger.error(f'read incomplete {len(e.partial)} bytes')
                elif isinstance(e, asyncio.exceptions.TimeoutError):
                    self.__logger.warning(f'connection timeout happened')
                elif isinstance(e, ConnectionResetError):
                    self.__logger.error('connection was reset. disconnected %s', e)
                elif isinstance(e, ConnectionError):
                    self.__logger.error('connection error. disconnected %s', e)
            except Exception as e:
                self.__logger.exception('unknown error. disconnected %s', e)
                raise  # TODO: why raise if noone is catching?
            finally:
                # cleanup pending tasks
                for task in potentially_pending_tasks:
                    if not task.done():
                        task.cancel()

                if message_stream is not None:
                    message_stream.close()
                    try:
                        await message_stream.wait_closed()
                    except Exception as e:
                        self.__logger.warning(f'failed to close stream: {str(e)}')

                # and just in case:
                if not writer.is_closing():
                    self.__logger.warning('stream did not close underlying writer, attempting to close')
                    writer.close()
                    try:
                        await writer.wait_closed()
                    except Exception as e:
                        self.__logger.warning(f'failed to close stream: {str(e)}')
        finally:
            self.__protocol_instance_counter._protocol_dec_count(self)
            # according to the note in the beginning of the function - now reference can be cleared
            self.__saved_references.remove(asyncio.current_task())
