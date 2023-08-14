import asyncio
import struct
import logging
from lifeblood.logging import get_logger
from datetime import datetime
from dataclasses import dataclass
from ..exceptions import MessageTransferError, MessageTransferTimeoutError
from ..interfaces import MessageStreamFactory
from ..stream_wrappers import MessageSendStream, MessageSendStreamBase
from ..address import DirectAddress, AddressChain
from ..defaults import default_stream_timeout
from ..messages import Message

from typing import Awaitable, Callable, Dict, List, Optional, Tuple


async def _initialize_connection(destination: DirectAddress, source: DirectAddress) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    host, sport = destination.split(':')
    reader, writer = await asyncio.open_connection(host, int(sport))
    source_bytes = source.encode('UTF-8')
    writer.write(struct.pack('>Q', len(source_bytes)))
    writer.write(source_bytes)
    await writer.drain()
    return reader, writer


@dataclass
class ConnectionPoolEntry:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    last_used: datetime
    users_count: int
    close_when_user_count_zero: bool = False
    bad: bool = False


class ReusableMessageSendStream(MessageSendStream):
    """

    """
    def __init__(self,
                 pool_entry: ConnectionPoolEntry,
                 *,
                 reply_address: DirectAddress,
                 destination_address: DirectAddress,
                 stream_timeout: float = default_stream_timeout,
                 confirmation_timeout: float = default_stream_timeout):
        super().__init__(pool_entry.reader,
                         pool_entry.writer,
                         reply_address=reply_address,
                         destination_address=destination_address,
                         stream_timeout=stream_timeout,
                         confirmation_timeout=confirmation_timeout)
        self.__pool_entry = pool_entry
        self.__closed = False
        self.__pool_entry.users_count += 1
        self.__do_actually_wait_closed = False

    async def send_raw_message(self, message: Message, *, message_delivery_timeout_override: Optional[float] = ...):
        try:
            return await super().send_raw_message(message, message_delivery_timeout_override=message_delivery_timeout_override)
        except MessageTransferTimeoutError:
            # we cannot be sure some crap won't arrive after timeout,
            # in that case future uses of this connection will be at risk of getting it,
            # so it's safer to mark it for closure
            self.__pool_entry.close_when_user_count_zero = True
            raise

    def close(self):
        if self.__closed:
            return
        self.__closed = True
        # note: section below might need locking when close() can be called from different threads
        #  actually, there is much more problems if streams are opened/closed from different threads
        self.__pool_entry.last_used = datetime.now()
        self.__pool_entry.users_count -= 1
        if self.__pool_entry.users_count == 0 and self.__pool_entry.close_when_user_count_zero:
            super().close()
            self.__do_actually_wait_closed = True

    async def wait_closed(self):
        if not self.__do_actually_wait_closed:
            return
        await super().wait_closed()

    def __del__(self):
        self.close()  # multiple close() calls are fine


class TcpMessageStreamPooledFactory(MessageStreamFactory):
    _logger: Optional[logging.Logger] = None

    def __init__(self,
                 pooled_connection_life: int = 0,
                 connection_open_function: Optional[Callable[[DirectAddress, DirectAddress], Awaitable[Tuple[asyncio.StreamReader, asyncio.StreamWriter]]]] = None,
                 timeout: float = default_stream_timeout):
        self.__pooled_connection_life = pooled_connection_life
        self.__pool: Dict[Tuple[str, int], List[ConnectionPoolEntry]] = {}
        self.__connection_open_func: Callable[[DirectAddress, DirectAddress], Awaitable[Tuple[asyncio.StreamReader, asyncio.StreamWriter]]] = connection_open_function or _initialize_connection
        self.__open_connection_calls_count = 0
        self.__pool_closed = asyncio.Event()
        self.__timeout = timeout
        if self._logger is None:
            TcpMessageStreamPooledFactory._logger = get_logger('TcpMessageStreamPooledFactory')

        self.__closing_task_to_wait = None

    async def prune(self):
        await self.close_unused_connections_older_than(self.__pooled_connection_life)

    async def close_unused_connections_older_than(self, older_than_this_seconds: float):
        keys_to_remove = []
        now = datetime.now()
        to_wait = []
        for key, entry_list in self.__pool.items():
            new_entries = []
            for entry in entry_list:
                if not entry.bad \
                        and (entry.users_count > 0
                             or ((now - entry.last_used).total_seconds() < older_than_this_seconds
                                 and not entry.close_when_user_count_zero)
                             ):
                    new_entries.append(entry)
                else:
                    if not entry.writer.is_closing():
                        entry.writer.close()
                    to_wait.append(asyncio.create_task(entry.writer.wait_closed()))
            if len(new_entries) == 0:
                keys_to_remove.append(key)
            else:
                self.__pool[key] = new_entries
        for key in keys_to_remove:
            self.__pool.pop(key)

        if to_wait:
            dones, _ = await asyncio.wait(to_wait, return_when=asyncio.ALL_COMPLETED)
            for task in dones:
                exc = task.exception()
                if exc is None:
                    continue
                self._logger.warning(f'pooled connection was faulty and failed to close: "{exc}"')

    def _get_cached_entry(self, host: str, port: int) -> Optional[ConnectionPoolEntry]:
        key = (host, port)
        if key not in self.__pool:
            return None

        entry_list = self.__pool[key]
        # to_remove = []
        selected: Optional[ConnectionPoolEntry] = None
        for entry in entry_list:
            if entry.users_count > 0 or entry.close_when_user_count_zero:
                continue
            if entry.bad or entry.reader.at_eof() or entry.writer.is_closing():
                # to_remove.append(entry)
                entry.bad = True
                continue
            selected = entry
        # for entry in to_remove:  # not the most optimal way......
        #     entry_list.remove(entry)
        return selected

    async def open_sending_stream(self, destination: DirectAddress, source: DirectAddress) -> MessageSendStreamBase:
        if self.__pool_closed.is_set():
            raise RuntimeError('cannot open sending stream when pool closed')
        host, sport = destination.split(':')
        port = int(sport)
        entry = self._get_cached_entry(host, port)
        if entry is not None:  # test entry with ping message
            stream = ReusableMessageSendStream(entry,
                                               reply_address=source,
                                               destination_address=destination,
                                               stream_timeout=self.__timeout,
                                               confirmation_timeout=self.__timeout)
            try:
                await stream.send_ping()
            except MessageTransferError as e:
                self._logger.debug('ping failed due to %s', e)
                entry.bad = True
                entry = None
                stream.close()

        if entry is None:
            key = (host, port)
            reader, writer = await self.__connection_open_func(destination, source)
            self.__open_connection_calls_count += 1
            entry = ConnectionPoolEntry(reader,
                                        writer,
                                        datetime.now(),
                                        0)
            self.__pool.setdefault(key, []).append(entry)
        assert entry is not None

        # Warning: be sure there is no possibility of prune running (no awaits)
        #  between here (where use_count incs)
        #  and where it was fetched/created. We don't want to lose connection we've just got
        stream = ReusableMessageSendStream(entry,
                                           reply_address=source,
                                           destination_address=destination,
                                           stream_timeout=self.__timeout,
                                           confirmation_timeout=self.__timeout)
        await self.prune()
        return stream

    async def close_all_unused_connections(self):
        await self.close_unused_connections_older_than(-1)

    def close_pool(self):
        self.__closing_task_to_wait = asyncio.create_task(self.__closing_task())
        self.__pool_closed.set()
        for items in self.__pool.values():
            for item in items:
                item.close_when_user_count_zero = True

    async def wait_pool_closed(self):
        await self.__pool_closed.wait()
        # by this time self.__closing_task_to_wait will be not None
        await self.__closing_task_to_wait

    async def __closing_task(self):
        await self.__pool_closed.wait()
        while len(self.__pool) > 0:
            await self.close_all_unused_connections()
            await asyncio.sleep(0.5)  # too heavy polling?

    def __del__(self):
        self._logger.debug(f'connections opened total: {self.__open_connection_calls_count}')
        if len(self.__pool) > 0:
            self._logger.warning(f'pooled connections remain ({len(self.__pool)}) after __del__, {len([1 for l in self.__pool.values() for x in l if x.users_count > 0])} still in use')


class TcpMessageStreamFactory(MessageStreamFactory):
    def __init__(self, *, timeout: float = default_stream_timeout):
        self.__timeout = timeout

    async def open_sending_stream(self, destination: DirectAddress, source: DirectAddress) -> MessageSendStreamBase:
        """
        address is expected to be in form of "1.2.3.4:1313|2.3.4.5:2424"...
        """
        reader, writer = await _initialize_connection(destination, source)

        return MessageSendStream(reader, writer,
                                 reply_address=source,
                                 destination_address=destination,
                                 stream_timeout=self.__timeout,
                                 confirmation_timeout=self.__timeout)
