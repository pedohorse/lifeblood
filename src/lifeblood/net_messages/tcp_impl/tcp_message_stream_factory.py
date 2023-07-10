import asyncio
import struct
from datetime import datetime
from dataclasses import dataclass
from ..interfaces import MessageStreamFactory
from ..stream_wrappers import MessageSendStream, MessageSendStreamBase
from ..address import DirectAddress, AddressChain

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


class ReusableMessageSendStream(MessageSendStream):
    def __init__(self, pool_entry: ConnectionPoolEntry, *, reply_address: DirectAddress, destination_address: DirectAddress):
        super().__init__(pool_entry.reader, pool_entry.writer, reply_address=reply_address, destination_address=destination_address)
        self.__pool_entry = pool_entry
        self.__closed = False
        self.__pool_entry.users_count += 1

    def close(self):
        if self.__closed:
            return
        self.__closed = True
        self.__pool_entry.last_used = datetime.now()
        self.__pool_entry.users_count -= 1

    async def wait_closed(self):
        return


class TcpMessageStreamPooledFactory:
    def __init__(self, pooled_connection_life: int = 0, connection_open_function: Optional[Callable[[DirectAddress, DirectAddress], Awaitable[Tuple[asyncio.StreamReader, asyncio.StreamWriter]]]] = None):
        self.__pooled_connection_life = pooled_connection_life
        self.__pool: Dict[Tuple[str, int], List[ConnectionPoolEntry]] = {}
        self.__connection_open_func: Callable[[DirectAddress, DirectAddress], Awaitable[Tuple[asyncio.StreamReader, asyncio.StreamWriter]]] = connection_open_function or _initialize_connection

    async def prune(self):
        keys_to_remove = []
        now = datetime.now()
        for key, entry_list in self.__pool.items():
            new_entries = []
            for entry in entry_list:
                if entry.users_count > 0 or (now - entry.last_used).total_seconds() < self.__pooled_connection_life:
                    new_entries.append(entry)
                else:
                    entry.writer.close()
                    await entry.writer.wait_closed()
                if len(new_entries) == 0:
                    keys_to_remove.append(key)
                else:
                    self.__pool[key] = new_entries
        for key in keys_to_remove:
            self.__pool.pop(key)

    def _get_cached_entry(self, host: str, port: int) -> Optional[ConnectionPoolEntry]:
        key = (host, port)
        if key not in self.__pool:
            return None

        entry_list = self.__pool[key]
        to_remove = []
        selected: Optional[ConnectionPoolEntry] = None
        for entry in entry_list:
            if entry.users_count > 0:
                continue
            if entry.reader.at_eof() or entry.writer.is_closing():
                to_remove.append(entry)
                continue
            selected = entry
        for entry in to_remove:  # not the most optimal way......
            entry_list.remove(entry)
        return selected

    async def open_sending_stream(self, destination: DirectAddress, source: DirectAddress) -> MessageSendStreamBase:
        host, sport = destination.split(':')
        port = int(sport)
        entry = self._get_cached_entry(host, port)
        if entry is None:
            key = (host, port)
            reader, writer = await self.__connection_open_func(destination, source)
            entry = ConnectionPoolEntry(reader,
                                        writer,
                                        datetime.now(),
                                        0)
            self.__pool.setdefault(key, []).append(entry)
        assert entry is not None

        # Warning: be sure there is no possibility of prune running (no awaits)
        #  between here (where use_count incs)
        #  and where it was fetched/created. We don't want to lose connection we've just got
        stream = ReusableMessageSendStream(entry, reply_address=source, destination_address=destination)
        await self.prune()
        return stream


class TcpMessageStreamFactory(MessageStreamFactory):
    async def open_sending_stream(self, destination: DirectAddress, source: DirectAddress) -> MessageSendStreamBase:
        """
        address is expected to be in form of "1.2.3.4:1313|2.3.4.5:2424"...
        """
        reader, writer = await _initialize_connection(destination, source)

        return MessageSendStream(reader, writer,
                                 reply_address=source,
                                 destination_address=destination)
