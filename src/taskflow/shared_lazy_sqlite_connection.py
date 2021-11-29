import asyncio
import aiosqlite
from typing import Optional, Dict
from .logging import get_logger
from dataclasses import dataclass


@dataclass
class ConnectionPoolEntry:
    con: aiosqlite.Connection
    __count: int = 1
    __count_max: int = 1
    __total_usages: int = 1
    closer_task: Optional[asyncio.Task] = None
    need_to_commit: bool = False
    do_close: bool = False

    @property
    def count(self) -> int:
        return self.__count

    @count.setter
    def count(self, val: int) -> None:
        self.__count_max = max(self.__count_max, val)
        if val > self.__count:
            self.__total_usages += val - self.__count
        self.__count = val

    @property
    def count_max(self) -> int:
        return self.__count_max

    @property
    def total_usages(self) -> int:
        return self.__total_usages


class ConnectionPool:
    def __init__(self):
        self.connection_cache: Dict[tuple, ConnectionPoolEntry] = {}
        self.pool_lock = asyncio.Lock()
        self.keep_open_period = 0.25

    async def connection_closer(self, key):
        await asyncio.sleep(self.keep_open_period)
        async with self.pool_lock:
            entry = self.connection_cache[key]
            if entry.count > 0:
                entry.do_close = 1
            else:  # only == 0 possible
                await self._closer_inner(key)

    async def _closer_inner(self, key):
        assert self.pool_lock.locked()  # cannot quite check if it's locked specifically for us, so this sanity check is partial at best
        entry = self.connection_cache[key]
        try:
            if entry.need_to_commit:
                await entry.con.commit()
        finally:
            await entry.con.close()
            del self.connection_cache[key]
            get_logger('shared_aiosqlite_connection').debug(f'closing shared connection used by total {entry.total_usages}, max {entry.count_max} at the same time. left cached conns: {len(self.connection_cache)}')


class SharedLazyAiosqliteConnection:
    """
    this will NOT work if used in different threads with the same pool!
    collects multiple commits in one.
    be VERY mindful where you use it!
    It was intended to collect the work of multiple similar independent tasks working in parallel
    to avoid hundreds of commits per second

    NOTE! To avoid confusion - row_factory of new connection is always set to ROW
    Be VERY mindful of what uses these shared connections!
    """
    connection_pool = ConnectionPool()

    def __init__(self, conn_pool, db_path: str, cache_name: str, *args, **kwargs):
        if conn_pool is not None:
            raise NotImplementedError()
        self.__pool = SharedLazyAiosqliteConnection.connection_pool

        self.__db_path = db_path
        self.__con_args = args
        self.__con_kwargs = kwargs
        self.__cache_key = (self.__db_path, cache_name, tuple(args), tuple(kwargs.items()))
        self.__con: Optional[aiosqlite.Connection] = None

    async def __aenter__(self):
        async with self.__pool.pool_lock:
            if self.__cache_key in self.__pool.connection_cache:
                self.__con = self.__pool.connection_cache[self.__cache_key].con
                self.__pool.connection_cache[self.__cache_key].count += 1
                return self
            self.__con = aiosqlite.connect(self.__db_path, *self.__con_args, **self.__con_kwargs)
            self.__pool.connection_cache[self.__cache_key] = ConnectionPoolEntry(con=self.__con)
            self.__pool.connection_cache[self.__cache_key].con = self.__con
            self.__pool.connection_cache[self.__cache_key].count = 1
            self.__pool.connection_cache[self.__cache_key].closer_task = asyncio.create_task(self.__pool.connection_closer(self.__cache_key))
            await self.__con
            self.__con.row_factory = aiosqlite.Row
            return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        async with self.__pool.pool_lock:
            entry = self.__pool.connection_cache[self.__cache_key]
            entry.count -= 1
            assert entry.count >= 0
            if entry.count > 0:
                return
            # so here only count == 0
            if entry.do_close:  # so closer task has already finished, and we need to do it's job here
                await self.__pool._closer_inner(self.__cache_key)

    async def commit(self):
        self.__pool.connection_cache[self.__cache_key].need_to_commit = True

    def __getattr__(self, item):
        return getattr(self.__con, item)
