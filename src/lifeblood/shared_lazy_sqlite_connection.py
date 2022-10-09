import asyncio
import aiosqlite
from typing import Optional, Dict, List, Callable
from .config import get_config
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
    __close_callbacks: Optional[List[Callable]] = None
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

    def add_close_callback(self, callback: Callable) -> None:
        if self.__close_callbacks is None:
            self.__close_callbacks = []
        self.__close_callbacks.append(callback)

    def add_close_callback_if_not_in(self, callback: Callable) -> None:
        if self.__close_callbacks is not None and callback in self.__close_callbacks:
            return
        self.add_close_callback(callback)

    def _invoke_close_callbacks(self):
        if self.__close_callbacks is None:
            return
        try:
            for cb in self.__close_callbacks:
                cb()
        except Exception:
            get_logger('ConnectionPoolEntry').exception('problem running close connection callbacks. callback chain aborted.')
        finally:
            self.__close_callbacks = None  # why? dunno


class ConnectionPool:
    default_period = get_config('scheduler').get_option_noasync('shared_connection.keep_open_period', 0.0125)
    get_logger('lazy_connection_pool').debug(f'using default open period: {default_period}')

    def __init__(self):
        self.connection_cache: Dict[tuple, ConnectionPoolEntry] = {}
        self.pool_lock = asyncio.Lock()
        self.keep_open_period = self.default_period

    async def connection_closer(self, key):
        await asyncio.sleep(self.keep_open_period)
        get_logger('shared_aiosqlite_connection').debug('shared connection lifetime reached, will close')
        async with self.pool_lock:
            entry = self.connection_cache[key]
            if entry.count > 0:
                entry.do_close = True
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
            entry._invoke_close_callbacks()


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
    connection_pool = None

    def __init__(self, conn_pool, db_path: str, cache_name: str, *args, **kwargs):
        if conn_pool is not None:
            raise NotImplementedError()
        if conn_pool is None and SharedLazyAiosqliteConnection.connection_pool is None:
            SharedLazyAiosqliteConnection.connection_pool = ConnectionPool()
            get_logger('shared_aiosqlite_connection').debug('default connection pool created')
        self.__pool = SharedLazyAiosqliteConnection.connection_pool

        self.__db_path = db_path
        self.__con_args = args
        self.__con_kwargs = kwargs
        self.__cache_key = (self.__db_path, cache_name, tuple(args), tuple(kwargs.items()))
        self.__con: Optional[aiosqlite.Connection] = None

    async def __aenter__(self):
        # TODO: CBB: currently constant stream of overlapping transactions can keep one connection open forever
        #  luckily currently we cannot have that cuz this lazy conn is only used by _awaiter, and it's transactions are
        #  additionally serialized by a lock.
        #  btw with lazy transactions like this we can remove that lock (the idea for it was that sqlite's locking is faaar slower than explicit threading lock, but if it's all one same transaction - no need in locking at all)
        #  and gain probably a lot of performance
        #  so
        #  the problem can be fixed if all instances remember entry at __init__ or __aenter__ and not use it in __aexit__ or anywhere else after
        #  then that closer task can delete connection_cache entry, keeping it internaly till everyone exits
        #  then new transaction with same key will not see existing cache and create a new entry.
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

    async def commit(self, callback=None):
        """
        optional callback argument for convenience.
        will be treated as unique callback, so same element will be added only once

        :param callback: callback to be launched right after db connection close
        :return:
        """
        entry = self.__pool.connection_cache[self.__cache_key]
        entry.need_to_commit = True
        if callback is not None:
            entry.add_close_callback_if_not_in(callback)

    async def rollback(self):
        raise RuntimeError('cannot rollback on shared connection')

    def __getattr__(self, item):
        return getattr(self.__con, item)
