import asyncio
from pathlib import Path
import sqlite3
from warnings import warn
from aiosqlite import *
from .logging import get_logger

from typing import Any, Callable, List, Optional, Tuple, Union


logger = get_logger('aiosqlite_overlay')


class ConnectionWithCallbacks(Connection):
    def __init__(self, *args, pragmas_after_connect=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.__callbacks: List[Tuple[Callable, tuple, dict]] = []
        self.__post_connect_pragmas = pragmas_after_connect

    def add_after_commit_callback(self, callable: Callable, *args, **kwargs):
        self.__callbacks.append((callable, args, kwargs))

    async def __aenter__(self) -> Connection:
        con = await super().__aenter__()
        if self.__post_connect_pragmas is not None:
            for statement in self.__post_connect_pragmas:
                await con.execute(f'PRAGMA {statement}')
        return con

    async def commit(self):
        await super().commit()
        for callback, args, kwargs in self.__callbacks:
            try:
                callback(*args, **kwargs)
            except Exception as e:
                logger.exception(f'failed to call post-commit callback {e}')
        self.__callbacks = []

    async def rollback(self) -> None:
        await super().rollback()
        self.__callbacks = []

#
# the function below is a direct copy from aiosqlite with the change of Connection class
# PLEASE don't forget to check this is asyncio version changes much
def connect(
    database: Union[str, Path],
    *,
    iter_chunk_size=64,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    pragmas_after_connect=None,
    **kwargs: Any
) -> ConnectionWithCallbacks:
    """Create and return a connection proxy to the sqlite database."""

    if loop is not None:
        warn(
            "aiosqlite.connect() no longer uses the `loop` parameter",
            DeprecationWarning,
        )

    def connector() -> sqlite3.Connection:
        if isinstance(database, str):
            loc = database
        elif isinstance(database, bytes):
            loc = database.decode("utf-8")
        else:
            loc = str(database)

        return sqlite3.connect(loc, **kwargs)

    return ConnectionWithCallbacks(connector, iter_chunk_size, pragmas_after_connect=pragmas_after_connect)
