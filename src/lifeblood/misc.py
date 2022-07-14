import asyncio
import random
import time
from .logging import get_logger, logging

from typing import List, Optional, Union


class DummyLock:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


def atimeit(threshold=0):
    def _atimeit(func):
        logger = get_logger('timeit')
        if not logger.isEnabledFor(logging.DEBUG):
            return func

        async def _wrapper(*args, **kwargs):
            _start = time.perf_counter()
            raised = False
            try:
                return await func(*args, **kwargs)
            except Exception:
                raised = True
                raise
            finally:
                dt = time.perf_counter() - _start
                if dt > threshold:
                    logger.debug(f'ran{"(raised)" if raised else ""} {func.__name__} in {dt}s')

        return _wrapper

    return _atimeit


def timeit(threshold=0):
    def _timeit(func):
        logger = get_logger('timeit')
        if not logger.isEnabledFor(logging.DEBUG):
            return func

        def _wrapper(*args, **kwargs):
            _start = time.perf_counter()
            raised = False
            try:
                return func(*args, **kwargs)
            except Exception:
                raised = True
                raise
            finally:
                dt = time.perf_counter() - _start
                if dt > threshold:
                    logger.debug(f'ran{"(raised)" if raised else ""} {func.__name__} in {dt}s')
        return _wrapper

    return _timeit


__alocking_locks = {}


def alocking(lock_or_name: Union[asyncio.Lock, str, None] = None):  # TODO: TESTS NEEDED!
    def decorator(func):
        async def _wrapper(*args, **kwargs):
            nonlocal lock_or_name
            lock = None
            if isinstance(lock_or_name, asyncio.Lock):
                lock = lock_or_name
            elif lock_or_name is None:
                lock_or_name = func.__name__

            if isinstance(lock_or_name, str):
                if lock_or_name not in __alocking_locks:
                    __alocking_locks[lock_or_name] = asyncio.Lock()
                lock = __alocking_locks[lock_or_name]

            if lock is None:
                raise ValueError('lock value bad')

            async with lock:
                return await func(*args, **kwargs)
        return _wrapper
    return decorator
