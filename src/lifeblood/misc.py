import asyncio
import random
import time
from .logging import get_logger, logging

from typing import List, Optional


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


def alocking(lock: asyncio.Lock):
    def decorator(func):
        async def _wrapper(*args, **kwargs):
            async with lock:
                return await func(*args, **kwargs)
        return _wrapper
    return decorator
