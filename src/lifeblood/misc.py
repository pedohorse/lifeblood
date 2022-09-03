import os
import asyncio
import random
import uuid
import time
import psutil
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


__stashed_machine_uuid = None


def get_unique_machine_id() -> int:
    """
    get a unique machine id (or something as close as possible)

    To work well with sqlite, id must be truncated to 64 bit

    :return: 64 bit integer
    """
    global __stashed_machine_uuid
    logger = get_logger('get_unique_machine_id')

    if __stashed_machine_uuid is None:
        midpaths = ['/etc/machine-id', '/var/lib/dbus/machine-id']
        for midpath in midpaths:
            if os.path.exists(midpath) and os.access(midpath, os.R_OK):
                with open(midpath, 'r') as f:
                    __stashed_machine_uuid = int(f.read(), base=16)
                break
        else:
            try:
                ia_pairs = sorted([(i, a) for i, a in psutil.net_if_addrs().items()], key=lambda x: x[0])  # to be more predictable in order
                for _, addrlist in ia_pairs:
                    for addr in addrlist:
                        if addr.family != psutil.AF_LINK or addr.address == '00:00:00:00:00:00':
                            continue
                        logger.debug(f'trying address: "{addr.address}"')
                        __stashed_machine_uuid = int(''.join(addr.address.split(':')), base=16)
                        break
                    if __stashed_machine_uuid is not None:
                        break
            except Exception as e:
                logger.warning(f'failed to get any MAC address, because of {repr(e)}: {str(e)}')
            if __stashed_machine_uuid is None:
                __stashed_machine_uuid = uuid.getnode()  # this is not very reliable.

        __stashed_machine_uuid &= (1 << 63) - 1  # sqlite eats signed 64b integers, so we leave single bit. though we could still cast and use it...
    return __stashed_machine_uuid
