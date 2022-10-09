import asyncio
from .logging import get_logger
from .scheduler_task_protocol import SchedulerTaskClient

from typing import Tuple, Callable, Coroutine


class PulseChecker:
    def __init__(self, address: Tuple[str, int], interval: float = 5, maximum_misses: int = 5):
        self.__address = address
        self.__interval = interval
        self.__pinger_task = None
        self.__stop_event = asyncio.Event()
        self.__misses = 0
        self.__maximum_misses = maximum_misses
        self.__miss_reported = False
        self.__logger = get_logger('Pulse')

        self.__on_fail_callbacks = set()

    async def start(self):
        self.__pinger_task = asyncio.create_task(self.pinger())

    def stop(self):
        if self.__pinger_task is None:
            raise RuntimeError('not started')
        self.__stop_event.set()

    async def wait_till_stops(self):
        if self.__pinger_task is None:
            raise RuntimeError('not started')
        await self.__pinger_task

    def __await__(self):
        return self.wait_till_stops().__await__()

    def add_pulse_fail_callback(self, async_func: Callable[[], Coroutine]):
        self.__on_fail_callbacks.add(async_func)

    def remove_pulse_fail_callback(self, async_func: Callable[[], Coroutine]):
        self.__on_fail_callbacks.remove(async_func)

    async def pinger(self):
        stop_waiter = asyncio.create_task(self.__stop_event.wait())
        while not self.__stop_event.is_set():
            done, _ = await asyncio.wait([stop_waiter], timeout=self.__interval)
            if stop_waiter in done:
                break

            try:
                async with SchedulerTaskClient(*self.__address) as client:
                    await client.pulse()
                    self.__misses = 0
                    if self.__miss_reported:
                        self.__logger.info('pulse restored')
                        self.__miss_reported = False
            except (ConnectionError, OSError):
                self.__misses += 1
                self.__logger.warning(f'scheduler missed pulse, current miss count: {self.__misses}')

            if self.__misses >= self.__maximum_misses:
                self.__logger.warning(f'shceduler missed {self.__misses} pulses, reporting')
                self.__miss_reported = True
                for func in self.__on_fail_callbacks:
                    await func()
