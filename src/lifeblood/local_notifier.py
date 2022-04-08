import asyncio
import json
from . import broadcasting
from . import logging
from .nethelpers import get_localhost

from typing import Optional, Callable, Coroutine, Any
from .logging import get_logger


class LocalMessageExchanger:
    __logger = None

    def __init__(self, identifier: str, callback: Callable[[Optional[str]], Coroutine]):
        self.__identifier = identifier
        self.__callback = callback
        self.__main_task = None
        self.__stop_event = asyncio.Event()
        self.__signal_number = 0
        if self.__logger is None:
            LocalMessageExchanger.__logger = get_logger('LocalMessageExchanger')

    def start(self):
        if self.__main_task is not None:
            return
        self.__main_task = asyncio.create_task(self.__main_body())

    def stop(self):
        if self.__main_task is None:
            return
        self.__stop_event.set()

    async def wait_till_stopped(self):
        if self.__main_task is None:
            return
        await self.__main_task

    async def __main_body(self):
        stopper = asyncio.create_task(self.__stop_event.wait())
        while not self.__stop_event.is_set():
            broadcaster_waiter = asyncio.create_task(broadcasting.await_broadcast(self.__identifier, 11235, listen_address=get_localhost()))
            done, _ = await asyncio.wait((stopper, broadcaster_waiter), return_when=asyncio.FIRST_COMPLETED)
            if stopper in done:
                broadcaster_waiter.cancel()
                return
            data = json.loads(await broadcaster_waiter)
            self.__logger.debug(f'got {repr(data)}')
            await asyncio.sleep(0.01)  # we wait a tiny bit to prevent catching our own packet. it prevents a bit of flooding
            received_signal_number = data['n']
            message = data['m']
            if received_signal_number <= self.__signal_number:  # ignore
                continue
            self.__signal_number = received_signal_number
            await self.__callback(message)

    async def send_sync_event(self, extra_message=None):
        self.__signal_number += 1
        await broadcasting.create_broadcaster(self.__identifier, json.dumps({'n': self.__signal_number,
                                                                             'm': extra_message}),
                                              broad_port=11235,
                                              ip=get_localhost(),
                                              broadcasts_count=1)


if __name__ == '__main__':  # a little test
    async def callback_test(message):
        print(f'callback! got {message}')

    async def main():
        mess = LocalMessageExchanger('test_exchanger', callback_test)
        mess.start()
        await asyncio.sleep(4)
        await mess.send_sync_event()
        await asyncio.sleep(4)
        mess.stop()
        print('waiting till stopped')
        await mess.wait_till_stopped()

    get_logger('LocalMessageExchanger').setLevel('DEBUG')
    asyncio.run(main())
