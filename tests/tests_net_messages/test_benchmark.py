import asyncio
import random
import string
import time
import threading
from unittest import IsolatedAsyncioTestCase, skip
from lifeblood.logging import get_logger, set_default_loglevel
from lifeblood.nethelpers import get_localhost
from lifeblood.net_messages.address import AddressChain, DirectAddress
from lifeblood.net_messages.messages import Message
from lifeblood.net_messages.client import MessageClient
from lifeblood.net_messages.exceptions import MessageSendingError, MessageTransferTimeoutError

from lifeblood.net_messages.impl.tcp_message_processor import TcpMessageProcessor, TcpMessageProxyProcessor

from typing import Callable, List, Type, Awaitable

set_default_loglevel('DEBUG')
logger = get_logger('message_test')


class NoopMessageServer(TcpMessageProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.test_messages_count = 0

    async def process_message(self, message: Message, client: MessageClient):
        self.test_messages_count += 1


class ThreadedFoo(threading.Thread):
    def __init__(self, server: TcpMessageProcessor):
        super().__init__()
        self.__stop = False
        self.__server = server

    def run(self):
        asyncio.run(self.async_run())

    async def async_run(self):
        await self.__server.start()
        while True:
            await asyncio.sleep(1)
            if self.__stop:
                break

        self.__server.stop()
        await self.__server.wait_till_stops()

    def stop(self):
        # crude crude crude
        self.__stop = True


class TestBenchmarkSendReceive(IsolatedAsyncioTestCase):
    async def test1(self):
        data = ''.join(random.choice(string.ascii_letters) for _ in range(1024)).encode('latin1')
        server1 = NoopMessageServer((get_localhost(), 28385))
        server2 = NoopMessageServer((get_localhost(), 28386))
        server1_runner = ThreadedFoo(server1)
        server1_runner.start()
        await server2.start()

        async def test_foo():
            with server2.message_client(AddressChain(f'{get_localhost()}:28385')) as client:  # type: MessageClient
                for _ in range(10):
                    await client.send_message(data)

        tasks = []
        for _ in range(100):
            tasks.append(asyncio.create_task(test_foo()))

        timestamp = time.perf_counter()
        await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
        total_time = time.perf_counter() - timestamp

        server2.stop()
        server1_runner.stop()
        await server2.wait_till_stops()
        server1_runner.join()
        self.assertEqual(1000, server1.test_messages_count)
        print(f'total go {server1.test_messages_count} in {total_time}s, avg {server1.test_messages_count/total_time} msg/s')