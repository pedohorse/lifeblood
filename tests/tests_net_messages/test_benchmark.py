import os
import asyncio
import random
import string
import time
import threading
import multiprocessing
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

    # async def new_message_received(self, message: Message) -> bool:
    #     self.test_messages_count += 1
    #     return True

    async def process_message(self, message: Message, client: MessageClient):
        self.test_messages_count += 1


class FooRunner:
    def start(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    def join(self):
        raise NotImplementedError()

    def get_message_count(self) -> int:
        raise NotImplementedError()


class ThreadedFoo(threading.Thread, FooRunner):
    def __init__(self, server: NoopMessageServer):
        super().__init__()
        self.__stop = threading.Event()
        self.__ready = threading.Event()
        self.__server = server

    def run(self):
        asyncio.run(self.async_run())

    def start(self):
        super().start()
        self.__ready.wait()

    async def async_run(self):
        await self.__server.start()
        self.__ready.set()
        while True:
            await asyncio.sleep(1)
            if self.__stop.is_set():
                break

        self.__server.stop()
        await self.__server.wait_till_stops()

    def stop(self):
        # crude crude crude
        self.__stop.set()

    def get_message_count(self) -> int:
        return self.__server.test_messages_count


class ProcessedFoo(FooRunner):
    def __init__(self, server: NoopMessageServer):
        super().__init__()
        self.__server = server
        ctx = multiprocessing.get_context('spawn')
        self.__stop = ctx.Event()
        self.__value = ctx.Value('i', -1)
        self.__ready = ctx.Event()
        self.__proc = ctx.Process(target=self.body)

    def start(self):
        self.__proc.start()
        self.__ready.wait()

    def body(self):
        asyncio.run(self.async_run())

    async def async_run(self):
        print('another process started')
        await self.__server.start()
        self.__ready.set()
        print('another process server started')
        while True:
            await asyncio.sleep(1)
            if self.__stop.is_set():
                break

        self.__server.stop()
        await self.__server.wait_till_stops()
        self.__value.value = self.__server.test_messages_count

    def stop(self):
        self.__stop.set()

    def join(self):
        self.__proc.join()

    def get_message_count(self) -> int:
        return self.__value.value


class TestBenchmarkSendReceive(IsolatedAsyncioTestCase):
    @skip("no reason to benchmark on slow machines")
    async def test_threaded(self):
        await self.helper_test(ThreadedFoo)

    @skip("no reason to benchmark on slow machines")
    async def test_proc(self):
        await self.helper_test(ProcessedFoo)

    async def helper_test(self, foo_factory: Callable[[NoopMessageServer], FooRunner]):
        """
        runs 2 servers
        starts X clients asyncio coroutines on one of the servers, each sends Y messages.
        average per-message time is then calculated
        """
        data = ''.join(random.choice(string.ascii_letters) for _ in range(16000)).encode('latin1')
        server1 = NoopMessageServer((get_localhost(), 28385))
        server2 = NoopMessageServer((get_localhost(), 28386))
        server1_runner = foo_factory(server1)
        server1_runner.start()
        await server2.start()
        pure_send_time = 0.0

        messages_per_client = 10
        total_clients = 100

        async def test_foo():
            nonlocal pure_send_time
            with server2.message_client(AddressChain(f'{get_localhost()}:28385')) as client:  # type: MessageClient
                beforesend = time.perf_counter()
                for _ in range(messages_per_client):
                    await client.send_message(data)
                pure_send_time += time.perf_counter() - beforesend

        tasks = []
        for _ in range(total_clients):
            tasks.append(asyncio.create_task(test_foo()))

        timestamp = time.perf_counter()
        await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
        total_time = time.perf_counter() - timestamp
        pure_send_time /= total_clients

        server2.stop()
        server1_runner.stop()
        await server2.wait_till_stops()
        server1_runner.join()
        s1_message_count = server1_runner.get_message_count()
        print(f'threaded total go {s1_message_count} in {total_time}s (pure send: {pure_send_time}s, avg {s1_message_count/total_time} (pure: {s1_message_count/pure_send_time}) msg/s')
        self.assertEqual(total_clients * messages_per_client, s1_message_count)
