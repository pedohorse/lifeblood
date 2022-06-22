
from unittest import IsolatedAsyncioTestCase
from pathlib import Path
import asyncio
import random
import signal
import subprocess
import time
import tracemalloc

from lifeblood.logging import get_logger
from lifeblood.simple_worker_pool import WorkerPool, create_worker_pool
from lifeblood.enums import WorkerType, WorkerState
from lifeblood.config import get_config
from lifeblood.nethelpers import get_default_addr
from lifeblood import launch


class WorkerPoolTests(IsolatedAsyncioTestCase):
    sched_addr = None
    _scheduler_proc = None

    # TODO: broadcasting is NOT tested here at all
    @classmethod
    def setUpClass(cls) -> None:
        print('settingup')
        tracemalloc.start()
        testdbpath = Path('test_empty.db')
        if testdbpath.exists():
            testdbpath.unlink()
        testdbpath.touch()
        cls._scheduler_proc = subprocess.Popen(['python', '-m', 'lifeblood.launch', 'scheduler', '--db-path', 'test_empty.db'], close_fds=True)
        config = get_config('scheduler')  # TODO: don't load actual local configuration, override with temporary!
        server_ip = config.get_option_noasync('core.server_ip', get_default_addr())
        server_port = config.get_option_noasync('core.server_port', 7979)
        cls.sched_addr = (server_ip, server_port)
        time.sleep(5)  # this is very arbitrary, but i'm too lazy to
        print('settingup done')

    @classmethod
    def tearDownClass(cls) -> None:
        print('tearingdown')
        cls._scheduler_proc: subprocess.Popen
        cls._scheduler_proc.send_signal(signal.SIGTERM)
        cls._scheduler_proc.wait()
        tracemalloc.stop()
        print('tearingdown done')

    def __init__(self, method='runTest'):
        super(WorkerPoolTests, self).__init__(method)
        get_logger(WorkerPool.__name__.lower()).setLevel('DEBUG')

    async def _helper_test_basic(self, rnd):
        print('a')
        swp = await create_worker_pool(idle_timeout=30, scheduler_address=WorkerPoolTests.sched_addr)
        print('b')
        await swp.add_worker()
        print('c')
        await asyncio.sleep(rnd.uniform(0, 2))
        workers = swp.list_workers()
        self.assertEqual(1, len(workers))
        await asyncio.sleep(rnd.uniform(10, 15))  # awaiting so that worker gets a broadcast from scheduler, which is now every 10s
        workers = swp.list_workers()
        self.assertEqual(WorkerState.IDLE, workers[0].state)
        swp.stop()
        await swp.wait_till_stops()

    async def test_basic(self):
        rnd = random.Random(666)
        for _ in range(3):
            await self._helper_test_basic(rnd)
            await asyncio.sleep(1)

    async def _helper_test_min1(self, rnd):
        mint = 4
        mini = 1
        swp = await create_worker_pool(minimal_total_to_ensure=mint, minimal_idle_to_ensure=mini, worker_suspicious_lifetime=0, scheduler_address=WorkerPoolTests.sched_addr)
        await asyncio.sleep(rnd.uniform(0, 1))
        workers = swp.list_workers()
        self.assertEqual(mint, len(workers))
        tuple(workers.values())[-1].process.send_signal(signal.SIGTERM)
        tuple(workers.values())[-2].process.send_signal(signal.SIGTERM)
        await asyncio.sleep(rnd.uniform(0, 1))
        workers = swp.list_workers()
        self.assertEqual(mint, len(workers))
        await asyncio.sleep(rnd.uniform(0, 12))
        swp.stop()
        await swp.wait_till_stops()

    async def test_min1(self):
        rnd = random.Random(666)
        for _ in range(3):
            await self._helper_test_min1(rnd)

    async def _helper_test_max1(self, rnd):
        maxt = 5
        swp = await create_worker_pool(scheduler_address=WorkerPoolTests.sched_addr)
        swp.set_maximum_workers(maxt)
        for i in range(maxt+5):
            await swp.add_worker()
            await asyncio.sleep(rnd.uniform(0, 1))
            workers = swp.list_workers()
            self.assertEqual(min(i + 1, maxt), len(workers))
        swp.stop()
        await swp.wait_till_stops()

    async def test_max1(self):
        rnd = random.Random(666)
        for _ in range(3):
            await self._helper_test_max1(rnd)

    async def _helper_test_smth1(self, rnd):
        swp = await create_worker_pool(minimal_idle_to_ensure=1, scheduler_address=WorkerPoolTests.sched_addr)
        await asyncio.sleep(2)
        swp.stop()
        await swp.wait_till_stops()

    async def test_smth1(self):
        rnd = random.Random(666)
        for _ in range(5):
            await self._helper_test_smth1(rnd)

    #TODO: add tests for worker_suspicious_lifetime
    #TODO: add tests for idle_timeout
    #TODO: add sanity check for behaviour when scheduler goes offline