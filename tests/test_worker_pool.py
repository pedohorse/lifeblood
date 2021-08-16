
from unittest import TestCase
import asyncio
import random
import signal
import subprocess
import time
#import tracemalloc

from taskflow.logging import get_logger
from taskflow.worker_pool import WorkerPool, create_worker_pool
from taskflow.enums import WorkerType, WorkerState
from taskflow import launch


class WorkerPoolTests(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._scheduler_proc = subprocess.Popen(['python', '-m', 'taskflow.launch', 'scheduler', '--db_path', 'test_empty.db'], close_fds=True)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._scheduler_proc: subprocess.Popen
        cls._scheduler_proc.send_signal(signal.SIGINT)
        cls._scheduler_proc.wait()

    def __init__(self, method='runTest'):
        super(WorkerPoolTests, self).__init__(method)
        get_logger(WorkerPool.__name__.lower()).setLevel('DEBUG')

    async def _helper_test_basic(self, rnd):
        print('a')
        swp = await create_worker_pool()
        print('b')
        await swp.add_worker()
        print('c')
        await asyncio.sleep(rnd.uniform(0, 12))
        workers = swp.list_workers()
        self.assertEqual(1, len(workers))
        self.assertEqual(WorkerState.IDLE, workers[0].state)
        swp.stop()
        await swp.wait_till_stops()

    def test_basic(self):
        rnd = random.Random(666)
        for _ in range(3):
            asyncio.run(self._helper_test_basic(rnd))
            time.sleep(1)

    async def _helper_test_min1(self, rnd):
        mint = 4
        mini = 1
        swp = await create_worker_pool(minimal_total_to_ensure=mint, minimal_idle_to_ensure=mini)
        await asyncio.sleep(rnd.uniform(0, 1))
        workers = swp.list_workers()
        self.assertEqual(mint, len(workers))
        tuple(workers.values())[-1].process.send_signal(signal.SIGINT)
        tuple(workers.values())[-2].process.send_signal(signal.SIGINT)
        await asyncio.sleep(rnd.uniform(0, 1))
        workers = swp.list_workers()
        self.assertEqual(mint, len(workers))
        await asyncio.sleep(rnd.uniform(0, 12))
        swp.stop()
        await swp.wait_till_stops()

    def test_min1(self):
        rnd = random.Random(666)
        for _ in range(3):
            asyncio.run(self._helper_test_min1(rnd))

    async def _helper_test_max1(self, rnd):
        maxt = 5
        swp = await create_worker_pool()
        swp.set_maximum_workers(maxt)
        for i in range(maxt+5):
            await swp.add_worker()
            await asyncio.sleep(rnd.uniform(0, 1))
            workers = swp.list_workers()
            self.assertEqual(min(i + 1, maxt), len(workers))
        swp.stop()
        await swp.wait_till_stops()

    def test_max1(self):
        rnd = random.Random(666)
        for _ in range(3):
            asyncio.run(self._helper_test_max1(rnd))

    async def _helper_test_smth1(self, rnd):
        swp = await create_worker_pool(minimal_idle_to_ensure=1)
        await asyncio.sleep(2)
        swp.stop()
        await swp.wait_till_stops()

    def test_smth1(self):
        rnd = random.Random(666)
        for _ in range(5):
            asyncio.run(self._helper_test_smth1(rnd))

