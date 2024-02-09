import sys
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
from lifeblood.defaults import scheduler_message_port
from lifeblood.net_messages.address import AddressChain


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
        cls._scheduler_proc = subprocess.Popen([sys.executable, '-m',
                                                'lifeblood.launch', 'scheduler',
                                                '--db-path', 'test_empty.db',
                                                '--broadcast-interval', '2'], close_fds=True)
        config = get_config('scheduler')  # TODO: don't load actual local configuration, override with temporary!
        server_ip = config.get_option_noasync('core.server_ip', get_default_addr())
        server_port = config.get_option_noasync('core.server_message_port', scheduler_message_port())
        cls.sched_addr = AddressChain(f'{server_ip}:{server_port}')
        time.sleep(1.5)  # this is very arbitrary, but i'm too lazy to
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
        await asyncio.sleep(rnd.uniform(2, 4))  # awaiting so that worker gets a broadcast from scheduler, which is now every 2s
        workers = swp.list_workers()
        self.assertEqual(WorkerState.IDLE, workers[0].state)
        swp.stop()
        await swp.wait_till_stops()

    async def test_basic(self):
        rnd = random.Random(666)
        for _ in range(3):
            await self._helper_test_basic(rnd)
            await asyncio.sleep(1)

    async def test_back_to_idle_min1(self):
        rng = random.Random(48172)
        await self._helper_test_min_from_idle(rng, 1, False)

    async def test_back_to_total_min1(self):
        rng = random.Random(48173)
        await self._helper_test_min_from_idle(rng, 1, True)

    async def test_back_to_idle_min2(self):
        rng = random.Random(48174)
        await self._helper_test_min_from_idle(rng, 2, False)

    async def test_back_to_total_min2(self):
        rng = random.Random(48175)
        await self._helper_test_min_from_idle(rng, 2, True)

    async def test_back_to_idle_min3(self):
        rng = random.Random(48176)
        await self._helper_test_min_from_idle(rng, 3, False)

    async def test_back_to_total_min3(self):
        rng = random.Random(48177)
        await self._helper_test_min_from_idle(rng, 3, True)

    async def _helper_test_min1(self, rnd):
        mint = 4
        mini = 1
        swp = await create_worker_pool(
            minimal_total_to_ensure=mint,
            minimal_idle_to_ensure=mini,
            worker_suspicious_lifetime=0,
            scheduler_address=WorkerPoolTests.sched_addr)
        await asyncio.sleep(rnd.uniform(0, 1))
        workers = swp.list_workers()
        self.assertEqual(mint, len(workers))
        tuple(workers.values())[-1].process.send_signal(signal.SIGTERM)
        tuple(workers.values())[-2].process.send_signal(signal.SIGTERM)
        await asyncio.sleep(rnd.uniform(1, 2))
        workers = swp.list_workers()
        self.assertEqual(mint, len(workers))
        await asyncio.sleep(rnd.uniform(1, 4))
        swp.stop()
        await swp.wait_till_stops()

    async def _helper_test_min_from_idle(self, rnd, mini: int, test_total: bool = False):
        if test_total:
            step1_total = mini * 2
            step1_idle = 0
            step2_total = mini
            step2_idle = 0
        else:
            step1_total = mini * 2
            step1_idle = mini
            step2_total = 0
            step2_idle = mini

        swp = await create_worker_pool(
            minimal_total_to_ensure=step1_total,  # force extras to start
            minimal_idle_to_ensure=step1_idle,
            worker_suspicious_lifetime=0,
            housekeeping_interval=0.2,
            idle_timeout=0.3,
            scheduler_address=WorkerPoolTests.sched_addr)
        await asyncio.sleep(rnd.uniform(0, 1))
        workers = swp.list_workers()
        self.assertEqual(mini*2, len(workers))
        # preparation completed

        swp.set_minimum_total_workers(step2_total)
        swp.set_minimum_idle_workers(step2_idle)
        time_mark = time.perf_counter()
        now = time_mark
        while now - time_mark < 2:
            # if worker count drops below min - means we have some flawed logic in worker pool
            self.assertLessEqual(mini, len(swp.list_workers()))
            await asyncio.sleep(0.000001)
            now = time.perf_counter()

        # we calc list, but allow some workers to be not actually dead yet
        num_all = len(swp.list_workers())
        num_all_no_term = len([w for w, d in swp.list_workers().items() if not d.sent_term_signal])
        self.assertEqual(mini, num_all_no_term)
        if num_all != num_all_no_term:  # so we are waiting for some workers to report terminated - we wait
            print('WAITING FOR WORKERS TO FINISH')
            time_mark = time.perf_counter()
            now = time_mark
            while now - time_mark < 10:
                self.assertLessEqual(mini, len(swp.list_workers()))
                if len(swp.list_workers()) == mini:
                    break
                await asyncio.sleep(0.1)
                now = time.perf_counter()
            else:
                raise AssertionError('unfinished workers never finished')

        await asyncio.sleep(0)
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
            await asyncio.sleep(rnd.uniform(0.2, 1))
            # NOTE: there is NO real guarantee that pool had enough time to spawn a worker, we just rely on sane conditions here
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