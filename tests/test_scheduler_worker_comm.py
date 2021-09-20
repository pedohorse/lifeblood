from unittest import IsolatedAsyncioTestCase
from pathlib import Path
import asyncio
import random
import signal
import subprocess
import time
import sqlite3
import logging

from taskflow.logging import get_logger
from taskflow.worker import Worker
from taskflow.scheduler import Scheduler
from taskflow.taskspawn import NewTask
from taskflow.enums import WorkerType, WorkerState
from taskflow.db_misc import sql_init_script
from taskflow.logging import set_default_loglevel
from taskflow.config import get_config
from taskflow.nethelpers import get_default_addr
from taskflow import launch


def purge_db():
    testdbpath = Path('test_swc.db')
    if testdbpath.exists():
        testdbpath.unlink()
    testdbpath.touch()
    with sqlite3.connect('test_swc.db') as con:
        con.executescript(sql_init_script)


class SchedulerWorkerCommSameProcess(IsolatedAsyncioTestCase):


    # TODO: broadcasting is NOT tested here at all
    @classmethod
    def setUpClass(cls) -> None:
        set_default_loglevel(logging.DEBUG)
        purge_db()
        print('settingup done')

    @classmethod
    def tearDownClass(cls) -> None:
        print('tearingdown done')

    async def test_simple_start_stop(self):
        purge_db()
        sched = Scheduler('test_swc.db', do_broadcasting=False, helpers_minimal_idle_to_ensure=0)
        ip, port = sched.server_address().split(':')
        worker = Worker(ip, int(port))

        await sched.start()
        await worker.start()
        await asyncio.gather(sched.wait_till_starts(),
                             worker.wait_till_starts())

        sched.stop()
        worker.stop()

        await asyncio.gather(sched.wait_till_stops(),
                             worker.wait_till_stops())

    async def test_task_get_order(self):
        purge_db()
        sched = Scheduler('test_swc.db', do_broadcasting=False, helpers_minimal_idle_to_ensure=0)
        ip, port = sched.server_address().split(':')
        worker = Worker(ip, int(port))

        await sched.start()
        await worker.start()
        await asyncio.gather(sched.wait_till_starts(),
                             worker.wait_till_starts())

        for i in range(30*10):
            with sqlite3.connect(database='test_swc.db') as con:
                cur = con.cursor()
                cur.execute('SELECT count("id") FROM workers')
                cnt = cur.fetchone()[0]
                if cnt > 0:
                    self.assertEqual(1, cnt)
                    break
            await asyncio.sleep(0.1)
        print('worker connected to scheduler')

        nid = await sched.add_node('python', 'foof')
        node = await sched.get_node_object_by_id(nid)
        node.set_param_value('process', 'schedule()')
        node.set_param_value('invoke', 'import time\ntime.sleep(1)')
        await sched.spawn_tasks(NewTask('testtask', nid))

        # Note: the order is:
        # - sched sets worker to INVOKING
        # - shced sends "task"
        # - worker receives task, sets is_task_running
        # - worker answers to sched
        # - sched sets worker to BUSY
        # and when finished:
        # - worker reports done             |
        # - sched sets worker to IDLE       | under __task_changing_state_lock
        # - worker unsets is_task_running   |
        # so there is no way it can be not task_running AND sched state busy.
        # if it is - it must be an error
        sttime = time.time()
        state = 0
        sstate = WorkerState.UNKNOWN
        wrun = 0
        wlocked = 0
        _last_state = ()
        while True:
            if (state, sstate, wrun, wlocked) != _last_state:
                print(f'\n\nTESTING:: test state: {state}: sched: {sstate.name}, invoc running: {wrun}, worker state locked: {wlocked}\n\n')
                _last_state = (state, sstate, wrun, wlocked)
            if time.time() - sttime > 60:
                raise AssertionError('timeout reached!')
            wrun = worker.is_task_running()
            wlocked = worker._Worker__task_changing_state_lock.locked()
            with sqlite3.connect(database='test_swc.db') as con:
                cur = con.cursor()
                cur.execute('SELECT "state" FROM workers WHERE "id" = 1')
                sstate = WorkerState(cur.fetchone()[0])

            if state == 0:
                if sstate == WorkerState.INVOKING:
                    state = 1
                else:
                    self.assertFalse(wrun)
                    self.assertEqual(WorkerState.IDLE, sstate)
            if state == 1:
                if wrun:
                    state = 2
                else:
                    self.assertFalse(wrun)
                    self.assertEqual(WorkerState.INVOKING, sstate)
            if state == 2:
                if sstate == WorkerState.BUSY:
                    state = 3
                else:
                    self.assertTrue(wrun)
                    self.assertEqual(WorkerState.INVOKING, sstate)
            if state == 3:
                if sstate == WorkerState.IDLE:
                    state = 4
                else:
                    self.assertTrue(wrun)
                    self.assertEqual(WorkerState.BUSY, sstate)
            if state == 4:
                if not wrun:
                    state = 5
                else:
                    self.assertTrue(wrun)
                    self.assertEqual(WorkerState.IDLE, sstate)
                    self.assertTrue(wlocked)
            if state == 5:
                if not wlocked:
                    state = 6
                else:
                    self.assertFalse(wrun)
                    self.assertEqual(WorkerState.IDLE, sstate)
                    self.assertTrue(wlocked)
            if state == 6:
                self.assertFalse(wrun)
                self.assertEqual(WorkerState.IDLE, sstate)
                self.assertFalse(wlocked)
                break
            await asyncio.sleep(0.0)

        sched.stop()
        worker.stop()

        await asyncio.gather(sched.wait_till_stops(),
                             worker.wait_till_stops())
