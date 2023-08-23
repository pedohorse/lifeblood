import os
import tempfile
from unittest import IsolatedAsyncioTestCase, mock
from pathlib import Path
import asyncio
import random
import signal
import subprocess
import time
import sqlite3
import logging

from lifeblood.logging import get_logger
from lifeblood.worker import Worker
from lifeblood.invocationjob import InvocationJob
from lifeblood.scheduler import Scheduler
from lifeblood.taskspawn import NewTask
from lifeblood.enums import WorkerType, WorkerState, SpawnStatus, InvocationState, InvocationMessageResult
from lifeblood.db_misc import sql_init_script
from lifeblood.logging import set_default_loglevel
from lifeblood.config import get_config
from lifeblood.nethelpers import get_default_addr
from lifeblood.net_messages.address import AddressChain
from lifeblood import launch

from typing import Awaitable, Callable, List, Optional, Tuple


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
        await sched.start()

        worker = Worker(sched.server_message_address())
        await worker.start()

        await asyncio.gather(sched.wait_till_starts(),
                             worker.wait_till_starts())

        sched.stop()
        worker.stop()

        await asyncio.gather(sched.wait_till_stops(),
                             worker.wait_till_stops())

    async def test_worker_invocation_api1(self):
        async def _logic(scheduler, workers, tmp_script_path, done_waiter):
            with mock.patch('lifeblood.scheduler.scheduler.Scheduler.spawn_tasks') as spawn_patch:
                spawn_patch.side_effect = lambda *args, **kwargs: print(f'spawn_tasks_called with {args}, {kwargs}') \
                                                                  or (SpawnStatus.SUCCEEDED, 2346)

                ij = InvocationJob(
                        ['python', tmp_script_path],
                        invocation_id=1234,
                    )
                ij._set_task_id(2345)
                await workers[0].run_task(
                    ij,
                    scheduler.server_message_address()
                )

                await asyncio.wait([done_waiter], timeout=10)

                self.assertTrue(spawn_patch.call_count == 1)

        await self._helper_test_worker_invocation_api(
            'import lifeblood_connection as lbc\n'
            'lbc.create_task("woobwoob", {"testattr": 42})\n'
            'print("invoc done")\n',
            _logic
        )

    async def test_worker_invocation_api2(self):
        async def _logic(scheduler, workers, tmp_script_path, done_waiter):
            with mock.patch('lifeblood.scheduler.scheduler.Scheduler.update_task_attributes') as attr_patch:
                attr_patch.side_effect = lambda *args, **kwargs: print(f'update attrs with {args}, {kwargs}')

                ij = InvocationJob(
                        ['python', tmp_script_path],
                        invocation_id=1234,
                    )
                ij._set_task_id(2345)
                await workers[0].run_task(
                    ij,
                    scheduler.server_message_address()
                )

                await asyncio.wait([done_waiter], timeout=10)

                self.assertTrue(attr_patch.call_count == 1)
                self.assertEqual(((2345, {"myattree": [1, 2, -42]}, set()),), attr_patch.call_args)

        await self._helper_test_worker_invocation_api(
            'import lifeblood_connection as lbc\n'
            'lbc.set_attributes({"myattree": [1, 2, -42]})\n'
            'print("invoc done")\n',
            _logic
        )

    async def test_worker_invocation_comm_api(self):
        await self._helper_test_worker_invocation_comm_api(
            i1_script=f'import lifeblood_connection as lbc\n'
                      f'lbc.message_to_invocation_send(11235, "foobaaaar", b"IamDATAbanana")\n'
                      f'print("done1")\n',
            i2_script=f'import lifeblood_connection as lbc\n'
                      f'src_inv, data = lbc.message_to_invocation_receive("foobaaaar")\n'
                      f'assert data == b"IamDATAbanana", data\n'
                      f'assert src_inv == 11234, src_inv\n'
                      f'print("done2")\n'
        )

    async def test_worker_invocation_comm_api_send_delay(self):
        await self._helper_test_worker_invocation_comm_api(
            i1_script=f'import lifeblood_connection as lbc\n'
                      f'import time\n'
                      f'time.sleep(5)\n'
                      f'lbc.message_to_invocation_send(11235, "foobaaaar", b"IamDATAbanana")\n'
                      f'print("done1")\n',
            i2_script=f'import lifeblood_connection as lbc\n'
                      f'src_inv, data = lbc.message_to_invocation_receive("foobaaaar")\n'
                      f'assert data == b"IamDATAbanana", data\n'
                      f'assert src_inv == 11234, src_inv\n'
                      f'print("done2")\n'
        )

    async def test_worker_invocation_comm_api_recv_delay(self):
        await self._helper_test_worker_invocation_comm_api(
            i1_script=f'import lifeblood_connection as lbc\n'
                      f'lbc.message_to_invocation_send(11235, "foobaaaar", b"IamDATAbanana")\n'
                      f'print("done1")\n',
            i2_script=f'import lifeblood_connection as lbc\n'
                      f'import time\n'
                      f'time.sleep(5)\n'
                      f'src_inv, data = lbc.message_to_invocation_receive("foobaaaar")\n'
                      f'assert data == b"IamDATAbanana", data\n'
                      f'assert src_inv == 11234, src_inv\n'
                      f'print("done2")\n'
        )

    async def test_worker_invocation_comm_api_worker_no_inv(self):
        for sdelay, rdelay in ((0, 0), (1, 0), (5, 0), (0, 1), (0, 5)):
            print(f'trying send delay {sdelay}, recv delay {rdelay}')
            await self._helper_test_worker_invocation_comm_api(
                i1_script=f'import lifeblood_connection as lbc\n'
                          f'import time\n'
                          f'time.sleep({sdelay})\n'
                          f'try:\n'
                          f'    lbc.message_to_invocation_send(11235, "foobaaaar", b"IamDATAbanana")\n'
                          f'except RuntimeError as e:\n'
                          f'    assert str(e) == "{InvocationMessageResult.ERROR_IID_NOT_RUNNING.value}", str(e)\n'
                          f'    print("all raised as expected")\n'
                          f'print("done1")\n',
                i2_script=f'import time\n'
                          f'time.sleep({rdelay})\n'
                          f'print("do nothing, done2")\n'
            )

    async def test_worker_invocation_comm_api_send_iid_not_found(self):
        await self._helper_test_worker_invocation_comm_api(
            i1_script=f'import lifeblood_connection as lbc\n'
                      f'try:\n'
                      f'    lbc.message_to_invocation_send(11666, "foobaaaar", b"IamDATAbanana")\n'
                      f'except RuntimeError as e:\n'
                      f'    assert str(e) == "{InvocationMessageResult.ERROR_BAD_IID.value}", str(e)\n'
                      f'    print("all raised as expected")\n'
                      f'print("done1")\n',
            i2_script=f'import time\n'
                      f'time.sleep(2)\n'
                      f'print("done2")\n'
        )

    async def test_worker_invocation_comm_api_send_comm_error(self):
        await self._helper_test_worker_invocation_comm_api(
            i1_script=f'import lifeblood_connection as lbc\n'
                      f'try:\n'
                      f'    lbc.message_to_invocation_send(80085, "foobaaaar", b"IamDATAbanana")\n'
                      f'except RuntimeError as e:\n'
                      f'    assert str(e) == "{InvocationMessageResult.ERROR_TRANSFER_ERROR.value}", str(e)\n'
                      f'    print("logged exception from scheduler above is also expected")\n'
                      f'    print("all raised as expected")\n'
                      f'print("done1")\n',
            i2_script=f'import time\n'
                      f'time.sleep(2)\n'
                      f'print("done2")\n'
        )

    async def test_worker_invocation_comm_api_worker_send_timeout(self):
        await self._helper_test_worker_invocation_comm_api(
            i1_script=f'import lifeblood_connection as lbc\n'
                      f'try:\n'
                      f'    lbc.message_to_invocation_send(11235, "foobaaaar", b"IamDATAbanana", addressee_timeout=1)\n'
                      f'except RuntimeError as e:\n'
                      f'    assert str(e) == "{InvocationMessageResult.ERROR_RECEIVER_TIMEOUT.value}", str(e)\n'
                      f'    print("all raised as expected")\n'
                      f'print("done1")\n',
            i2_script=f'import time\n'
                      f'time.sleep(3)\n'
                      f'print("do nothing, done2")\n'
            )

    async def _helper_test_worker_invocation_comm_api(self, *,
                                                      i1_script: str, i2_script: str):
        async def _logic(scheduler, workers: List[Worker], tmp_script_path, done_waiter):
            with mock.patch('lifeblood.scheduler.scheduler.Scheduler.update_task_attributes') as attr_patch, \
                 mock.patch('lifeblood.scheduler.scheduler.Scheduler.get_invocation_state') as get_invoc_patch, \
                 mock.patch('lifeblood.scheduler.scheduler.Scheduler.get_invocation_worker') as get_invoc_worker_patch:
                attr_patch.side_effect = lambda *args, **kwargs: print(f'update attrs with {args}, {kwargs}')
                get_invoc_patch.return_value = InvocationState.IN_PROGRESS
                get_invoc_worker_patch.side_effect = lambda inv_id: \
                    {
                        11234: workers[0].message_processor().listening_address(),
                        11235: workers[1].message_processor().listening_address(),
                        80085: AddressChain('127.2.3.4:567'),  # BAD address
                    }.get(inv_id)

                ij1 = InvocationJob(
                        ['python', '-c',
                            i1_script
                         ],
                        invocation_id=11234,
                    )
                ij1._set_task_id(3456)

                ij2 = InvocationJob(
                    ['python', '-c',
                        i2_script
                     ],
                    invocation_id=11235,
                )
                ij2._set_task_id(3457)

                await workers[0].run_task(
                    ij1,
                    scheduler.server_message_address()
                )
                await workers[1].run_task(
                    ij2,
                    scheduler.server_message_address()
                )

                await asyncio.wait([done_waiter], timeout=10)

        await self._helper_test_worker_invocation_api(
            '',
            _logic,
            worker_count=2
        )

    async def _helper_test_worker_invocation_api(self, runcode: str, logic: Callable, *, worker_count: int = 1, tasks_to_complete=None):
        purge_db()
        sched = Scheduler('test_swc.db', do_broadcasting=False, helpers_minimal_idle_to_ensure=0)
        await sched.start()

        workers = []
        for i in range(worker_count):
            worker = Worker(sched.server_message_address())
            await worker.start()
            workers.append(worker)

        await asyncio.gather(sched.wait_till_starts(),
                             *[worker.wait_till_starts() for worker in workers])

        #
        fd, tmp_script_path = tempfile.mkstemp('.py')
        try:
            with open(tmp_script_path, 'w') as f:
                f.write(runcode)

            done_ev = asyncio.Event()
            tasks_to_complete = tasks_to_complete or worker_count
            side_effect_was_good = True
            with mock.patch('lifeblood.scheduler.scheduler.Scheduler.task_done_reported') as td_patch:
                def _side_effect(task: InvocationJob, stdout: str, stderr: str):
                    nonlocal tasks_to_complete, side_effect_was_good
                    tasks_to_complete -= 1
                    print(f'finished {task.task_id()} out: {stdout}')
                    print(f'finished {task.task_id()} err: {stderr}')
                    side_effect_was_good = side_effect_was_good and 0 == task.exit_code()
                    if tasks_to_complete <= 0:
                        done_ev.set()

                td_patch.side_effect = _side_effect
                done_waiter = asyncio.create_task(done_ev.wait())
                await logic(sched, workers, tmp_script_path, done_waiter)
                self.assertTrue(side_effect_was_good)
        finally:
            os.close(fd)
            os.unlink(tmp_script_path)

            for worker in workers:
                worker.stop()
                await worker.wait_till_stops()
            sched.stop()
            await sched.wait_till_stops()

    async def test_task_get_order(self):
        purge_db()
        sched = Scheduler('test_swc.db', do_broadcasting=False, helpers_minimal_idle_to_ensure=0)
        await sched.start()

        worker = Worker(sched.server_message_address(), scheduler_ping_interval=999)  # huge ping interval to prevent pinger from interfering with the test
        await worker.start()
        self.assertTrue(sched.is_started())
        self.assertTrue(worker.is_started())

        # theses are actually noops
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
        node = await sched._get_node_object_by_id(nid)
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
        state_enter_time = -1
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
                    state_enter_time = time.time()
                else:
                    self.assertFalse(wrun)
                    self.assertEqual(WorkerState.IDLE, sstate)
                    self.assertTrue(wlocked)
            if state == 6:
                self.assertFalse(wrun)
                self.assertEqual(WorkerState.IDLE, sstate)
                self.assertFalse(wlocked)
                if time.time() - state_enter_time > 11:  # just wait some random time to ensure nothing changes
                    break
            await asyncio.sleep(0.0)

        sched.stop()
        worker.stop()

        await asyncio.gather(sched.wait_till_stops(),
                             worker.wait_till_stops())
