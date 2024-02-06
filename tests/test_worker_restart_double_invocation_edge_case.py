import aiosqlite
import asyncio
import os
from contextlib import contextmanager
from lifeblood_testing_common.integration_common import IsolatedAsyncioTestCaseWithDb
from unittest import mock
from lifeblood.enums import TaskState, WorkerState, WorkerPingState, TaskScheduleStatus, InvocationState
from lifeblood.invocationjob import InvocationJob
from lifeblood.scheduler.data_access import DataAccess, TaskSpawnData
from lifeblood.scheduler.task_processor import TaskProcessor
from lifeblood.scheduler.scheduler import Scheduler

from typing import List, Optional

foowait_call_count = 0


async def foowait(*args, **kwargs):
    global foowait_call_count
    print("mock_waiting")
    await asyncio.sleep(1.0)
    print("mock_waiting done")
    foowait_call_count += 1
    return TaskScheduleStatus.SUCCESS


@contextmanager
def get_worker_control_client_mock(*args, **kwargs) -> "WorkerControlClient":
    m = mock.MagicMock()
    m.give_task = foowait
    yield m


async def chain(*coros):
    for coro in coros:
        await coro


class WorkerRestartDoubleInvocationCaseTest(IsolatedAsyncioTestCaseWithDb):
    """
    These tests are imitating race conditions in submitting process
    """
    def setUp(self):
        super().setUp()
        global foowait_call_count
        foowait_call_count = 0

    async def test_multi_invoc2(self):
        """
        This test imitates the following:
        usual submission goes like this (every step is a transaction):
        -     worker up  (worker: idle)
        ...
        - submission scheduled  (worker: invoking)
        - submission init       (worker: invoking)
        -     worker communicate    (worker: invoking)
        - submission finalized  (worker: busy)
        ...
        -     worker stop   (worker: off)

        But race conditions are possible:
        -     worker up (worker: idle)
        ...
        - submission scheduled  (worker: invoking)
        -     worker down       (worker: off)
        -     worker up         (worker: on)
        - submission2 scheduled (worker: invoking)
        - submission init       (worker: invoking)   (expected behaviour is that one of submissions will terminate itself)
        - submission2 init      (worker: invoking)
        ... ONLY one submission must be able to reach this point ...
        -     worker communicate(worker: invoking)
        - submission2 finalize  (worker: busy)
        ...
        -     worker stop       (worker: off)

        case above may scale beyond just 2 submissions
        """
        await self._helper_test_multi_invoc(2)

    async def test_multi_invoc3(self):
        await self._helper_test_multi_invoc(3)

    async def test_multi_invoc4(self):
        await self._helper_test_multi_invoc(4)

    async def test_multi_invoc5(self):
        await self._helper_test_multi_invoc(5)

    async def test_multi_invoc6(self):
        await self._helper_test_multi_invoc(6)

    async def test_multi_invoc7(self):
        await self._helper_test_multi_invoc(7)

    async def test_multi_invoc_trivial(self):
        await self._helper_test_multi_invoc(1)

    # empty ones

    async def test_multi_invoc_trivial_empty1(self):
        await self._helper_test_multi_invoc(1, 1)

    async def test_multi_invoc2_empty1(self):
        await self._helper_test_multi_invoc(2, 1)

    async def test_multi_invoc2_empty2(self):
        await self._helper_test_multi_invoc(2, 2)

    async def test_multi_invoc3_empty1(self):
        await self._helper_test_multi_invoc(3, 1)

    async def test_multi_invoc3_empty2(self):
        await self._helper_test_multi_invoc(3, 2)

    async def test_multi_invoc3_empty3(self):
        await self._helper_test_multi_invoc(3, 3)

    async def test_multi_invoc2_empty1_delays1(self):
        await self._helper_test_multi_invoc(2, 1, delays=[0.25, 0])

    async def test_multi_invoc2_empty1_delays2(self):
        await self._helper_test_multi_invoc(2, 1, delays=[0, 0.25])

    async def test_multi_invoc3_empty1_delays1(self):
        await self._helper_test_multi_invoc(3, 1, delays=[0.25, 0, 0])

    async def test_multi_invoc3_empty1_delays2(self):
        await self._helper_test_multi_invoc(3, 1, delays=[0, 0.25, 0])

    async def _helper_test_multi_invoc(self, racing_tasks_count: int, num_empty_invocs: int = 0, delays: Optional[List[int]] = None):
        sched = Scheduler(self.db_file, do_broadcasting=False, node_data_provider=None, node_serializers=[None])
        data_access = DataAccess(self.db_file, 60)
        m = mock.MagicMock()
        m.data_access = data_access
        task_ids = []
        for i in range(racing_tasks_count):
            task_ids.append(await sched.data_access.create_task(TaskSpawnData(f'test{1+i}', None, {}, TaskState.INVOKING, 1, 'main', None)))

        fake_task_rows = []
        for i, task_id in enumerate(task_ids):
            fake_task_rows.append({
                'id': task_id,
                'node_id': 1,
                'work_data': await InvocationJob([] if i < num_empty_invocs else ['/bin/true']).serialize_async(),
                'state': TaskState.INVOKING.value
            })
        fake_worker_row = {
            'id': 1,
            'last_address': '555.555.555.555:5555555',
            'hwid': 12345,
        }
        async with sched.data_access.data_connection() as con:
            await con.execute('INSERT INTO workers ("id", "state", "ping_state", hwid, last_address) VALUES (?, ?, ?, ?, ?)',
                              (1, WorkerState.INVOKING.value, WorkerPingState.WORKING.value, fake_worker_row['hwid'], fake_worker_row['last_address']))
            await con.commit()

        with mock.patch('lifeblood.scheduler.Scheduler._update_worker_resouce_usage'), \
                mock.patch('lifeblood.scheduler.Scheduler.server_message_address'), \
                mock.patch('lifeblood.worker_messsage_processor.WorkerControlClient.get_worker_control_client') as get_client_mock:
            get_client_mock.side_effect = get_worker_control_client_mock
            if delays:
                await asyncio.gather(*[
                    chain(asyncio.sleep(delay), sched.task_processor._submitter(fake_task_row, fake_worker_row)) for fake_task_row, delay in zip(fake_task_rows, delays)
                ])
            else:
                await asyncio.gather(
                    *[sched.task_processor._submitter(fake_task_row, fake_worker_row) for fake_task_row in fake_task_rows]
                )

        async with sched.data_access.data_connection() as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT "id", state FROM invocations') as cur:
                invoc_rows = await cur.fetchall()
            async with con.execute('SELECT "id", state FROM workers') as cur:
                work_rows = await cur.fetchall()
            async with con.execute('SELECT "id", state FROM tasks') as cur:
                task_rows = await cur.fetchall()

        print('testing invoc count')
        invoc_count = len(invoc_rows)
        self.assertGreaterEqual(1, invoc_count)
        self.assertLessEqual(max(0, 1 - num_empty_invocs), invoc_count)
        if invoc_count > 0:
            print('testing invoc states')
            self.assertEqual(InvocationState.IN_PROGRESS.value, invoc_rows[0]['state'])

        print('testing tasks count')
        self.assertEqual(racing_tasks_count, len(task_rows))

        in_progress_tasks_count = sum(int(r['state'] == TaskState.IN_PROGRESS.value) for r in task_rows)
        skipped_tasks_count = sum(int(r['state'] == TaskState.POST_WAITING.value) for r in task_rows)
        ready_tasks_count = sum(int(r['state'] == TaskState.READY.value) for r in task_rows)
        print('testing tasks states')
        # 2 checks below should turn into single Equal if num_empty_invocs == 0
        self.assertGreaterEqual(1, in_progress_tasks_count, "in progress high bound not met")
        self.assertLessEqual(max(0, 1 - num_empty_invocs), in_progress_tasks_count, "in progress low bound not met")

        # 2 checks below should turn into single Equal if num_empty_invocs == 0
        self.assertGreaterEqual(max(0, racing_tasks_count - in_progress_tasks_count), ready_tasks_count, "ready high bound not met")
        self.assertLessEqual(max(0, racing_tasks_count - in_progress_tasks_count - num_empty_invocs), ready_tasks_count, "ready low bound not met")

        # 2 checks below should turn into single Equal if num_empty_invocs == 0
        self.assertGreaterEqual(num_empty_invocs, skipped_tasks_count, "skipped high bound not met")
        self.assertLessEqual(0, skipped_tasks_count, "skipped low bound not met")

        self.assertEqual(racing_tasks_count, in_progress_tasks_count + skipped_tasks_count + ready_tasks_count, "total not met. tasks in unexpected states?")

        print('testing worker count')
        self.assertEqual(1, len(work_rows))
        print('testing worker states')
        self.assertEqual(WorkerState.BUSY.value if in_progress_tasks_count > 0 else WorkerState.IDLE.value, work_rows[0]['state'])

        print('testing call count')
        self.assertEqual(1 if in_progress_tasks_count > 0 else 0, foowait_call_count)
