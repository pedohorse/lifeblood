import asyncio
from lifeblood_testing_common.integration_common import IsolatedAsyncioTestCaseWithDb
from lifeblood.scheduler.scheduler import Scheduler
from lifeblood.taskspawn import NewTask


async def chain(*coros):
    for coro in coros:
        await coro


class TestSpawnTasksRace(IsolatedAsyncioTestCaseWithDb):
    async def test_race(self):
        """
        this tests for the case when spawn is called from an existing transaction and a new one,
        which in reality can happen for example in _awaiter (where transaction is passed to spawn),
        and spawn called by message server for example.
        """
        sched = Scheduler(self.db_file, do_broadcasting=False, node_data_provider=None, node_serializers=[None])
        async with sched.data_access.data_connection() as con:
            await con.execute('BEGIN IMMEDIATE')
            task1 = asyncio.create_task(sched.spawn_tasks([NewTask('foo1', 1, None, {})]))
            await asyncio.sleep(1)  # we need task1 to get to deadlock position
            self.assertFalse(task1.done())
            # now finish the deadlock
            task2 = asyncio.create_task(
                chain(
                    sched.spawn_tasks([NewTask('foo2', 1, None, {})], con=con),
                    con.commit()
                )
            )
            done, pend = await asyncio.wait([task1, task2], timeout=35, return_when=asyncio.ALL_COMPLETED)  # also 30 sec is db locked timeout
            self.assertEqual(0, len(pend), 'tasks deadlocked')
            for task in done:
                self.assertIsNone(task.exception())
