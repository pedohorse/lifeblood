import random

import aiosqlite
import inspect
from unittest import IsolatedAsyncioTestCase
from unittest import mock
import shutil
from pathlib import Path
from lifeblood.scheduler.pinger import Pinger
from lifeblood_testing_common.common import create_default_scheduler
from itertools import chain
from typing import Iterable, Union


class SchedulerTests(IsolatedAsyncioTestCase):
    @staticmethod
    def dbpath() -> str:
        return 'test_taskgroup_del.db'

    @classmethod
    def purge_db(cls, recreate=True):
        testdbpath = Path(cls.dbpath())
        if testdbpath.exists():
            testdbpath.unlink()
        if recreate:
            shutil.copy2(Path(inspect.getmodule(cls).__file__).parent / 'data' / 'test_taskgroup_del.db', testdbpath)

    @classmethod
    def setUp(cls) -> None:
        cls.purge_db()
        print('settingup done')

    @classmethod
    def tearDown(cls) -> None:
        cls.purge_db(recreate=False)
        print('tearingdown done')

    @classmethod
    async def _db_integrity_check(cls, sched):
        async with sched.data_access.data_connection() as con:
            async with con.execute('PRAGMA integrity_check') as cur:
                if (errors := await cur.fetchall()) and len(errors) > 0 and errors[0][0] != 'ok':
                    raise RuntimeError(f'database upgrade failed with errors: {[str(x[0]) for x in errors]}')

    async def _check_tasks_exist(self, sched, tasks_iterable):
        async with sched.data_access.data_connection() as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT "id" FROM tasks') as cur:
                tasks = {x['id'] for x in await cur.fetchall()}
            self.assertSetEqual({i for i in tasks_iterable}, tasks)

    async def _task_group_tasks(self, sched, task_group_name: str, expect_tasks: Union[bool, Iterable[int]]):
        async with sched.data_access.data_connection() as con:
            con.row_factory = aiosqlite.Row
            async with con.execute(
                    'SELECT task_id FROM task_groups WHERE "group" == ?',
                    (task_group_name,)
            ) as cur:
                actual_tasks = set(x['task_id'] for x in await cur.fetchall())
            if isinstance(expect_tasks, bool):
                self.assertTrue((len(actual_tasks) > 0) == expect_tasks, actual_tasks)
            else:
                self.assertSetEqual(set(expect_tasks), actual_tasks)

    async def test_delete_task_groups_normal(self):

        with mock.patch('lifeblood.scheduler.scheduler_core.Pinger') as ppatch:
            ppatch.return_value = mock.AsyncMock(Pinger)

            sched = create_default_scheduler(self.dbpath(), do_broadcasting=False, helpers_minimal_idle_to_ensure=0)
            await sched.start()

            # pre sanity check
            async with sched.data_access.data_connection() as con:
                async with con.execute('SELECT "id" FROM tasks') as cur:
                    self.assertEqual(42, len(await cur.fetchall()))

            # delete group
            await sched.delete_task_group('test_split1#1')
            await self._db_integrity_check(sched)
            await self._check_tasks_exist(sched, range(11, 43))
            await self._task_group_tasks(sched, 'test_split1#1', False)
            await self._task_group_tasks(sched, 'test_par1#11', range(11, 17))
            await self._task_group_tasks(sched, 'test_par2#17', range(17, 43))

            # delete group
            await sched.delete_task_group('test_par2#17')
            await self._db_integrity_check(sched)
            await self._check_tasks_exist(sched, range(11, 17))
            await self._task_group_tasks(sched, 'test_split1#1', False)
            await self._task_group_tasks(sched, 'test_par1#11', range(11, 17))
            await self._task_group_tasks(sched, 'test_par2#17', False)

            # delete group
            await sched.delete_task_group('test_par1#11')
            await self._db_integrity_check(sched)
            await self._check_tasks_exist(sched, ())
            await self._task_group_tasks(sched, 'test_split1#1', False)
            await self._task_group_tasks(sched, 'test_par1#11', False)
            await self._task_group_tasks(sched, 'test_par2#17', False)

            sched.stop()
            await sched.wait_till_stops()

    async def test_delete_non_existing_be_noop(self):
        with mock.patch('lifeblood.scheduler.scheduler_core.Pinger') as ppatch:
            ppatch.return_value = mock.AsyncMock(Pinger)

            sched = create_default_scheduler(self.dbpath(), do_broadcasting=False, helpers_minimal_idle_to_ensure=0)
            await sched.start()

            await sched.delete_task_group('nonononono')
            await self._check_tasks_exist(sched, range(1, 43))
            await self._task_group_tasks(sched, 'test_split1#1', range(1, 11))
            await self._task_group_tasks(sched, 'test_par1#11', range(11, 17))
            await self._task_group_tasks(sched, 'test_par2#17', range(17, 43))

            sched.stop()
            await sched.wait_till_stops()

    async def test_delete_empty_group(self):
        with mock.patch('lifeblood.scheduler.scheduler_core.Pinger') as ppatch:
            ppatch.return_value = mock.AsyncMock(Pinger)

            await self._helper_test_no_delete_partial_stuff([], [], range(1, 43))

    async def test_no_delete_partial_split(self):
        with mock.patch('lifeblood.scheduler.scheduler_core.Pinger') as ppatch:
            ppatch.return_value = mock.AsyncMock(Pinger)

            tasks_to_add = list(range(1, 11))
            random.Random(12345).shuffle(tasks_to_add)

            for i in range(0, len(tasks_to_add)-1):
                self.purge_db()
                await self._helper_test_no_delete_partial_stuff(range(11, 17), tasks_to_add[:i], chain(range(1, 11), range(17, 43)))

            self.purge_db()
            # at this point we add 2 full groups, so both should be deleted
            await self._helper_test_no_delete_partial_stuff(range(11, 17), tasks_to_add, range(17, 43))

    async def test_no_delete_partial_parent1(self):
        with mock.patch('lifeblood.scheduler.scheduler_core.Pinger') as ppatch:
            ppatch.return_value = mock.AsyncMock(Pinger)

            tasks_to_add = list(range(11, 17))
            random.Random(23456).shuffle(tasks_to_add)

            for i in range(0, len(tasks_to_add)-1):
                self.purge_db()
                await self._helper_test_no_delete_partial_stuff(range(1, 11), tasks_to_add[:i], range(11, 43))

            self.purge_db()
            # at this point we add 2 full groups, so both should be deleted
            await self._helper_test_no_delete_partial_stuff(range(1, 11), tasks_to_add, range(17, 43))

    async def test_no_delete_partial_parent2(self):
        with mock.patch('lifeblood.scheduler.scheduler_core.Pinger') as ppatch:
            ppatch.return_value = mock.AsyncMock(Pinger)

            tasks_to_add = list(range(17, 43))
            random.Random(34567).shuffle(tasks_to_add)

            tasks_to_add = [
                42, 41, 40, 39, 18,  # group of 2nd gen
                21, 35, 36, 37, 38,  # group of 2nd gen
                17, 18, 19, 20, 21, 22,  # group of 1st gen
            ]

            for i in range(0, len(tasks_to_add)-1):
                self.purge_db()
                await self._helper_test_no_delete_partial_stuff(range(1, 17), tasks_to_add[:i], range(17, 43))

    async def _helper_test_no_delete_partial_stuff(
            self,
            tasks_to_pre_add: Iterable[int],
            tasks_to_add_to_group: Iterable[int],
            tasks_to_exist_after_delete: Iterable[int]
    ):
        tasks_to_pre_add = list(tasks_to_pre_add)
        tasks_to_add_to_group = list(tasks_to_add_to_group)
        tasks_to_exist_after_delete = list(tasks_to_exist_after_delete)

        sched = create_default_scheduler(self.dbpath(), do_broadcasting=False, helpers_minimal_idle_to_ensure=0)
        await sched.start()

        # pre sanity check
        async with sched.data_access.data_connection() as con:
            async with con.execute('SELECT "id" FROM tasks') as cur:
                self.assertEqual(42, len(await cur.fetchall()))

        group_name = 'testgroup1-2'
        async with sched.data_access.data_connection() as con:
            await con.execute('PRAGMA FOREIGN_KEYS = on')

            # remove existing groups
            await sched.data_access.delete_task_group('test_split1#1', con=con)
            await sched.data_access.delete_task_group('test_par1#11', con=con)
            await sched.data_access.delete_task_group('test_par2#17', con=con)

            await sched.data_access.create_task_group(group_name, con=con)
            # add random tasks in group
            all_tasks_added = set()
            all_tasks_added.update(tasks_to_pre_add)
            all_tasks_added.update(tasks_to_add_to_group)
            for tid in tasks_to_pre_add:
                await sched.data_access.assign_task_to_group(tid, group_name, con=con)
            for tid in tasks_to_add_to_group:
                await sched.data_access.assign_task_to_group(tid, group_name, con=con)
            await con.commit()

        await sched.delete_task_group(group_name)

        await self._check_tasks_exist(sched, tasks_to_exist_after_delete)
        await self._task_group_tasks(sched, group_name, bool(all_tasks_added.intersection(tasks_to_exist_after_delete)))

        await self._db_integrity_check(sched)

        sched.stop()
        await sched.wait_till_stops()
