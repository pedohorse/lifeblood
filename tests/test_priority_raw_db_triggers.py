from lifeblood.scheduler.data_access import DataAccess, TaskSpawnData
from lifeblood.enums import TaskState
from lifeblood_testing_common.scheduler_config_provider_default_override import SchedulerConfigProviderOverrides
from unittest import IsolatedAsyncioTestCase
import os
import aiosqlite


class TestPriorityTriggers(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_file = f'test_{cls.__name__}.db'

    def setUp(self):
        if os.path.exists(self.db_file):
            os.unlink(self.db_file)

    async def test_priority_triggers_multiple_transactions(self):
        # TODO: more tests with variations
        #  - add to 2 groups
        await self._priority_triggers_helper(commit_after_each=True)

    async def test_priority_triggers_single_transaction(self):
        await self._priority_triggers_helper(commit_after_each=False)

    async def test_priority_triggers_multiple_transactions_with_group_delete(self):
        await self._priority_triggers_helper(commit_after_each=True, delete_whole_group_instead_of_unassigning=True)

    async def test_priority_triggers_single_transaction_with_group_delete(self):
        await self._priority_triggers_helper(commit_after_each=False, delete_whole_group_instead_of_unassigning=True)

    async def test_priority_triggers_multiple_transactions_many_groups(self):
        await self._priority_triggers_helper(commit_after_each=True, many_groups=True)

    async def test_priority_triggers_single_transaction_many_groups(self):
        await self._priority_triggers_helper(commit_after_each=False, many_groups=True)

    async def test_priority_triggers_multiple_transactions_with_group_delete_many_groups(self):
        await self._priority_triggers_helper(commit_after_each=True, delete_whole_group_instead_of_unassigning=True, many_groups=True)

    async def test_priority_triggers_single_transaction_with_group_delete_many_groups(self):
        await self._priority_triggers_helper(commit_after_each=False, delete_whole_group_instead_of_unassigning=True, many_groups=True)

    async def _priority_triggers_helper(self, commit_after_each: bool, delete_whole_group_instead_of_unassigning: bool = False, many_groups: bool = False):
        data_access = DataAccess(config_provider=SchedulerConfigProviderOverrides(self.db_file))

        grp_prio = 56.7
        inv_prio = 0.0
        task_group_name = 'test_group'
        node_id = await data_access.create_node('null', 'test')
        task_id = await data_access.create_task(TaskSpawnData('test_task', None, {}, TaskState.DONE, node_id, 'main', None))
        await data_access.create_task_group(task_group_name, 'testuser', grp_prio)
        if many_groups:
            for i in range(2):
                await data_access.create_task_group(task_group_name + str(i), 'testuser', grp_prio - i - 1)
            for i in range(2):
                await data_access.create_task_group(f'unrelated group {i}', 'testuser', grp_prio*(2 + i))
        await data_access.assign_task_to_group(task_id, task_group_name)
        if many_groups:
            for i in range(2):
                await data_access.assign_task_to_group(task_id, task_group_name + str(i))

        # we init everything with DataAccess, then do raw sql tests

        async with data_access.data_connection() as con:
            async with con.execute('SELECT priority FROM tasks WHERE "id" == ?', (task_id,)) as cur:
                prio = (await cur.fetchone())[0]
            self.assertEqual(grp_prio + inv_prio, prio)

            inv_prio = 23.4
            await con.execute('UPDATE "tasks" SET priority_invocation_adjust = ? WHERE "id" == ?',
                              (inv_prio, task_id))
            if commit_after_each:
                await con.commit()

            async with con.execute('SELECT priority FROM tasks WHERE "id" == ?', (task_id,)) as cur:
                prio = (await cur.fetchone())[0]
            self.assertEqual(grp_prio + inv_prio, prio)

            grp_prio = 100.0
            await data_access.set_task_group_priority(task_group_name, grp_prio, con=con)
            if many_groups:
                for i in range(2):
                    await data_access.set_task_group_priority(task_group_name + str(i), grp_prio - i - 1, con=con)
            if commit_after_each:
                await con.commit()

            async with con.execute('SELECT priority FROM tasks WHERE "id" == ?', (task_id,)) as cur:
                prio = (await cur.fetchone())[0]
            self.assertEqual(grp_prio + inv_prio, prio)

            if delete_whole_group_instead_of_unassigning:
                if con.in_transaction:  # this workaround is needed until #126 is solved
                    await con.commit()
                await data_access.delete_task_group(task_group_name, con=con)
                if many_groups:
                    for i in range(2):
                        async with con.execute('SELECT priority FROM tasks WHERE "id" == ?', (task_id,)) as cur:
                            prio = (await cur.fetchone())[0]
                            self.assertEqual(grp_prio - i - 1 + inv_prio, prio)
                        await data_access.delete_task_group(task_group_name + str(i), con=con)
            else:
                await data_access.unassign_task_from_group(task_id, task_group_name, con=con)
                if many_groups:
                    for i in range(2):
                        async with con.execute('SELECT priority FROM tasks WHERE "id" == ?', (task_id,)) as cur:
                            prio = (await cur.fetchone())[0]
                            self.assertEqual(grp_prio - i - 1 + inv_prio, prio)
                        await data_access.unassign_task_from_group(task_id, task_group_name + str(i), con=con)
            if commit_after_each:
                await con.commit()
            async with con.execute('SELECT priority FROM tasks WHERE "id" == ?', (task_id,)) as cur:
                prio = (await cur.fetchone())[0]
            self.assertEqual(inv_prio, prio)
