import os
from unittest import IsolatedAsyncioTestCase
from lifeblood.scheduler.data_access import DataAccess, TaskSpawnData
from lifeblood.enums import TaskState


class TestTaskBlocking(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_file = f'test_{cls.__name__}.db'

    def setUp(self):
        if os.path.exists(self.db_file):
            os.unlink(self.db_file)

    async def test_simple(self):
        data_access = DataAccess(self.db_file, 60)
        node_id = await data_access.create_node('null', 'test node')
        task_id = await data_access.create_task(TaskSpawnData('test task', None, {}, TaskState.WAITING, node_id, 'main', None))

        # initial
        self.assertFalse(await data_access.is_task_blocked(task_id))

        # block
        await data_access.hint_task_needs_blocking(task_id)
        self.assertTrue(await data_access.is_task_blocked(task_id))
        await data_access.hint_task_needs_blocking(task_id)
        self.assertTrue(await data_access.is_task_blocked(task_id))
        await data_access.hint_task_needs_blocking(task_id)
        self.assertTrue(await data_access.is_task_blocked(task_id))

        # unblock
        await data_access.hint_task_needs_unblocking(task_id)
        self.assertFalse(await data_access.is_task_blocked(task_id))
        await data_access.hint_task_needs_unblocking(task_id)
        self.assertFalse(await data_access.is_task_blocked(task_id))
        await data_access.hint_task_needs_unblocking(task_id)
        self.assertFalse(await data_access.is_task_blocked(task_id))
        await data_access.hint_task_needs_unblocking(task_id)
        self.assertFalse(await data_access.is_task_blocked(task_id))

        # block considering hint count
        await data_access.hint_task_needs_blocking(task_id)
        self.assertFalse(await data_access.is_task_blocked(task_id))
        await data_access.hint_task_needs_blocking(task_id)
        self.assertFalse(await data_access.is_task_blocked(task_id))
        await data_access.hint_task_needs_blocking(task_id)
        self.assertFalse(await data_access.is_task_blocked(task_id))
        await data_access.hint_task_needs_blocking(task_id)
        self.assertTrue(await data_access.is_task_blocked(task_id))
        await data_access.hint_task_needs_blocking(task_id)
        self.assertTrue(await data_access.is_task_blocked(task_id))

        # unblock again
        await data_access.hint_task_needs_unblocking(task_id)
        self.assertFalse(await data_access.is_task_blocked(task_id))

        # final block
        await data_access.hint_task_needs_blocking(task_id)
        self.assertTrue(await data_access.is_task_blocked(task_id))

    async def test_reset(self):
        data_access = DataAccess(self.db_file, 60)
        node_id = await data_access.create_node('null', 'test node')
        task_id = await data_access.create_task(TaskSpawnData('test task', None, {}, TaskState.WAITING, node_id, 'main', None))

        # initial
        self.assertFalse(await data_access.is_task_blocked(task_id))

        # block
        await data_access.hint_task_needs_blocking(task_id)
        self.assertTrue(await data_access.is_task_blocked(task_id))

        # reset1
        await data_access.reset_task_blocking(task_id)
        self.assertFalse(await data_access.is_task_blocked(task_id))

        await data_access.hint_task_needs_blocking(task_id)
        self.assertTrue(await data_access.is_task_blocked(task_id))

        # unblock
        await data_access.hint_task_needs_unblocking(task_id)
        self.assertFalse(await data_access.is_task_blocked(task_id))
        await data_access.hint_task_needs_unblocking(task_id)
        self.assertFalse(await data_access.is_task_blocked(task_id))
        await data_access.hint_task_needs_unblocking(task_id)
        self.assertFalse(await data_access.is_task_blocked(task_id))

        # reset2
        await data_access.reset_task_blocking(task_id)
        self.assertFalse(await data_access.is_task_blocked(task_id))

        await data_access.hint_task_needs_blocking(task_id)
        self.assertTrue(await data_access.is_task_blocked(task_id))
