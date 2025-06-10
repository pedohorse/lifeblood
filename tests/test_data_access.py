import os.path

from lifeblood.scheduler.data_access import DataAccess, TaskSpawnData
from lifeblood.enums import TaskState
from lifeblood_testing_common.scheduler_config_provider_default_override import SchedulerConfigProviderOverrides
from unittest import IsolatedAsyncioTestCase


class TestDataAccess(IsolatedAsyncioTestCase):
    db_location = 'test_dataaccess.db'

    def setUp(self):
        if os.path.exists(self.db_location):
            os.unlink(self.db_location)

    async def test_unique_task_group_assignment(self):
        config = SchedulerConfigProviderOverrides(
            self.db_location
        )
        data_access = DataAccess(config_provider=config)

        group_id = 'test group'
        await data_access.create_task_group(group_id)

        node_id = await data_access.create_node('test_type', 'test name')
        task_data = TaskSpawnData('foo task', None, {}, TaskState.READY, node_id, 'main', None)
        task_id = await data_access.create_task(task_data)

        test_tasks = await data_access.get_group_tasks(group_id)
        self.assertEqual(0, len(test_tasks))

        await data_access.assign_task_to_group(task_id, group_id)
        test_tasks = await data_access.get_group_tasks(group_id)
        self.assertEqual(1, len(test_tasks))

        await data_access.assign_task_to_group(task_id, group_id)
        test_tasks = await data_access.get_group_tasks(group_id)
        self.assertEqual(1, len(test_tasks))
