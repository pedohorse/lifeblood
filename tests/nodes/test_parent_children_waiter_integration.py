from .common import FullIntegrationTestCase
from lifeblood.taskspawn import TaskSpawn
from lifeblood.enums import SpawnStatus

from typing import Iterable


class ParentChildrenIntegrationTest(FullIntegrationTestCase):
    @classmethod
    def _initial_db_file(cls) -> str:
        return 'nodes/data/test_pwint.db'

    async def _create_test_tasks(self) -> Iterable[int]:
        tasks = [
            *await self._create_node(node_name='TEST IN'),
            *await self._create_node(node_name='TEST IN RECURSIVE'),
            *await self._create_node(node_name='TEST IN MULTI'),
        ]
        return tasks

    def _expected_task_attributes(self):
        return {
            0: {'foo': list(range(11, 11+10*2, 2))},
            1: {'foo': list(range(123, 123+20))},
            2: {'fee': list(reversed(range(531, 531+20*2, 2)))},
        }

    def _timeout(self) -> float:
        return 60.0
