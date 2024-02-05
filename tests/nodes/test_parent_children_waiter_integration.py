from lifeblood_testing_common.integration_common import FullIntegrationTestCase

from typing import Iterable


class ParentChildrenIntegrationTest(FullIntegrationTestCase):
    __test__ = True

    @classmethod
    def _initial_db_file(cls) -> str:
        return 'data/test_pwint.db'

    async def _create_test_tasks(self) -> Iterable[int]:
        tasks = [
            *await self._create_task(node_name='TEST IN'),
            *await self._create_task(node_name='TEST IN RECURSIVE'),
            *await self._create_task(node_name='TEST IN MULTI'),
        ]
        return tasks

    def _expected_task_attributes(self):
        return {
            0: {'foo': list(range(11, 11+10*2, 2))},
            1: {'foo': list(range(123, 123+20))},
            2: {'fee': list(reversed(range(531, 531+20*2, 2)))},
        }

    async def _additional_checks_on_finish(self):
        stat = await self.scheduler.data_access.invocations_statistics()
        self.assertEqual(60, stat.total)
        self.assertEqual(60, stat.finished_good)
        self.assertEqual(0, stat.finished_bad)

    def _minimal_idle_to_ensure(self) -> int:
        return 2

    def _minimal_total_to_ensure(self) -> int:
        return 4

    def _timeout(self) -> float:
        return 60.0
