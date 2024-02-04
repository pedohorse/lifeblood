from lifeblood_testing_common.integration_common import FullIntegrationTestCase

from typing import Iterable


class SplitWaiterIntegrationTest(FullIntegrationTestCase):
    __test__ = True

    @classmethod
    def _initial_db_file(cls) -> str:
        return 'data/test_splitint.db'

    async def _create_test_tasks(self) -> Iterable[int]:
        tasks = [
            *await self._create_task(node_name='TEST IN', attributes={'froimes': list(range(11, 211))}),
        ]
        return tasks

    def _expected_task_attributes(self):
        return {
            0: {'farmes': list(range(22, 422, 2))},
        }

    async def _additional_checks_on_finish(self):
        stat = await self.scheduler.data_access.invocations_statistics()
        self.assertEqual(50, stat.total)
        self.assertEqual(50, stat.finished_good)
        self.assertEqual(0, stat.finished_bad)

    def _minimal_idle_to_ensure(self) -> int:
        return 2

    def _minimal_total_to_ensure(self) -> int:
        return 4

    def _timeout(self) -> float:
        return 60.0
