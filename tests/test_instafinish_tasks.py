"""
we mainly test for the race condition in scheduler
when task done/cancel is reported BEFORE submission finishes.
For that we need to load scheduler with a bunch of tasks to process
so that awaiter lock is contested for, and have close to zero time invocations
"""
from lifeblood_testing_common.integration_common import FullIntegrationTestCase

from typing import Iterable


class TestInstantTaskFinishDone(FullIntegrationTestCase):
    __test__ = True

    @classmethod
    def _initial_db_file(cls) -> str:
        return 'data/test_instadoneinv.db'

    async def _create_test_tasks(self) -> Iterable[int]:
        tasks = [
            *await self._create_task(node_name='TEST IN'),
        ]
        return tasks

    async def _additional_checks_on_finish(self):
        stat = await self.scheduler.data_access.invocations_statistics()
        self.assertEqual(40, stat.total)
        self.assertEqual(40, stat.finished_good)
        self.assertEqual(0, stat.finished_bad)

    def _timeout(self) -> float:
        return 90.0

    def _minimal_idle_to_ensure(self) -> int:
        return 2

    def _minimal_total_to_ensure(self) -> int:
        return 8
