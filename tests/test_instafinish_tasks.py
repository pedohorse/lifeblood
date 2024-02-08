"""
we mainly test for the race condition in scheduler
when task done/cancel is reported BEFORE submission finishes.
For that we need to load scheduler with a bunch of tasks to process
so that awaiter lock is contested for, and have close to zero time invocations
"""
from unittest import mock
from lifeblood_testing_common.integration_common import FullIntegrationTestCase

from typing import Iterable


class TestInstantTaskFinishDone(FullIntegrationTestCase):
    __test__ = True


    @classmethod
    def _initial_db_file(cls) -> str:
        return 'data/test_instadoneinv.db'

    async def _create_test_tasks(self) -> Iterable[int]:
        self.scheduler.task_done_reported = mock.Mock(spec=self.scheduler.task_done_reported, wraps=self.scheduler.task_done_reported)
        self.scheduler.task_cancel_reported = mock.Mock(spec=self.scheduler.task_cancel_reported, wraps=self.scheduler.task_cancel_reported)
        tasks = [
            *await self._create_task(node_name='TEST IN'),
        ]
        return tasks

    async def _additional_checks_on_finish(self):
        stat = await self.scheduler.data_access.invocations_statistics()
        self.assertEqual(40, self.scheduler.task_done_reported.call_count)
        cancels = self.scheduler.task_cancel_reported.call_count
        self.assertEqual(40 + cancels, stat.total)
        self.assertEqual(40, stat.finished_good)
        self.assertEqual(cancels, stat.finished_bad)

    def _timeout(self) -> float:
        return 120.0

    def _minimal_idle_to_ensure(self) -> int:
        return 2

    def _minimal_total_to_ensure(self) -> int:
        return 8
