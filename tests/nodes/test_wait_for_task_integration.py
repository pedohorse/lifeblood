from lifeblood_testing_common.integration_common import FullIntegrationTestCase

from typing import Iterable


class WaitForTaskIntegrationTest(FullIntegrationTestCase):
    """
    TODO: this does test that wait_for_task unblocked what is needed,
     this does NOT test that wait_for_task blocked anything at all,
     as there is no attrib transfer feature for now, so no way to make sure
     (and actually even that does not guarantee blocking happened)
    """
    __test__ = True

    @classmethod
    def _initial_db_file(cls) -> str:
        return 'data/test_waitforint.db'

    async def _create_test_tasks(self) -> Iterable[int]:
        tasks = [
            *await self._create_task(node_name='TEST IN'),
        ]
        return tasks

    def _expected_task_attributes(self):
        return {
            0: {'conds': list(range(4, 83, 2))},  # note, here we rely on task id assignment specifics, not too good for future
        }

    async def _additional_checks_on_finish(self):
        stat = await self.scheduler.data_access.invocations_statistics()
        self.assertEqual(41, stat.total)
        self.assertEqual(41, stat.finished_good)
        self.assertEqual(0, stat.finished_bad)

    def _minimal_idle_to_ensure(self) -> int:
        return 2

    def _minimal_total_to_ensure(self) -> int:
        return 4

    def _timeout(self) -> float:
        return 60.0
