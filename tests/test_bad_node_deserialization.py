from lifeblood.enums import TaskState
from lifeblood_testing_common.integration_common import FullIntegrationTestCase

from typing import Iterable, Union, Tuple, Dict


class TestBadNodeDeserialization(FullIntegrationTestCase):
    __test__ = True

    @classmethod
    def _initial_db_file(cls) -> str:
        return 'data/test_badndeser.db'

    async def _create_test_tasks(self) -> Iterable[int]:
        tasks = [
            *await self._create_task(node_name='TEST IN PROC BADOBJ'),
            *await self._create_task(node_name='TEST IN POSTPROC BADOBJ'),
            *await self._create_task(node_name='TEST IN PROC BADSTATE'),
            *await self._create_task(node_name='TEST IN POSTPROC BADSTATE'),
        ]
        await self.scheduler.force_change_task_state(tasks[0::2], TaskState.WAITING)
        await self.scheduler.force_change_task_state(tasks[1::2], TaskState.POST_WAITING)
        return tasks

    def _expected_asks_state(self) -> Union[Tuple[TaskState, bool, str], Dict[int, Tuple[TaskState, bool, str]]]:
        return {
            0: (TaskState.ERROR, False, 'TEST IN PROC BADOBJ'),
            1: (TaskState.ERROR, False, 'TEST IN POSTPROC BADOBJ'),
            2: (TaskState.ERROR, False, 'TEST IN PROC BADSTATE'),
            3: (TaskState.ERROR, False, 'TEST IN POSTPROC BADSTATE'),
        }

    def _timeout(self) -> float:
        return 15
