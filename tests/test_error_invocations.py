import aiosqlite
from lifeblood.enums import TaskState
from lifeblood_testing_common.integration_common import FullIntegrationTestCase

from typing import Iterable, Union, Tuple, Dict


class TestInvocationExitCodes(FullIntegrationTestCase):
    __test__ = True

    @classmethod
    def _initial_db_file(cls) -> str:
        return 'data/test_errorinv.db'

    async def _create_test_tasks(self) -> Iterable[int]:
        self.__task_that_should_retry = (await self._create_task(node_name='TEST ERROR RETRY'))[0]
        self.__task_that_should_just_error = (await self._create_task(node_name='TEST ERROR'))[0]
        self.__task_that_should_ok = (await self._create_task(node_name='TEST OK'))[0]
        self.__task_that_should_ok_42 = (await self._create_task(node_name='TEST OK STRANGE CODE', attributes={'exit': 42}))[0]
        self.__task_that_should_retry_0 = (await self._create_task(node_name='TEST OK STRANGE CODE', attributes={'exit': 0}))[0]
        tasks = [
            self.__task_that_should_ok,
            self.__task_that_should_just_error,
            self.__task_that_should_retry,
            self.__task_that_should_ok_42,
            self.__task_that_should_retry_0,
        ]
        return tasks

    async def _additional_checks_on_finish(self):
        async with self.scheduler.data_access.data_connection() as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT "work_data_invocation_attempt" FROM tasks WHERE "id" == ?', (self.__task_that_should_retry,)) as cur:
                data = await cur.fetchone()
            self.assertEqual(3, data['work_data_invocation_attempt'])
            async with con.execute('SELECT "work_data_invocation_attempt" FROM tasks WHERE "id" == ?', (self.__task_that_should_just_error,)) as cur:
                data = await cur.fetchone()
            self.assertEqual(0, data['work_data_invocation_attempt'])
            async with con.execute('SELECT "work_data_invocation_attempt" FROM tasks WHERE "id" == ?', (self.__task_that_should_ok,)) as cur:
                data = await cur.fetchone()
            self.assertEqual(0, data['work_data_invocation_attempt'])
            async with con.execute('SELECT "work_data_invocation_attempt" FROM tasks WHERE "id" == ?', (self.__task_that_should_ok_42,)) as cur:
                data = await cur.fetchone()
            self.assertEqual(0, data['work_data_invocation_attempt'])
            async with con.execute('SELECT "work_data_invocation_attempt" FROM tasks WHERE "id" == ?', (self.__task_that_should_retry_0,)) as cur:
                data = await cur.fetchone()
            self.assertEqual(3, data['work_data_invocation_attempt'])

    def _expected_asks_state(self) -> Union[Tuple[TaskState, bool, str], Dict[int, Tuple[TaskState, bool, str]]]:
        return {
            0: (TaskState.DONE, True, 'TEST OK OUT'),
            1: (TaskState.ERROR, False, 'TEST ERROR'),
            2: (TaskState.ERROR, False, 'TEST ERROR RETRY'),
            3: (TaskState.DONE, True, 'TEST OK STRANGE CODE OUT'),
            4: (TaskState.ERROR, False, 'TEST OK STRANGE CODE'),
        }

    def _minimal_total_to_ensure(self) -> int:
        return 5
