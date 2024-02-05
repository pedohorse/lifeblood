import asyncio
import time
import os
import inspect
from pathlib import Path
import shutil
from unittest import IsolatedAsyncioTestCase
from lifeblood.enums import TaskState
from lifeblood.main_scheduler import create_default_scheduler
from lifeblood.nethelpers import get_default_addr
from lifeblood.simple_worker_pool import WorkerPool
from lifeblood.net_messages.address import AddressChain
from lifeblood.taskspawn import TaskSpawn
from lifeblood.enums import SpawnStatus

from typing import Dict, Iterable, Optional, Tuple, Union


class FullIntegrationTestCase(IsolatedAsyncioTestCase):
    __test__ = False

    async def asyncSetUp(self):
        db_name = f'test_{self.__class__.__name__}.db'
        if os.path.exists(db_name):
            os.unlink(db_name)
        shutil.copy2(Path(inspect.getmodule(self.__class__).__file__).parent / self._initial_db_file(), db_name)

        test_server_port1 = 18273
        test_server_port2 = 18283
        test_server_port3 = 18293
        self.scheduler = create_default_scheduler(
            db_name,
            do_broadcasting=False,
            server_addr=(get_default_addr(), test_server_port1, test_server_port2),
            server_ui_addr=(get_default_addr(), test_server_port3)
        )
        self.worker_pool = WorkerPool(
            scheduler_address=AddressChain(f'{get_default_addr()}:{test_server_port2}'),
            minimal_idle_to_ensure=self._minimal_idle_to_ensure(),
            minimal_total_to_ensure=self._minimal_total_to_ensure(),
            maximum_total=self._maximum_total(),
        )

        await self.scheduler.start()
        await self.worker_pool.start()

    async def asyncTearDown(self):
        self.worker_pool.stop()
        self.scheduler.stop()
        await self.worker_pool.wait_till_stops()
        await self.scheduler.wait_till_stops()
        self.worker_pool = None
        self.scheduler = None

    async def test_main(self):
        # create test tasks
        task_ids = list(await self._create_test_tasks())
        print(f'test created tasks: {task_ids}')
        expected_states = self._expected_asks_state()
        if not isinstance(expected_states, dict):
            node_ids = await self.scheduler.node_name_to_id(expected_states[2])
            self.assertEqual(1, len(node_ids))
            expected_states = {tid: (expected_states[0], expected_states[1], node_ids[0]) for tid in task_ids}
        else:
            expected_states = {
                tid: (
                    exp[0],
                    exp[1],
                    (await self.scheduler.node_name_to_id(exp[2]))[0],
                ) for tid, exp in expected_states.items()
            }
        print(f'expecting {expected_states}')
        # so expected_states is dict of task id to (state, is paused, node_id), not node name

        required_succ_time = 0

        # wait for processing
        timeout = self._timeout()
        ts1 = time.perf_counter()
        check_succ_time = None
        while timeout > 0:
            await asyncio.sleep(0.5)
            ts2 = time.perf_counter()
            timeout -= ts2 - ts1
            ts1 = ts2
            if await self.__check_tasks(expected_states):
                if check_succ_time is None:
                    check_succ_time = time.perf_counter()
                if time.perf_counter() - check_succ_time >= required_succ_time:
                    # check successful for enough time!
                    break
            else:
                check_succ_time = None
        else:
            self.assertTrue(False, "tasks have not reached required state!")

        # now check attributes
        for idx, attribs in self._expected_task_attributes().items():
            task_id = task_ids[idx]
            actual_attribs, _ = await self.scheduler.get_task_attributes(task_id)
            self.assertDictEqual(attribs, {k: v for k, v in actual_attribs.items() if k in attribs}, 'tasks finished as expected, but attribs are wrong')

        await self._additional_checks_on_finish()

    async def __check_tasks(self, expected_states: Dict[int, Tuple[TaskState, int]]) -> bool:
        actual = {task_id: (
            *(await self.scheduler.data_access.get_task_state(task_id)),
            await self.scheduler.data_access.get_task_node(task_id)
        ) for task_id in expected_states.keys()}
        return actual == expected_states

    async def _create_task(self, *, task_name: str = 'test task', node_name: str = 'TEST IN', output_name: str = 'main', attributes: Optional[dict] = None):
        if attributes is None:
            attributes = {}
        node_ids = await self.scheduler.node_name_to_id(node_name)
        self.assertEqual(1, len(node_ids))
        task_spawn = TaskSpawn(task_name, task_attributes=attributes)
        task_spawn.force_set_node_task_id(node_ids[0], None)
        task_spawn.set_node_output_name(output_name)

        statuses = await self.scheduler.spawn_tasks([task_spawn])
        self.assertEqual(1, len(statuses))
        self.assertEqual(SpawnStatus.SUCCEEDED, statuses[0][0])
        return [statuses[0][1]]

    @classmethod
    def _initial_db_file(cls) -> str:
        raise NotImplementedError()

    async def _create_test_tasks(self) -> Iterable[int]:
        """
        should return iterable of created task ids
        """
        raise NotImplementedError()

    def _expected_asks_state(self) -> Union[Tuple[TaskState, bool, str], Dict[int, Tuple[TaskState, bool, str]]]:
        """
        should return either one single tuple of (task state, is paused, node name)
        or a dict of which of created test tasks are in what state

        Default impl expects all test tasks to be done and paused
        """
        return TaskState.DONE, True, 'TEST OUT'

    def _expected_task_attributes(self) -> Dict[int, dict]:
        """
        keys in returned dict are NOT task_id, as task_ids are not know till test run.
        they are instead indexes into _create_test_tasks returned array,
        as at least that array's length is predictable by the test writer
        """
        return {}

    async def _additional_checks_on_finish(self):
        return

    def _timeout(self) -> float:
        return 15.0

    def _minimal_idle_to_ensure(self) -> int:
        return 1

    def _minimal_total_to_ensure(self) -> int:
        return 0

    def _maximum_total(self) -> int:
        return 16
