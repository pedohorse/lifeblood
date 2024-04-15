import os
import aiosqlite
import string
import random
import tempfile
from lifeblood.enums import TaskState
from lifeblood_testing_common.integration_common import FullIntegrationTestCase

from typing import Iterable, Union, Tuple, Dict, Optional


class TestInvocationMessaging(FullIntegrationTestCase):
    __test__ = True

    @classmethod
    def _initial_db_file(cls) -> str:
        return 'data/test_attribserialization.db'

    async def _create_test_tasks(self):
        return [
            *await self._create_task(node_name='IN1', attributes={}),
            *await self._create_task(node_name='IN2', attributes={}),
            *await self._create_task(node_name='IN3', attributes={}),
            *await self._create_task(node_name='ING', attributes={}),
        ]

    def _expected_asks_state(self) -> Union[Tuple[TaskState, bool, str], Dict[int, Tuple[TaskState, bool, str]]]:
        return {
            0: (TaskState.ERROR, False, 'python bad1'),
            1: (TaskState.ERROR, False, 'python bad2'),
            2: (TaskState.ERROR, False, 'python bad3'),
            3: (TaskState.DONE, True, 'OUT'),
        }
