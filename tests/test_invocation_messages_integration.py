import os
import aiosqlite
import string
import random
import tempfile
from lifeblood.enums import TaskState
from lifeblood_testing_common.integration_common import FullIntegrationTestCase

from typing import Iterable, Union, Tuple, Dict


class TestInvocationMessaging(FullIntegrationTestCase):
    __test__ = True
    __cleanup = None

    @classmethod
    def _initial_db_file(cls) -> str:
        return 'data/test_invocmessaging.db'

    def cleanup(self):
        if not self.__unique_smth:
            return

        for fname in (f'lifeblood-foo-test-beep1-{self.__unique_smth}.txt',
                      f'lifeblood-foo-test-beep2-{self.__unique_smth}.txt',
                      f'lifeblood-foo-test-beep3-{self.__unique_smth}.txt',
                      f'lifeblood-foo-test-beep4-{self.__unique_smth}.txt'):
            filepath = os.path.join(tempfile.gettempdir(), fname)
            if os.path.exists(filepath):
                os.unlink(filepath)

    async def _create_test_tasks(self) -> Iterable[int]:
        self.__unique_smth = ''.join(random.choice(string.ascii_lowercase) for _ in range(12))
        self.addCleanup(self.cleanup)
        # clear some tempfiles. this test RELIES on same /tmp mount for all workers
        rng = random.Random(61827394)
        tasks = [
            *await self._create_task(node_name='TEST 1', attributes={
                'expected_messages': [
                    'blabla',
                    *[''.join(rng.choice(string.printable) for _ in range(rng.randint(2, 15))) for _ in range(50)]
                ],
                'uname': self.__unique_smth,
            }),
            *await self._create_task(node_name='TEST 2', attributes={
                'expected_messages': [
                    'bleble',
                    *[''.join(rng.choice(string.printable) for _ in range(rng.randint(2, 15))) for _ in range(50)]
                ],
                'uname': self.__unique_smth,
            }),
            *await self._create_task(node_name='TEST 3', attributes={
                'expected_messages': [
                    'blublu',
                    *[''.join(rng.choice(string.printable) for _ in range(rng.randint(2, 15))) for _ in range(50)]
                ],
                'uname': self.__unique_smth,
            }),
            *await self._create_task(node_name='TEST 4'),
        ]
        return tasks

    def _expected_asks_state(self) -> Union[Tuple[TaskState, bool, str], Dict[int, Tuple[TaskState, bool, str]]]:
        return {
            **{k: (TaskState.DONE, True, 'TEST OUT') for k in (0, 1, 2)},
            3: (TaskState.ERROR, False, 'receiver interrupted')
        }

    def _minimal_total_to_ensure(self) -> int:
        return 6

    def _timeout(self) -> float:
        return 120.0
