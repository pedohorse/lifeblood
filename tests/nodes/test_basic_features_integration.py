from lifeblood_testing_common.integration_common import FullIntegrationTestCase

from typing import Iterable, Tuple


class TestNodeStuff(FullIntegrationTestCase):
    @classmethod
    def _initial_db_file(cls) -> str:
        return 'data/test_basic_stuff.db'

    @classmethod
    def node_configs(cls) -> Tuple[dict, dict]:
        return {'set_attrib': {
            'conf1': 12,
            'conf2': 2.345,
        }}, {
            'conf1': 55,
            'conf3': 'foo be me',
        }

    async def _create_test_tasks(self) -> Iterable[int]:
        tasks = [
            *await self._create_task(node_name='IN EXPR CONTEXT'),
        ]
        return tasks

    def _expected_task_attributes(self):
        return {
            0: {
                'test1': 12,
                'test2': 2.345,
                'test3': 'foo be me',
                'test4': '12',
            },
        }

    async def _additional_checks_on_finish(self):
        pass

    def _minimal_idle_to_ensure(self) -> int:
        return 0

    def _minimal_total_to_ensure(self) -> int:
        return 0

    def _timeout(self) -> float:
        return 30.0
