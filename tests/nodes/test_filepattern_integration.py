from lifeblood_testing_common.integration_common import FullIntegrationTestCase

from typing import Iterable


class FilepatternTest(FullIntegrationTestCase):
    __test__ = True

    @classmethod
    def _initial_db_file(cls) -> str:
        return 'data/test_filepattern.db'

    async def _create_test_tasks(self) -> Iterable[int]:
        attrs = {
                'attr1': 23.41,
                'attr2': 4567,
                'base_dir': str(self.this_test_dir() / 'data' / 'filepattern' / 'tree1'),
            }
        tasks = [
            *await self._create_task(node_name='TEST IN', attributes={
                **attrs,
                'on_worker': False,
            }),
            *await self._create_task(node_name='TEST IN', attributes={
                **attrs,
                'on_worker': True,
            }),
        ]
        return tasks

    def _expected_task_attributes(self):
        self.maxDiff = 1024
        base_path = self.this_test_dir() / 'data' / 'filepattern' / 'tree1'
        exp_attrs = {
                'files': [
                    str(base_path / 'dirfooken' / 'bagogogbr'),
                    str(base_path / 'dirfooken' / 'barbr'),
                    str(base_path / 'dirgorfool' / 'baqwebr'),
                ],
                'inh_attr1': [23.41, 23.41, 23.41],
            }
        return {
            0: exp_attrs,
            1: exp_attrs,
        }

    def _timeout(self) -> float:
        return 90.0
