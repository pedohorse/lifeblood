from pathlib import Path
from lifeblood_testing_common.nodes_common import TestCaseBase


# TODO: tests are currently very shallow !!

class MantraTestCase(TestCaseBase):
    async def test_mantra_node(self):
        await self._helper_test_render_node('mantra', 'ifd', 'mantra', Path(__file__).parent / 'data' / 'mock_houdini')
