from pathlib import Path

from lifeblood_testing_common.nodes_common import TestCaseBase


class RedshiftTestCase(TestCaseBase):
    async def test_redshift_node(self):
        await self._helper_test_render_node('redshift', 'rs', 'redshiftCmdLine', Path(__file__).parent / 'data' / 'mock_redshift')

        # TODO: add check for 'files' attr (when redshift node is fixed to be able to set them on "skip existing")
