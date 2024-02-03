import os
from pathlib import Path

from lifeblood.invocationjob import Environment
from lifeblood.environment_resolver import EnvironmentResolverArguments

from lifeblood_testing_common.nodes_common import TestCaseBase


# TODO: tests are currently very shallow !!

class HuskTestCase(TestCaseBase):
    async def test_husk_node(self):
        await self._helper_test_render_node('houdini_husk', 'usd', 'husk', Path(__file__).parent / 'data' / 'mock_houdini')

    async def test_karma_node(self):
        await self._helper_test_render_node('karma', 'usd', 'husk', Path(__file__).parent / 'data' / 'mock_houdini')
