import os
import asyncio
import json
import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase, mock

from lifeblood.invocationjob import InvocationJob, Environment
from lifeblood.enums import SpawnStatus
from lifeblood.pluginloader import create_node
from lifeblood.processingcontext import ProcessingContext
from lifeblood.environment_resolver import EnvironmentResolverArguments
from lifeblood.process_utils import oh_no_its_windows

from .common import TestCaseBase


class FakeEnvArgs(EnvironmentResolverArguments):
    def get_environment(self):
        print(str(Path(__file__).parent / 'data' / 'mock_houdini'))
        return Environment({**os.environ,
                            'PATH': os.pathsep.join((str(Path(__file__).parent / 'data' / 'mock_houdini'), os.environ.get('PATH', ''))),
                            'PYTHONUNBUFFERED': '1'})


# TODO: tests are currently very shallow !!

class HuskTestCase(TestCaseBase):
    async def test_husk_node(self):
        await self._helper_test_render_node('houdini_husk', 'usd', 'husk', 'data/mock_houdini')

    async def test_karma_node(self):
        await self._helper_test_render_node('karma', 'usd', 'husk', 'data/mock_houdini')