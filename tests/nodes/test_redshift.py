import os
import asyncio
import json
from pathlib import Path
from unittest import IsolatedAsyncioTestCase, mock

from lifeblood.invocationjob import InvocationJob, Environment
from lifeblood.enums import SpawnStatus
from lifeblood.processingcontext import ProcessingContext
from lifeblood.environment_resolver import EnvironmentResolverArguments
from lifeblood.process_utils import oh_no_its_windows

from .common import TestCaseBase, create_node


class FakeEnvArgs(EnvironmentResolverArguments):
    def get_environment(self):
        print(str(Path(__file__).parent / 'data' / 'mock_redshift'))
        return Environment({**os.environ,
                            'PATH': os.pathsep.join((str(Path(__file__).parent / 'data' / 'mock_redshift'), os.environ.get('PATH', ''))),
                            'PYTHONUNBUFFERED': '1'})


class RedshiftTestCase(TestCaseBase):
    async def test_redshift_node(self):
        await self._helper_test_render_node('redshift', 'rs', 'redshiftCmdLine', 'data/mock_redshift')

        # TODO: add check for 'files' attr (when redshift node is fixed to be able to set them on "skip existing")
