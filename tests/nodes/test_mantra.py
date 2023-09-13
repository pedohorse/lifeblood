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


# TODO: tests are currently very shallow !!

class MantraTestCase(TestCaseBase):
    async def test_mantra_node(self):
        await self._helper_test_render_node('mantra', 'ifd', 'mantra', 'data/mock_houdini')
