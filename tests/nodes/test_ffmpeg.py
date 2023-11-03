import os
import asyncio
import re
import shutil
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
        return Environment({**os.environ,
                            'PATH': os.pathsep.join((str(Path(__file__).parent / 'data' / 'mock_ffmpeh'), os.environ.get('PATH', ''))),
                            'PYTHONUNBUFFERED': '1'})


class HuskTestCase(TestCaseBase):
    async def test_movie_to_sequence(self):
        temp_dir = tempfile.mkdtemp("lifeblood_tests")

        try:
            await self._helper_test_simple_invocation(
                'ffmpeg',
                [
                    {'operation_mode': 1},  # movie_to_sequence mode
                    {
                        'in_movie': os.path.join(temp_dir, 'movie.mov'),
                        'out_sequence': os.path.join(temp_dir, 'seq', 'file.%04d.png'),
                        'start_frame': 100,
                    }
                ],
                {},
                add_relative_to_PATH=Path('data') / 'mock_ffmpeg',
                commands_to_replace_with_py_mock=['ffmpeg', 'ffprobe'],
            )

            # we expect sequence
            files = list(os.listdir(os.path.join(temp_dir, 'seq')))
            self.assertEqual(7, len(files))
            frames = [int(re.match(r'file.(\d+).png', f).group(1)) for f in files]
            self.assertListEqual([100, 101, 102, 103, 104, 105, 106], sorted(frames))

        finally:
            shutil.rmtree(temp_dir)
