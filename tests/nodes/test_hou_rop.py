import json

import shutil
from pathlib import Path
from lifeblood_testing_common.nodes_common import TestCaseBase, PseudoContext
import tempfile


class TestHipScript(TestCaseBase):
    def setUp(self):
        self._tmp_path = Path(tempfile.mkdtemp('unittest'))

    def tearDown(self):
        shutil.rmtree(self._tmp_path)

    async def test_simple_no_checkpoint(self):
        (self._tmp_path / 'out').mkdir()
        with open(self._tmp_path / 'test.hip', 'w') as f:
            json.dump({
                'bad_frames': [],
                'default_output': str(self._tmp_path / 'out'),
            }, f)

        await self._helper_test_simple_invocation(
            'hip_usd_generator',
            [{
                'hip path': (self._tmp_path / 'test.hip'),
                'scene file output': (self._tmp_path / 'delme.$F4.usd'),
                'do checkpoint': False,
                'driver path': '/rop/driver'
            }],
            {
                'frames': [1234, 12, 333],
            },
            add_relative_to_PATH=Path(__file__).parent / 'data' / 'mock_houdini',
            commands_to_replace_with_py_mock=['hython'],
        )

        render_log_path = self._tmp_path / 'out' / 'render_log'
        self.assertTrue(render_log_path.exists())
        lines = render_log_path.read_text().splitlines(keepends=False)
        self.assertEqual(
            [
                '/rop/driver ::: 1234',
                '/rop/driver ::: 12',
                '/rop/driver ::: 333',
            ],
            lines,
        )

    async def test_simple_checkpoint(self):
        (self._tmp_path / 'out').mkdir()
        with open(self._tmp_path / 'test.hip', 'w') as f:
            json.dump({
                'bad_frames': [],
                'default_output': str(self._tmp_path / 'out'),
            }, f)

        await self._helper_test_simple_invocation(
            'hip_usd_generator',
            [{
                'hip path': (self._tmp_path / 'test.hip'),
                'scene file output': (self._tmp_path / 'delme.$F4.usd'),
                'do checkpoint': True,
                'driver path': '/rop/driver'
            }],
            {
                'frames': [1234, 12, 333],
            },
            add_relative_to_PATH=Path(__file__).parent / 'data' / 'mock_houdini',
            commands_to_replace_with_py_mock=['hython'],
        )

        render_log_path = self._tmp_path / 'out' / 'render_log'
        self.assertTrue(render_log_path.exists())
        lines = render_log_path.read_text().splitlines(keepends=False)
        self.assertEqual(
            [
                '/rop/driver ::: 1234',
                '/rop/driver ::: 12',
                '/rop/driver ::: 333',
            ],
            lines,
        )

    async def test_checkpoint_crash_continue(self):
        await self._helper_test_crash_continue(do_checkpoint=True)

    async def test_no_checkpoint_crash_continue(self):
        await self._helper_test_crash_continue(do_checkpoint=False)

    async def _helper_test_crash_continue(self, do_checkpoint: bool = False):
        (self._tmp_path / 'out').mkdir()
        with open(self._tmp_path / 'test_crash.hip', 'w') as f:
            json.dump({
                'bad_frames': [12],
                'default_output': str(self._tmp_path / 'out'),
            }, f)
        with open(self._tmp_path / 'test.hip', 'w') as f:
            json.dump({
                'bad_frames': [],
                'default_output': str(self._tmp_path / 'out'),
            }, f)

        checkpoint_path = self._tmp_path / f'task-{1}-{1}.chkpt'
        await self._helper_test_simple_invocation(
            'hip_usd_generator',
            [{
                'hip path': (self._tmp_path / 'test_crash.hip'),
                'scene file output': (self._tmp_path / 'delme.$F4.usd'),
                'do checkpoint': do_checkpoint,
                'driver path': '/rop/driver'
            }],
            {
                'frames': [1234, 12, 333],
            },
            add_relative_to_PATH=Path(__file__).parent / 'data' / 'mock_houdini',
            commands_to_replace_with_py_mock=['hython'],
            expected_task_exit_code=1,
        )
        self.assertEqual(do_checkpoint, checkpoint_path.exists())

        # now run again, expect to continue from checkpoint
        await self._helper_test_simple_invocation(
            'hip_usd_generator',
            [{
                'hip path': (self._tmp_path / 'test.hip'),
                'scene file output': (self._tmp_path / 'delme.$F4.usd'),
                'do checkpoint': do_checkpoint,
                'driver path': '/rop/driver'
            }],
            {
                'frames': [1234, 12, 333],
            },
            add_relative_to_PATH=Path(__file__).parent / 'data' / 'mock_houdini',
            commands_to_replace_with_py_mock=['hython'],
        )
        self.assertFalse(checkpoint_path.exists())

        # now we expect no duplicated lines in log
        render_log_path = self._tmp_path / 'out' / 'render_log'
        self.assertTrue(render_log_path.exists())
        lines = render_log_path.read_text().splitlines(keepends=False)

        if do_checkpoint:
            self.assertEqual(
                [
                    '/rop/driver ::: 1234',
                    '/rop/driver ::: 12',
                    '/rop/driver ::: 333',
                ],
                lines,
            )
        else:
            self.assertEqual(
                [
                    '/rop/driver ::: 1234',
                    '/rop/driver ::: 1234',
                    '/rop/driver ::: 12',
                    '/rop/driver ::: 333',
                ],
                lines,
            )

    # TODO: test checkpoint must be discarded if task had different parameters (use script hash for example)