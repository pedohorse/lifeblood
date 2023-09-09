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
        the_worker = None
        out_exr_path = os.path.join(tempfile.gettempdir(), 'test_render_foooo_husk.exr')
        if os.path.exists(out_exr_path):
            os.unlink(out_exr_path)

        def _logic_gen(skip_existing: bool):
            async def _logic(scheduler, workers, tmp_script_path, done_waiter):
                nonlocal the_worker
                updated_attrs = {}
                with mock.patch('lifeblood.scheduler.scheduler.Scheduler.update_task_attributes') as attr_patch:
                    attr_patch.side_effect = lambda *args, **kwargs: updated_attrs.update(args[1]) \
                                                                     or print(f'update_task_attributes with {args}, {kwargs}')

                    out_preexists = os.path.exists(out_exr_path)
                    pre_contents = None
                    if out_preexists:
                        with open(out_exr_path, 'rb') as f:
                            pre_contents = f.read()

                    node = create_node('houdini_husk', 'test husk', scheduler, 1)
                    node.set_param_value('skip if exists', skip_existing)

                    res = node.process_task(ProcessingContext(node, {'attributes': json.dumps({
                        'file': '/tmp/something/rs/filename.1234.usd',
                        'outimage': out_exr_path,
                        'frames': [1, 2, 3]
                    })}))

                    ij = res.invocation_job
                    self.assertTrue(ij is not None)
                    ij._set_envresolver_arguments(FakeEnvArgs())
                    if oh_no_its_windows:
                        # cuz windows does not allow to just run batch/python scripts with no extension...
                        for filename, contents in list(ij.extra_files().items()):
                            ij.set_extra_file(filename, contents.replace("'husk'", f"'python', {repr(str(Path(__file__).parent / 'data' / 'mock_houdini' / 'husk'))}"))

                        if ij.args()[0] == 'husk':
                            ij.args().pop(0)
                            ij.args().insert(0, str(Path(__file__).parent / 'data' / 'mock_houdini' / 'husk'))
                            ij.args().insert(0, 'python')

                    ij._set_task_id(2345)
                    ij._set_invocation_id(1234)
                    the_worker = workers[0]

                    await workers[0].run_task(
                        ij,
                        scheduler.server_message_address()
                    )

                    await asyncio.wait([done_waiter], timeout=30)

                    self.assertTrue(os.path.exists(out_exr_path))

                    if pre_contents is not None:
                        with open(out_exr_path, 'rb') as f:
                            post_contents = f.read()
                        if skip_existing:
                            self.assertEqual(pre_contents, post_contents)
                        else:
                            self.assertEqual(b'ok', post_contents)

            return _logic

        def _task_done_logic(task: InvocationJob):
            self.assertEqual(100.0, the_worker.task_status())

        for skip_exist, pre_exist in ((False, False), (True, False), (True, True)):
            print(f'testing with: skip_exist={skip_exist}')

            self.assertFalse(os.path.exists(out_exr_path))
            if pre_exist:
                os.makedirs(os.path.dirname(out_exr_path), exist_ok=True)
                with open(out_exr_path, 'w') as f:
                    f.write('some preexisting contents that is not the same as husk mock outputs')
            await self._helper_test_worker_node(
                _logic_gen(skip_exist),
                task_done_logic=_task_done_logic if not skip_exist or not pre_exist else lambda *args, **kwargs: None
            )
            if os.path.exists(out_exr_path):
                os.unlink(out_exr_path)

    async def test_karma_node(self):
        """
        NOTE !!

        This is a simplified COPY of test_husk_node.

        NOTE !!
        """
        the_worker = None
        out_exr_path = os.path.join(tempfile.gettempdir(), 'test_render_foooo_karma.exr')
        if os.path.exists(out_exr_path):
            os.unlink(out_exr_path)

        def _logic_gen(skip_existing: bool):
            async def _logic(scheduler, workers, tmp_script_path, done_waiter):
                nonlocal the_worker
                updated_attrs = {}
                with mock.patch('lifeblood.scheduler.scheduler.Scheduler.update_task_attributes') as attr_patch:
                    attr_patch.side_effect = lambda *args, **kwargs: updated_attrs.update(args[1]) \
                                                                     or print(f'update_task_attributes with {args}, {kwargs}')

                    out_preexists = os.path.exists(out_exr_path)
                    pre_contents = None
                    if out_preexists:
                        with open(out_exr_path, 'rb') as f:
                            pre_contents = f.read()

                    node = create_node('karma', 'test karma', scheduler, 1)
                    node.set_param_value('skip if exists', skip_existing)

                    res = node.process_task(ProcessingContext(node, {'attributes': json.dumps({
                        'file': '/tmp/something/rs/filename.1234.usd',
                        'outimage': out_exr_path,
                        'frames': [1, 2, 3]
                    })}))

                    ij = res.invocation_job
                    self.assertTrue(ij is not None)
                    ij._set_envresolver_arguments(FakeEnvArgs())
                    if oh_no_its_windows:
                        # cuz windows does not allow to just run batch/python scripts with no extension...
                        for filename, contents in list(ij.extra_files().items()):
                            ij.set_extra_file(filename, contents.replace("'husk'", f"'python', {repr(str(Path(__file__).parent / 'data' / 'mock_houdini' / 'husk'))}"))

                        if ij.args()[0] == 'husk':
                            ij.args().pop(0)
                            ij.args().insert(0, str(Path(__file__).parent / 'data' / 'mock_houdini' / 'husk'))
                            ij.args().insert(0, 'python')

                    ij._set_task_id(2345)
                    ij._set_invocation_id(1234)
                    the_worker = workers[0]

                    await workers[0].run_task(
                        ij,
                        scheduler.server_message_address()
                    )

                    await asyncio.wait([done_waiter], timeout=30)

                    self.assertTrue(os.path.exists(out_exr_path))

                    if pre_contents is not None:
                        with open(out_exr_path, 'rb') as f:
                            post_contents = f.read()
                        if skip_existing:
                            self.assertEqual(pre_contents, post_contents)
                        else:
                            self.assertEqual(b'ok', post_contents)

            return _logic

        def _task_done_logic(task: InvocationJob):
            self.assertEqual(100.0, the_worker.task_status())

        for skip_exist, pre_exist in ((False, False), (True, False), (True, True)):
            print(f'testing with: skip_exist={skip_exist}')

            self.assertFalse(os.path.exists(out_exr_path))
            if pre_exist:
                os.makedirs(os.path.dirname(out_exr_path), exist_ok=True)
                with open(out_exr_path, 'w') as f:
                    f.write('some preexisting contents that is not the same as husk mock outputs')
            await self._helper_test_worker_node(
                _logic_gen(skip_exist),
                task_done_logic=_task_done_logic if not skip_exist or not pre_exist else lambda *args, **kwargs: None
            )
            if os.path.exists(out_exr_path):
                os.unlink(out_exr_path)
