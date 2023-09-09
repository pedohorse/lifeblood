import os
import asyncio
import json
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
        print(str(Path(__file__).parent / 'data' / 'mock_redshift'))
        return Environment({**os.environ,
                            'PATH': os.pathsep.join((str(Path(__file__).parent / 'data' / 'mock_redshift'), os.environ.get('PATH', ''))),
                            'PYTHONUNBUFFERED': '1'})


class RedshiftTestCase(TestCaseBase):
    async def test_redshift_node(self):
        the_worker = None

        async def _logic(scheduler, workers, tmp_script_path, done_waiter):
            nonlocal the_worker
            updated_attrs = {}
            with mock.patch('lifeblood.scheduler.scheduler.Scheduler.update_task_attributes') as attr_patch:
                attr_patch.side_effect = lambda *args, **kwargs: updated_attrs.update(args[1]) \
                                                                 or print(f'update_task_attributes with {args}, {kwargs}')

                node = create_node('redshift', 'test redshift', scheduler, 1)

                res = node.process_task(ProcessingContext(node, {'attributes': json.dumps({'file': '/tmp/something/rs/filename.1234.rs'})}))
                ij = res.invocation_job
                self.assertTrue(ij is not None)
                ij._set_envresolver_arguments(FakeEnvArgs())
                if oh_no_its_windows:
                    # cuz windows does not allow to just run batch/python scripts with no extension...
                    for filename, contents in list(ij.extra_files().items()):

                        ij.set_extra_file(filename, contents.replace("'redshiftCmdLine'", f"'python', {repr(str(Path(__file__).parent / 'data' / 'mock_redshift' / 'redshiftCmdLine'))}"))


                ij._set_task_id(2345)
                ij._set_invocation_id(1234)
                the_worker = workers[0]

                await workers[0].run_task(
                    ij,
                    scheduler.server_message_address()
                )

                await asyncio.wait([done_waiter], timeout=30)

                self.assertTrue(attr_patch.call_count == 1)
                self.assertIn('file', updated_attrs)
                self.assertIn('files', updated_attrs)
                self.assertDictEqual({
                    'file': '/tmp/test/out.1234.exr',
                    'files': ['/tmp/test/out.1234.exr',
                              '/tmp/fofo/with spaces/file.aov.1234.exr',
                              '/tmp/fofo/with юникод щит/file.aov.1234.exr'],
                }, updated_attrs)

        def _task_done_logic(task: InvocationJob):
            self.assertEqual(100.0, the_worker.task_status())

        await self._helper_test_worker_node(
            _logic,
            task_done_logic=_task_done_logic
        )

