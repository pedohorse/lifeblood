import asyncio
import os
import tempfile
from pathlib import Path
import sqlite3
import json
from unittest import mock, IsolatedAsyncioTestCase
from lifeblood.db_misc import sql_init_script
from lifeblood.scheduler import Scheduler
from lifeblood.worker import Worker
from lifeblood.invocationjob import InvocationJob, Environment
from lifeblood.scheduler.pinger import Pinger
from lifeblood.pluginloader import create_node
from lifeblood.processingcontext import ProcessingContext
from lifeblood.process_utils import oh_no_its_windows
from lifeblood.environment_resolver import EnvironmentResolverArguments

from typing import Callable, Optional


class FakeEnvArgs(EnvironmentResolverArguments):
    def __init__(self, rel_path_to_bin: str):
        super().__init__()
        self.__bin_path = Path(rel_path_to_bin)

    def get_environment(self):
        print(str(Path(__file__).parent / self.__bin_path))
        return Environment({**os.environ,
                            'PATH': os.pathsep.join((str(Path(__file__).parent / self.__bin_path), os.environ.get('PATH', ''))),
                            'PYTHONUNBUFFERED': '1'})



def purge_db():
    testdbpath = Path('test_swc.db')
    if testdbpath.exists():
        testdbpath.unlink()
    testdbpath.touch()
    with sqlite3.connect('test_swc.db') as con:
        con.executescript(sql_init_script)


class TestCaseBase(IsolatedAsyncioTestCase):
    async def _helper_test_worker_node(self,
                                       logic: Callable,
                                       *,
                                       task_done_logic: Optional[Callable] = None,
                                       runcode: Optional[str] = None,
                                       worker_count: int = 1,
                                       tasks_to_complete=None):
        purge_db()
        with mock.patch('lifeblood.scheduler.scheduler.Pinger') as ppatch, \
             mock.patch('lifeblood.worker.Worker.scheduler_pinger') as wppatch:

            ppatch.return_value = mock.AsyncMock(Pinger)
            wppatch.return_value = mock.AsyncMock()

            sched = Scheduler('test_swc.db', do_broadcasting=False, helpers_minimal_idle_to_ensure=0)
            await sched.start()

            workers = []
            for i in range(worker_count):
                worker = Worker(sched.server_message_address())
                await worker.start()
                workers.append(worker)

            await asyncio.gather(sched.wait_till_starts(),
                                 *[worker.wait_till_starts() for worker in workers])

            #
            fd, tmp_script_path = None, None
            if runcode:
                fd, tmp_script_path = tempfile.mkstemp('.py')
            try:
                if tmp_script_path:
                    with open(tmp_script_path, 'w') as f:
                        f.write(runcode)

                done_ev = asyncio.Event()
                tasks_to_complete = tasks_to_complete or worker_count
                side_effect_was_good = True
                with mock.patch('lifeblood.scheduler.scheduler.Scheduler.task_done_reported') as td_patch:
                    def _side_effect(task: InvocationJob, stdout: str, stderr: str):
                        nonlocal tasks_to_complete, side_effect_was_good
                        tasks_to_complete -= 1
                        print(f'finished {task.task_id()} out: {stdout}')
                        print(f'finished {task.task_id()} err: {stderr}')
                        print(f'exit code: {task.exit_code()}')
                        side_effect_was_good = side_effect_was_good and 0 == task.exit_code()
                        if task_done_logic:
                            try:
                                task_done_logic(task)
                            except Exception as e:
                                side_effect_was_good = False
                                print(e)
                        if tasks_to_complete <= 0:
                            done_ev.set()

                    td_patch.side_effect = _side_effect
                    done_waiter = asyncio.create_task(done_ev.wait())
                    await logic(sched, workers, tmp_script_path, done_waiter)
                    self.assertTrue(side_effect_was_good)
            finally:
                if tmp_script_path:
                    os.close(fd)
                    os.unlink(tmp_script_path)

                for worker in workers:
                    worker.stop()
                    await worker.wait_till_stops()
                sched.stop()
                await sched.wait_till_stops()

    async def _helper_test_render_node(self, node_type_name, scn_ext, command, bin_rel_path):
        the_worker = None
        out_exr_path = os.path.join(tempfile.gettempdir(), f'test_render_foooo_{node_type_name}.exr')
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

                    node = create_node(node_type_name, f'test {node_type_name}', scheduler, 1)
                    node.set_param_value('skip if exists', skip_existing)

                    scn_filepath = f'/tmp/something/karma/filename.1234.{scn_ext}'
                    res = node.process_task(ProcessingContext(node, {'attributes': json.dumps({
                        'file': scn_filepath,
                        'outimage': out_exr_path,
                        'frames': [1, 2, 3]
                    })}))

                    ij = res.invocation_job
                    self.assertTrue(ij is not None)
                    ij._set_envresolver_arguments(FakeEnvArgs(bin_rel_path))
                    if oh_no_its_windows:
                        # cuz windows does not allow to just run batch/python scripts with no extension...
                        for filename, contents in list(ij.extra_files().items()):
                            ij.set_extra_file(filename, contents.replace(f"'{command}'", f"'python', {repr(str(Path(__file__).parent / 'data' / 'mock_houdini' / command))}"))

                        if ij.args()[0] == command:
                            ij.args().pop(0)
                            ij.args().insert(0, str(Path(__file__).parent / Path(bin_rel_path) / command))
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

                    with open(out_exr_path, 'rb') as f:
                        post_contents = f.read()
                    if pre_contents is not None and skip_existing:
                        self.assertEqual(pre_contents, post_contents)
                    if pre_contents is None or not skip_existing:
                        line_ok, line_args, line_fname = post_contents.splitlines(keepends=False)
                        self.assertEqual(b'ok', line_ok)
                        self.assertEqual(scn_filepath.encode(), line_fname)

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
