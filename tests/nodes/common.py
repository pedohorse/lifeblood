import asyncio
import os
import tempfile
from pathlib import Path
import sqlite3
from unittest import mock, IsolatedAsyncioTestCase
from lifeblood.db_misc import sql_init_script
from lifeblood.scheduler import Scheduler
from lifeblood.worker import Worker
from lifeblood.invocationjob import InvocationJob
from lifeblood.scheduler.pinger import Pinger

from typing import Callable, Optional


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
