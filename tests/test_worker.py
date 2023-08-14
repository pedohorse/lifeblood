import os
import asyncio
import logging
import tempfile
from unittest import IsolatedAsyncioTestCase, mock
from lifeblood.worker import Worker
from lifeblood.scheduler import Scheduler
from lifeblood.logging import set_default_loglevel
from lifeblood.invocationjob import InvocationJob, InvocationEnvironment
from lifeblood.net_messages.address import AddressChain


class RunningSchedulerTests(IsolatedAsyncioTestCase):
    __fd = None
    __db_path = None

    @classmethod
    def setUpClass(cls) -> None:
        set_default_loglevel(logging.DEBUG)
        cls.__fd, cls.__db_path = tempfile.mkstemp('_lifeblood.db')
        print('settingup done')

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.__fd is not None:
            os.close(cls.__fd)
        if cls.__db_path is not None:
            os.unlink(cls.__db_path)
        print('tearingdown done')

    async def asyncSetUp(self) -> None:
        self.scheduler = Scheduler(self.__db_path, do_broadcasting=False, helpers_minimal_idle_to_ensure=0, server_addr=('127.0.0.1', 12347, 12345), server_ui_addr=('127.0.0.1', 12346))
        if not self.scheduler.is_started():
            await self.scheduler.start()

    async def asyncTearDown(self) -> None:
        if self.scheduler.is_started():
            self.scheduler.stop()
            await self.scheduler.wait_till_stops()


class Moxecption(Exception):
    pass


class WorkerRunTest(RunningSchedulerTests):
    async def test_worker_run_task_env(self):
        worker = Worker(AddressChain('127.0.0.1:12345'))
        # NOTE: we are testing on non-started worker...
        expected_env = InvocationEnvironment()
        expected_env.set_variable('qwe', 'rty')
        expected_env.set_variable('asd', 'fgh')
        expected_args = ['arg0', '-1', 'ass']
        job = InvocationJob(expected_args, env=expected_env, invocation_id=1123)
        job._set_task_attributes({'test1': 42, 'TesT2': 'food', '_bad': 2.3, '__bbad': 'no',
                                  'nolists1': [1, 2, 3], 'nolists2': [],
                                  'nodicts1': {'a': 'b'}, 'nodicts2': {}})
        with mock.patch('lifeblood.worker.create_process') as m,\
                mock.patch('shutil.which') as sw:
            sw.return_value = os.path.join(os.getcwd(), 'arg0')
            m.side_effect = Moxecption('expected exception')
            try:
                await worker.run_task(job, AddressChain(''))
            except Moxecption:
                pass
            m.assert_called()
            test_args, test_env, test_cwd = m.call_args[0]
        print(test_args)
        print(test_env)
        print(test_cwd)
        self.assertListEqual(expected_args, test_args)
        self.assertEqual(test_env, {**test_env, **expected_env.resolve()})
        self.assertEqual(os.getcwd(), test_cwd)

        # test that task's attributes were set to env correctly
        self.assertIn('LBATTR_test1', test_env)
        self.assertEqual('42', test_env['LBATTR_test1'])
        self.assertIn('LBATTR_TesT2', test_env)
        self.assertEqual('food', test_env['LBATTR_TesT2'])
        self.assertNotIn('LBATTR__bad', test_env)
        self.assertNotIn('LBATTR_bad', test_env)
        self.assertNotIn('LBATTR___bbad', test_env)
        self.assertNotIn('LBATTR__bbad', test_env)
        self.assertNotIn('LBATTR_bbad', test_env)
        self.assertNotIn('LBATTR_nolists1', test_env)  # dicts and lists may increase env block too much
        self.assertNotIn('LBATTR_nolists2', test_env)  # so we do NOT promote them
        self.assertNotIn('LBATTR_nodicts1', test_env)
        self.assertNotIn('LBATTR_nodicts2', test_env)

    async def test_run_task_report(self):
        worker = Worker(AddressChain('127.0.0.1:12345'))
        # NOTE: we are testing on non-started worker...
        job = InvocationJob(['echo', 'task run'], invocation_id=1123)
        with mock.patch('lifeblood.worker.SchedulerWorkerControlClient.get_scheduler_control_client') as m:
            cm = mock.AsyncMock()
            m.return_value = cm
            cm.__enter__.return_value = cm
            await worker.run_task(job, AddressChain('127.1.2.3:1234'))
            for i in range(15):  # reasonable timeout
                await asyncio.sleep(1)
                if not worker.is_task_running():
                    print('ye task done!')
                    cm.report_task_done.assert_called()
                    break
            else:
                self.assertEqual(False, True)
