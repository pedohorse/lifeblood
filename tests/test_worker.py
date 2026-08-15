import os
import asyncio
import tempfile
from unittest import IsolatedAsyncioTestCase, mock
from lifeblood.enums import WorkerType, WorkerState
from lifeblood.hardware_resources import HardwareResources
from lifeblood.worker import Worker
from lifeblood.invocationjob import Invocation, InvocationJob, InvocationEnvironment, InvocationResources
from lifeblood.environment_resolver import EnvironmentResolverArguments
from lifeblood.net_messages.address import AddressChain
from lifeblood.worker_metadata import WorkerMetadata
from lifeblood_testing_common.common import create_default_scheduler


class RunningSchedulerTests(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        print('settingup done')

    @classmethod
    def tearDownClass(cls) -> None:
        print('tearingdown done')

    def setUp(self):
        self.__fd, self.__db_path = tempfile.mkstemp('_lifeblood.db')

    def tearDown(self):
        if self.__fd is not None:
            os.close(self.__fd)
        if self.__db_path is not None:
            os.unlink(self.__db_path)

    async def asyncSetUp(self) -> None:
        self.scheduler = create_default_scheduler(self.__db_path, do_broadcasting=False, helpers_minimal_idle_to_ensure=0, server_addr=('127.0.0.1', 12347, 12345), server_ui_addr=('127.0.0.1', 12346))
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
        job = InvocationJob(expected_args, env=expected_env)
        job._set_envresolver_arguments(EnvironmentResolverArguments('TrivialEnvironmentResolver', {
            'wawawa': 1234,
            'rororo': 'flofloflo',
        }))
        job._set_task_attributes({'test1': 42, 'TesT2': 'food', '_bad': 2.3, '__bbad': 'no',
                                  'nolists1': [1, 2, 3], 'nolists2': [],
                                  'nodicts1': {'a': 'b'}, 'nodicts2': {}})
        inv = Invocation(
            job,
            invocation_id=1123,
            task_id=6492,
            resources_to_use=InvocationResources({}, {})
        )
        with mock.patch('lifeblood.environment_resolver.create_process') as m, \
                mock.patch('shutil.which') as sw:
            sw.return_value = os.path.join(os.getcwd(), 'arg0')
            m.side_effect = Moxecption('expected exception')
            try:
                await worker.run_task(inv, AddressChain(''))
            except Moxecption:
                pass
            m.assert_called()
            test_args, test_env, test_cwd = m.call_args[0]
        print(test_args)
        print(test_env)
        print(test_cwd)
        self.assertListEqual(expected_args, test_args)
        self.assertDictEqual({**test_env, **expected_env.resolve()}, test_env)
        self.assertEqual(os.getcwd(), test_cwd)

        # test that task's attributes were set to env correctly
        self.assertIn('LBATTR_test1', test_env)
        self.assertEqual('42', test_env['LBATTR_test1'])
        self.assertIn('LBATTR_TesT2', test_env)
        self.assertIn('wawawa', test_env)
        self.assertIn('rororo', test_env)
        self.assertEqual('1234', test_env['wawawa'])
        self.assertEqual('flofloflo', test_env['rororo'])
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
        job = InvocationJob(['echo', 'task run'])
        inv = Invocation(
            job,
            invocation_id=1123,
            task_id=6492,
            resources_to_use=InvocationResources({}, {})
        )
        with mock.patch('lifeblood.worker_core.SchedulerWorkerControlClient.get_scheduler_control_client') as m:
            cm = mock.AsyncMock()
            m.return_value = cm
            cm.__enter__.return_value = cm
            await worker.run_task(inv, AddressChain('127.1.2.3:1234'))
            for i in range(15):  # reasonable timeout
                await asyncio.sleep(1)
                if not worker.is_task_running():
                    print('ye task done!')
                    cm.report_task_done.assert_called()
                    break
            else:
                self.assertEqual(False, True)

    #
    # sequential/double add worker with same address and hwid
    async def test_sequential_add_worker(self):
        # first we need to create incorrect situation
        with self.assertRaises(ValueError):
            await self.scheduler.get_worker_state(1)

        await self.scheduler.add_worker(
            AddressChain('127.0.0.1:23456'),
            WorkerType.STANDARD,
            HardwareResources(hwid=12345, devices=[], resources={}),
            worker_metadata=WorkerMetadata('testhost'),
        )
        self.assertEqual(WorkerState.IDLE, await self.scheduler.get_worker_state(1))
        # worker 1 added fine
        await self.scheduler.worker_stopped(AddressChain('127.0.0.1:23456'))
        self.assertEqual(WorkerState.OFF, await self.scheduler.get_worker_state(1))
        # worker 1 stopped fine

        await self.scheduler.add_worker(
            AddressChain('127.0.0.1:23456'),  # same address
            WorkerType.STANDARD,
            HardwareResources(hwid=12345, devices=[], resources={}),  # same hwid
            worker_metadata=WorkerMetadata('testhost'),
        )
        self.assertEqual(WorkerState.IDLE, await self.scheduler.get_worker_state(1))
        # worker 1 re-added fine
        # check that there is no worker 2
        with self.assertRaises(ValueError):
            await self.scheduler.get_worker_state(2)

    async def test_double_add_worker(self):
        # first we need to create incorrect situation
        with self.assertRaises(ValueError):
            await self.scheduler.get_worker_state(1)

        await self.scheduler.add_worker(
            AddressChain('127.0.0.1:23456'),
            WorkerType.STANDARD,
            HardwareResources(hwid=12345, devices=[], resources={}),
            worker_metadata=WorkerMetadata('testhost'),
        )
        self.assertEqual(WorkerState.IDLE, await self.scheduler.get_worker_state(1))
        await self.scheduler.add_worker(
            AddressChain('127.0.0.1:23456'),  # same address
            WorkerType.STANDARD,
            HardwareResources(hwid=12345, devices=[], resources={}),  # same hwid
            worker_metadata=WorkerMetadata('testhost'),
        )

        self.assertEqual(WorkerState.IDLE, await self.scheduler.get_worker_state(1))
        with self.assertRaises(ValueError):
            await self.scheduler.get_worker_state(2)

    #
    # sequential/double add worker with same address, different hwid
    async def test_sequential_add_worker_changed_hwid(self):
        # first we need to create incorrect situation
        with self.assertRaises(ValueError):
            await self.scheduler.get_worker_state(1)

        await self.scheduler.add_worker(
            AddressChain('127.0.0.1:23456'),
            WorkerType.STANDARD,
            HardwareResources(hwid=12345, devices=[], resources={}),
            worker_metadata=WorkerMetadata('testhost'),
        )
        self.assertEqual(WorkerState.IDLE, await self.scheduler.get_worker_state(1))
        # worker 1 added fine
        await self.scheduler.worker_stopped(AddressChain('127.0.0.1:23456'))
        self.assertEqual(WorkerState.OFF, await self.scheduler.get_worker_state(1))
        # worker 1 stopped fine

        await self.scheduler.add_worker(
            AddressChain('127.0.0.1:23456'),  # same address
            WorkerType.STANDARD,
            HardwareResources(hwid=54321, devices=[], resources={}),  # DIFFERENT hwid
            worker_metadata=WorkerMetadata('testhost'),
        )
        self.assertEqual(WorkerState.IDLE, await self.scheduler.get_worker_state(2))
        self.assertEqual(WorkerState.OFF, await self.scheduler.get_worker_state(1))
        # worker 2 added fine
        # checked that there is no reuse of worker 1

    async def test_double_add_worker_changed_hwid(self):
        # first we need to create incorrect situation
        with self.assertRaises(ValueError):
            await self.scheduler.get_worker_state(1)

        await self.scheduler.add_worker(
            AddressChain('127.0.0.1:23456'),
            WorkerType.STANDARD,
            HardwareResources(hwid=12345, devices=[], resources={}),
            worker_metadata=WorkerMetadata('testhost'),
        )
        self.assertEqual(WorkerState.IDLE, await self.scheduler.get_worker_state(1))
        with self.assertRaises(RuntimeError):
            await self.scheduler.add_worker(
                AddressChain('127.0.0.1:23456'),  # same address
                WorkerType.STANDARD,
                HardwareResources(hwid=54321, devices=[], resources={}),  # DIFFERENT hwid
                worker_metadata=WorkerMetadata('testhost'),
            )
        self.assertEqual(WorkerState.IDLE, await self.scheduler.get_worker_state(1))
