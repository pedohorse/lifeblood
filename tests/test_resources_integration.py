import uuid

from lifeblood.config import Config
from lifeblood.worker_resource_definition import WorkerDeviceTypeDefinition, WorkerResourceDefinition, WorkerResourceDataType
from lifeblood_testing_common.integration_common import FullIntegrationTestCase

from typing import Iterable, Optional, Tuple

# TODO: to cover:
#  - no device
#  - 1 min device req, 0 pref
#  - 1 min device req, >1 pref
#  - >1 min device req


class ResourceBaseTestCase(FullIntegrationTestCase):
    def _device_type_definitions(self) -> Optional[Tuple[WorkerDeviceTypeDefinition, ...]]:
        return (
            WorkerDeviceTypeDefinition('gapauu', (
                WorkerResourceDefinition('megaresfoo', WorkerResourceDataType.GENERIC_FLOAT, 'foo is foo', 'Foo', 0),
                WorkerResourceDefinition('megaresbar', WorkerResourceDataType.GENERIC_INT, 'bar is bar', 'Bar', 0),
            )),
        )

    def _resource_definitions(self) -> Optional[Tuple[WorkerResourceDefinition, ...]]:
        return (
            WorkerResourceDefinition('cpu_count', WorkerResourceDataType.SHARABLE_COMPUTATIONAL_UNIT, 'test cpu', 'cpu cores'),
        )

    def _worker_config(self) -> Optional[Config]:
        config = Config()
        config.set_option_noasync('devices', {
            'gapauu': {
                'devname1': {
                    'resources': {
                        'megaresfoo': 22.2,
                        'megaresbar': 42,
                    },
                },
                'devname2': {
                    'resources': {
                        'megaresfoo': 11.1,
                        'megaresbar': 21,
                    },
                },
            },
        })
        config.set_option_noasync('resources', {
            'cpu_count': 5,
        })
        return config

    def _timeout(self) -> float:
        return 90.0

    def _minimal_helper_idle_to_ensure(self):
        return 0


class TestDeviceRequirement(ResourceBaseTestCase):
    __test__ = True

    @classmethod
    def _initial_db_file(cls) -> str:
        return 'data/test_resources.db'

    async def _create_test_tasks(self) -> Iterable[int]:
        tasks = [
            *await self._create_task(node_name='TEST IN', attributes={
            }),
        ]
        return tasks

    def _expected_task_attributes(self):
        return {
        }

    async def _additional_checks_on_finish(self, task_attributes):
        print(task_attributes)
        count_dev1 = 0
        count_dev2 = 0
        self.assertSetEqual({0}, set(task_attributes.keys()))
        self.assertSetEqual({'used_devs'}, set(task_attributes[0].keys()))
        self.assertEqual(6, len(task_attributes[0]['used_devs']))  # 6 children
        for task_attrs in task_attributes[0]['used_devs']:  # it's a list of attributes dicts from all children tasks
            self.assertSetEqual({'gapauu'}, set(task_attrs.keys()))
            if {'devname1': {}} == task_attrs['gapauu']:
                count_dev1 += 1
            elif {'devname2': {}} == task_attrs['gapauu']:
                count_dev2 += 1
            else:
                self.assertTrue(False, f'unexpected task dev usage f{task_attrs["gapauu"]}')
        self.assertGreater(count_dev1, 0)
        self.assertGreater(count_dev2, 0)

    def _additional_checks_interval(self) -> float:
        return 0.1

    async def _additional_checks_during_run(self):
        count = 0
        devices = set()
        for wid in range(1, 100):  # there should not be more than that
            res = await self.scheduler.data_access.get_invocation_resources_assigned_to(wid)
            if res is None:
                continue
            count += 1
            self.assertSetEqual({'gapauu'}, set(res.devices.keys()))
            devices.update(res.devices['gapauu'])

        self.assertGreaterEqual(2, count)
        if count == 0:
            self.assertSetEqual(set(), devices)
        elif count == 1:
            self.assertTrue({'devname1'} == devices or {'devname2'} == devices, devices)
        elif count == 2:
            self.assertSetEqual({'devname1', 'devname2'}, devices)
        else:
            raise AssertionError('should have been caught by check above')


class TestDeviceRequirementMultipleHardware(ResourceBaseTestCase):
    __test__ = True

    @classmethod
    def _initial_db_file(cls) -> str:
        return 'data/test_resources.db'

    async def _create_test_tasks(self) -> Iterable[int]:
        tasks = [
            *await self._create_task(node_name='TEST IN', attributes={
            }),
        ]
        return tasks

    def _expected_task_attributes(self):
        return {
        }

    async def _additional_checks_on_finish(self, task_attributes):
        print(task_attributes)
        count_dev1_1 = 0
        count_dev1_2 = 0
        count_dev2_1 = 0
        count_dev2_2 = 0
        self.assertSetEqual({0}, set(task_attributes.keys()))
        self.assertSetEqual({'used_devs'}, set(task_attributes[0].keys()))
        self.assertEqual(6, len(task_attributes[0]['used_devs']))  # 6 children
        for task_attrs in task_attributes[0]['used_devs']:  # it's a list of attributes dicts from all children tasks
            self.assertSetEqual({'gapauu'}, set(task_attrs.keys()))
            if {'devname1': {}} == task_attrs['gapauu']:
                count_dev1_1 += 1
            elif {'devname2': {}} == task_attrs['gapauu']:
                count_dev1_2 += 1
            elif {'2devname1': {}} == task_attrs['gapauu']:
                count_dev2_1 += 1
            elif {'2devname2': {}} == task_attrs['gapauu']:
                count_dev2_2 += 1
            else:
                self.assertTrue(False, f'unexpected task dev usage f{task_attrs["gapauu"]}')
        stat = (count_dev1_1, count_dev1_2, count_dev2_1, count_dev2_2)
        self.assertGreater(count_dev1_1, 0, stat)
        self.assertGreater(count_dev1_2, 0, stat)
        self.assertGreater(count_dev2_1, 0, stat)
        self.assertGreater(count_dev2_2, 0, stat)

    def _additional_checks_interval(self) -> float:
        return 0.1

    async def _additional_checks_during_run(self):
        count = 0
        devices = set()
        for wid in range(1, 100):  # there should not be more than that
            res = await self.scheduler.data_access.get_invocation_resources_assigned_to(wid)
            if res is None:
                continue
            count += 1
            self.assertSetEqual({'gapauu'}, set(res.devices.keys()))
            devices.update(res.devices['gapauu'])

        self.assertGreaterEqual(4, count)
        if count == 0:
            self.assertSetEqual(set(), devices)
        elif count == 1:
            self.assertTrue(
                'devname1' in devices or
                'devname2' in devices or
                '2devname1' in devices or
                '2devname2' in devices
                , devices)
        elif count == 2:
            self.assertTrue(
                {'devname1', 'devname2'} == devices or
                {'devname1', '2devname1'} == devices or
                {'devname1', '2devname2'} == devices or
                {'devname2', '2devname1'} == devices or
                {'devname2', '2devname2'} == devices or
                {'2devname1', '2devname2'} == devices
                ,
                devices
            )
        elif count == 3:
            self.assertTrue(
                {'devname1', 'devname2', '2devname1'} == devices or
                {'devname1', 'devname2', '2devname2'} == devices or
                {'devname1', '2devname1', '2devname2'} == devices or
                {'devname2', '2devname1', '2devname2'} == devices
                ,
                devices
            )
        elif count == 4:
            self.assertSetEqual({'devname1', 'devname2', '2devname1', '2devname2'}, devices, devices)
        else:
            raise AssertionError('should have been caught by check above')

    def _worker_config2(self) -> Optional[Config]:
        config = Config()
        hwid = uuid.uuid4().int
        hwid &= (1 << 63) - 1
        config.set_option_noasync('worker.override_hwid', hwid)
        config.set_option_noasync('devices', {
            'gapauu': {
                '2devname1': {
                    'resources': {
                        'megaresfoo': 22.2,
                        'megaresbar': 42,
                    },
                },
                '2devname2': {
                    'resources': {
                        'megaresfoo': 11.1,
                        'megaresbar': 21,
                    },
                },
            },
        })
        config.set_option_noasync('resources', {
            'cpu_count': 5,
        })
        return config


class TestDeviceRequirementMultipleHardware2(ResourceBaseTestCase):
    __test__ = True

    @classmethod
    def _initial_db_file(cls) -> str:
        return 'data/test_resources.db'

    async def _create_test_tasks(self) -> Iterable[int]:
        tasks = [
            *await self._create_task(node_name='TEST IN', attributes={
            }),
        ]
        return tasks

    def _expected_task_attributes(self):
        return {
        }

    async def _additional_checks_on_finish(self, task_attributes):
        print(task_attributes)
        count_dev1_1 = 0
        count_dev1_2 = 0
        count_dev2_1 = 0
        count_dev2_2 = 0
        self.assertSetEqual({0}, set(task_attributes.keys()))
        self.assertSetEqual({'used_devs'}, set(task_attributes[0].keys()))
        self.assertEqual(6, len(task_attributes[0]['used_devs']))  # 6 children
        for task_attrs in task_attributes[0]['used_devs']:  # it's a list of attributes dicts from all children tasks
            self.assertSetEqual({'gapauu'}, set(task_attrs.keys()))
            if {'devname1': {}} == task_attrs['gapauu']:
                count_dev1_1 += 1
            elif {'devname2': {}} == task_attrs['gapauu']:
                count_dev1_2 += 1
            elif {'2devname1': {}} == task_attrs['gapauu']:
                count_dev2_1 += 1
            elif {'2devname2': {}} == task_attrs['gapauu']:
                count_dev2_2 += 1
            else:
                self.assertTrue(False, f'unexpected task dev usage f{task_attrs["gapauu"]}')
        stat = (count_dev1_1, count_dev1_2, count_dev2_1, count_dev2_2)
        self.assertGreater(count_dev1_1, 0, stat)
        self.assertEqual(count_dev1_2, 0, stat)
        self.assertGreater(count_dev2_1, 0, stat)
        self.assertEqual(count_dev2_2, 0, stat)

    def _additional_checks_interval(self) -> float:
        return 0.1

    async def _additional_checks_during_run(self):
        count = 0
        devices = set()
        for wid in range(1, 100):  # there should not be more than that
            res = await self.scheduler.data_access.get_invocation_resources_assigned_to(wid)
            if res is None:
                continue
            count += 1
            self.assertSetEqual({'gapauu'}, set(res.devices.keys()))
            devices.update(res.devices['gapauu'])

        self.assertGreaterEqual(2, count)
        if count == 0:
            self.assertSetEqual(set(), devices)
        elif count == 1:
            self.assertTrue(
                'devname1' in devices or
                '2devname1' in devices
                ,
                devices)
        elif count == 2:
            self.assertSetEqual({'devname1', '2devname1'}, devices, devices)
        else:
            raise AssertionError('should have been caught by check above')

    def _worker_config(self) -> Optional[Config]:
        config = Config()
        config.set_option_noasync('devices', {
            'gapauu': {
                'devname1': {
                    'resources': {
                        'megaresfoo': 22.2,
                        'megaresbar': 42,
                    },
                },
                'devname2': {
                    'resources': {
                        'megaresfoo': 11.1,
                        'megaresbar': 2,  # not enough for the tasks
                    },
                },
            },
        })
        config.set_option_noasync('resources', {
            'cpu_count': 5,
        })
        return config

    def _worker_config2(self) -> Optional[Config]:
        config = Config()
        hwid = uuid.uuid4().int
        hwid &= (1 << 63) - 1
        config.set_option_noasync('worker.override_hwid', hwid)
        config.set_option_noasync('devices', {
            'gapauu': {
                '2devname1': {
                    'resources': {
                        'megaresfoo': 22.2,
                        'megaresbar': 42,
                    },
                },
                '2devname2': {
                    'resources': {
                        'megaresfoo': 1.1,  # not enough for the task
                        'megaresbar': 21,
                    },
                },
            },
        })
        config.set_option_noasync('resources', {
            'cpu_count': 5,
        })
        return config


class TestResourceRequirement(ResourceBaseTestCase):
    __test__ = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__reached_full_cpu = False

    @classmethod
    def _initial_db_file(cls) -> str:
        return 'data/test_resources1.db'

    async def _create_test_tasks(self) -> Iterable[int]:
        tasks = [
            *await self._create_task(node_name='TEST IN', attributes={
            }),
        ]
        return tasks

    def _expected_task_attributes(self):
        return {
        }

    def _additional_checks_interval(self) -> float:
        return 0.1

    async def _additional_checks_during_run(self):
        count = 0
        total_cpu = 0.0
        for wid in range(1, 100):  # there should not be more than that
            res = await self.scheduler.data_access.get_invocation_resources_assigned_to(wid)
            if res is None:
                continue
            count += 1
            self.assertIn('cpu_count', res.resources)
            total_cpu += res.resources['cpu_count']

        self.assertLessEqual(total_cpu, 5.0)
        if not self.__reached_full_cpu and total_cpu == 5.0:
            self.__reached_full_cpu = True

    async def _additional_checks_on_finish(self, task_attributes):
        self.assertTrue(self.__reached_full_cpu)
