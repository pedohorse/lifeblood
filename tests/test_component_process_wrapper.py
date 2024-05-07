import os
import asyncio
import time
from unittest import IsolatedAsyncioTestCase
import tempfile
from lifeblood.component_base import ComponentBase
from lifeblood.component_process_wrapper import ComponentProcessWrapper


class ComponentTestSomething(ComponentBase):
    def __init__(self, a, b, fpath):
        super().__init__()
        self._a = a
        self._b = b
        self._fpath = fpath

    async def _main_task(self):
        self._main_task_is_ready_now()
        with open(self._fpath, 'w') as f:
            while not self._stop_event.is_set():
                await asyncio.sleep(1)
                f.write(f'fee {self._a}  {self._b}\n')
        print('exiting')


class ComponentTestSomethingElse(ComponentBase):
    def __init__(self, a, b, fpath):
        super().__init__()
        self._a = a
        self._b = b
        self._fpath = fpath

    async def _main_task(self):
        self._main_task_is_ready_now()
        with open(self._fpath, 'w') as f:
            for i in range(10):
                await asyncio.sleep(0.2)
                f.write(f'{i} fee {self._a}  {self._b}\n')
        print('exiting')


class TestComponentProcessWrapper(IsolatedAsyncioTestCase):
    async def test_basic(self):
        fd, path = tempfile.mkstemp('.txt', 'unittests_')
        os.close(fd)
        a, b = 13, 24
        wrapper = ComponentProcessWrapper(ComponentTestSomethingElse(a, b, path))
        await wrapper.start()
        # no stop
        await wrapper.wait_till_stops()
        with open(path, 'r') as f:
            lines = f.readlines()
        self.assertEqual(10, len(lines))
        for i, line in enumerate(lines):
            self.assertEqual(f'{i} fee {a}  {b}\n', line)

    async def test_stop(self):
        fd, path = tempfile.mkstemp('.txt', 'unittests_')
        os.close(fd)
        a, b = 13, 24
        wrapper = ComponentProcessWrapper(ComponentTestSomething(a, b, path))
        await wrapper.start()
        time.sleep(3.5)  # non-asyncio sleep to make sure blocking this thread won't affect component wrapper
        wrapper.stop()
        await wrapper.wait_till_stops()
        with open(path, 'r') as f:
            lines = f.readlines()
        self.assertLess(3, len(lines))
        for line in lines:
            self.assertEqual(f'fee {a}  {b}\n', line)
