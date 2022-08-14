from lifeblood.process_utils import create_process, kill_process_tree
from lifeblood.logging import get_logger
from unittest import IsolatedAsyncioTestCase
import asyncio
import os
import sys
import psutil


class ProcessTestCase(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        get_logger('process management').setLevel('DEBUG')

    async def test_simple_process_creation(self):
        proc = await create_process(['echo'], dict(os.environ), stdout=None, stderr=None)
        self.assertLess(0, proc.pid)
        await proc.wait()
        self.assertLess(0, proc.pid)
        self.assertEqual(0, proc.returncode)

    async def test_simple_kill(self):
        # this is the most OS-independent way of sleeping
        proc = await create_process([sys.executable, '-c', 'import time;time.sleep(10)'], dict(os.environ), stdout=None, stderr=None)
        self.assertLess(0, proc.pid)
        self.assertIsNone(proc.returncode)
        await asyncio.sleep(1)
        self.assertIsNone(proc.returncode)
        await kill_process_tree(proc)
        self.assertIsNotNone(proc.returncode)
        self.assertNotEqual(0, proc.returncode)

    async def test_kill_tree(self):
        template = f'import time, subprocess, os\n' \
                   f'pids = str(os.getpid()).encode()\n' \
                   f'for _ in range({{}}):\n' \
                   f'    p = subprocess.Popen([{repr(sys.executable)}, "-c", {{}}], stdout=subprocess.PIPE)\n' \
                   f'    pids += b"," + p.stdout.readline()[:-1]\n' \
                   f'print(pids.decode())\n' \
                   f'time.sleep(10)\n'
        script = template.format(0, '"pass"')
        for i in range(1, 4):
            script = template.format(i, repr(script))

        proc = await create_process([sys.executable, '-c', script], dict(os.environ), stdout=asyncio.subprocess.PIPE, stderr=None)
        #for _ in range(16):  # cuz (((1+1)*2+1)*3+1)
        line = await proc.stdout.readline()
        print(line)
        pids = [int(x) for x in line.split(b',')]
        print(pids)
        await asyncio.sleep(1)
        await kill_process_tree(proc)
        for pid in pids:
            try:
                self.assertFalse(psutil.Process(pid).is_running())  # in case it's zombie
            except Exception as e:
                self.assertTrue(isinstance(e, psutil.NoSuchProcess))

    async def test_kill_orphaned_tree(self):
        template = f'import time, subprocess, os\n' \
                   f'pids = str(os.getpid()).encode()\n' \
                   f'i = {{}}\n' \
                   f'for _ in range(i):\n' \
                   f'    p = subprocess.Popen([{repr(sys.executable)}, "-c", {{}}], stdout=subprocess.PIPE)\n' \
                   f'    pids += b"," + p.stdout.readline()[:-1]\n' \
                   f'print(pids.decode())\n' \
                   f'time.sleep(9 - i*3)\n'  # note that this way parents will be dying before children
        script = template.format(0, '"pass"')
        for i in range(1, 4):
            script = template.format(i, repr(script))

        proc = await create_process([sys.executable, '-c', script], dict(os.environ), stdout=asyncio.subprocess.PIPE, stderr=None)

        line = await proc.stdout.readline()
        print(line)
        pids = [int(x) for x in line.split(b',')]
        print(pids)
        await asyncio.sleep(4)
        await kill_process_tree(proc)
        await asyncio.sleep(1.5)  # we wait for group sending signal to reach the processes
        for pid in pids:
            try:
                self.assertFalse(psutil.Process(pid).is_running())  # in case it's zombie
            except Exception as e:
                self.assertTrue(isinstance(e, psutil.NoSuchProcess))
