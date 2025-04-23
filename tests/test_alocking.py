import asyncio
from lifeblood.misc import alocking
from unittest import IsolatedAsyncioTestCase


class TestAlocking(IsolatedAsyncioTestCase):

    @alocking('test.lock1')
    async def _meth1(self, obj):
        obj[0] += 1
        await asyncio.sleep(0.2)
        obj[0] += 1
        await asyncio.sleep(0.2)
        obj[0] += 1
        await asyncio.sleep(0.2)

    @alocking('test.lock1')
    async def _meth2(self, obj):
        obj[0] *= 7
        await asyncio.sleep(0.1)
        obj[0] *= 7
        await asyncio.sleep(0.1)
        obj[0] *= 7
        await asyncio.sleep(0.1)

    @alocking('test.lock1')
    async def _meth3(self, obj):
        await asyncio.sleep(0.4)
        obj[0] += 1
        await asyncio.sleep(0.4)
        obj[0] += 1
        await asyncio.sleep(0.4)
        obj[0] += 1

    @alocking('test.lock2')
    async def _meth4(self, obj):
        await asyncio.sleep(0.01)
        obj[0] *= 13
        await asyncio.sleep(0.01)
        obj[0] *= 13
        await asyncio.sleep(0.01)
        obj[0] *= 13

    async def test_locking_simple(self):
        obj = [0]
        await asyncio.gather(self._meth1(obj), self._meth2(obj))

        self.assertEqual(3*7*7*7, obj[0])

    async def test_locking_different_locks(self):
        obj = [1]
        await asyncio.gather(self._meth3(obj), self._meth4(obj))

        self.assertEqual(13*13*13 + 3, obj[0])
