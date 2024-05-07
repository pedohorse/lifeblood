import asyncio
from unittest import IsolatedAsyncioTestCase
import pickle
from lifeblood.component_base import ComponentBase


class ComponentSimple(ComponentBase):
    def __init__(self):
        super().__init__()
        self.a = 123
        self.b = [2, 3, 4]
        self.b1 = self.b
        self.c = {1: 'a', 2: 'b'}


class ComponentSimpleWithAio(ComponentSimple):
    def __init__(self):
        super().__init__()
        self.ev1 = asyncio.Event()
        self.ev2 = asyncio.Event()
        self.ev1a = self.ev1
        self.ev2a = self.ev2

        self.l1 = asyncio.Lock()
        self.l2 = asyncio.Lock()
        self.l1a = self.l1
        self.l2a = self.l2


class TestComponentBasePickling(IsolatedAsyncioTestCase):
    async def test_simple(self, c=None):
        if c is None:
            c = ComponentSimple()
        c.a = 234
        c.b.append(11)
        c.c[3] = 'c'

        c1 = pickle.loads(pickle.dumps(c))

        # check that we have not changed c itself
        self.assertEqual(234, c.a)
        self.assertListEqual([2, 3, 4, 11], c.b)
        self.assertDictEqual({1: 'a', 2: 'b', 3: 'c'}, c.c)

        self.assertEqual(c.a, c1.a)
        self.assertEqual(c.b, c1.b)
        self.assertEqual(c.b1, c1.b1)
        self.assertEqual(c.c, c1.c)
        self.assertIs(c1.b, c1.b1)

    async def test_events_locks(self):
        c = ComponentSimpleWithAio()
        c.ev2.set()
        await c.l2.acquire()
        self.assertFalse(c.l1.locked())
        self.assertFalse(c.l1a.locked())
        self.assertTrue(c.l2.locked())
        self.assertTrue(c.l2a.locked())

        await self.test_simple(c)

        c1 = pickle.loads(pickle.dumps(c))

        # check that we have not changed c itself

        self.assertIsInstance(c1.ev1, asyncio.Event)
        self.assertIsInstance(c1.ev2, asyncio.Event)
        self.assertIsInstance(c1.ev1a, asyncio.Event)
        self.assertIsInstance(c1.ev2a, asyncio.Event)
        self.assertIsInstance(c1.l1, asyncio.Lock)
        self.assertIsInstance(c1.l2, asyncio.Lock)
        self.assertIsInstance(c1.l1a, asyncio.Lock)
        self.assertIsInstance(c1.l2a, asyncio.Lock)

        self.assertFalse(c1.l1.locked())
        self.assertFalse(c1.l1a.locked())
        self.assertTrue(c1.l2.locked())
        self.assertTrue(c1.l2a.locked())
        self.assertFalse(c1.ev1.is_set())
        self.assertFalse(c1.ev1a.is_set())
        self.assertTrue(c1.ev2.is_set())
        self.assertTrue(c1.ev2a.is_set())
        self.assertIs(c1.l1, c1.l1a)
        self.assertIs(c1.l2, c1.l2a)
