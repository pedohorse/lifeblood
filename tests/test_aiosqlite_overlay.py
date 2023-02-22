from unittest import IsolatedAsyncioTestCase
from lifeblood.aiosqlite_overlay import ConnectionWithCallbacks, connect


class TestConnectionWithCallbacks(IsolatedAsyncioTestCase):
    async def test_callbacks(self):
        a = []
        async with connect(':memory:') as con:  # type: ConnectionWithCallbacks
            self.assertIsInstance(con, ConnectionWithCallbacks)
            await con.execute('CREATE TABLE "ass" (foo INTEGER NOT NULL PRIMARY KEY, bar TEXT)')
            self.assertListEqual([], a)
            con.add_after_commit_callback(lambda: a.append('a'))
            self.assertListEqual([], a)
            await con.commit()
            self.assertListEqual(['a'], a)
        self.assertListEqual(['a'], a)

    async def test_many_callbacks(self):
        a = []
        b = {}
        async with connect(':memory:') as con:  # type: ConnectionWithCallbacks
            self.assertIsInstance(con, ConnectionWithCallbacks)
            con.add_after_commit_callback(lambda: b.setdefault('b', 'foo'))
            con.add_after_commit_callback(lambda: a.append('a'))
            await con.execute('CREATE TABLE "ass" (foo INTEGER NOT NULL PRIMARY KEY, bar TEXT)')

            con.add_after_commit_callback(lambda: a.append('c'))
            con.add_after_commit_callback(lambda: b.setdefault('d', 'lalo'))
            self.assertDictEqual({}, b)
            self.assertListEqual([], a)
            await con.commit()
        self.assertDictEqual({'b': 'foo', 'd': 'lalo'}, b)
        self.assertListEqual(['a', 'c'], a)
