import os
import asyncio
import aiosqlite
import random
import tempfile
from unittest import IsolatedAsyncioTestCase
from taskflow.shared_lazy_sqlite_connection import SharedLazyAiosqliteConnection, ConnectionPool
from taskflow.logging import get_logger


class SharedAsyncSqliteConnectionTest(IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        get_logger('shared_aiosqlite_connection').setLevel('DEBUG')

    async def test_one(self):
        fd, dbpath = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        # dbpath = 'file::memory:?cache=shared'
        async with aiosqlite.connect(dbpath, timeout=15) as con:
            con.row_factory = aiosqlite.Row
            await con.executescript('''
            CREATE TABLE test (
            "id"    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            some_field_text  TEXT,
            some_field_int  INT
            );
            PRAGMA journal_mode=wal;
            ''')
            await con.execute("INSERT INTO test (some_field_text, some_field_int) VALUES ('ass', 123)")
            await con.execute("INSERT INTO test (some_field_text, some_field_int) VALUES ('bob', 234)")
            await con.execute("INSERT INTO test (some_field_text, some_field_int) VALUES ('veg', 345)")
            await con.commit()
            # and we keep this connection opened till the end, or mem db will be dropped

            _consistency_checker_stop_event = asyncio.Event()
            async def _consistency_checker():  # we will be activelly checking that no conuser was committed halfway between it's two inserts
                while not _consistency_checker_stop_event.is_set():
                    await asyncio.sleep(0.01)
                    async with aiosqlite.connect(dbpath, timeout=15) as con:
                        con.row_factory = aiosqlite.Row
                        async with con.execute('SELECT * FROM test') as cur:
                            rows = await cur.fetchall()
                    #print('\n'.join(str(dict(x)) for x in rows))
                    #print('==')
                    starts = set()
                    ends = set()
                    for row in rows:
                        if not row['some_field_text'].startswith('s_'):
                            continue
                        self.assertNotIn(row['some_field_text'], starts)
                        starts.add(row['some_field_text'])
                    for row in rows:
                        if not row['some_field_text'].startswith('end_s_'):
                            continue
                        self.assertNotIn(row['some_field_text'], ends)
                        ends.add(row['some_field_text'])
                    for start in starts:
                        self.assertIn(f'end_{start}', ends)
                    for end in ends:
                        self.assertIn(f'{end[4:]}', starts)

            rand = random.Random(6661313)
            cnt = 25
            names = ('alice', 'bob', 'vitalik')
            check_task = asyncio.create_task(_consistency_checker())
            await asyncio.gather(*[self.conuser(x, rand.uniform(0, 1), name, dbpath) for x in range(cnt) for name in names])
            _consistency_checker_stop_event.set()
            print('stoppen!')
            await check_task

            pool = SharedLazyAiosqliteConnection.connection_pool
            await asyncio.sleep(pool.keep_open_period * 1.1)
            self.assertEqual(0, len(pool.connection_cache))

            for name in names:
                for i in range(15):
                    async with con.execute('SELECT * FROM test WHERE some_field_text == ?', (f's_{name}_{i}',)) as cur:
                        rows = await cur.fetchall()
                    self.assertEqual(1, len(rows))
                    row = rows[0]
                    self.assertEqual(f's_{name}_{i}', row['some_field_text'])
                    self.assertEqual(i, row['some_field_int'])
                    async with con.execute('SELECT * FROM test WHERE some_field_text == ?', (f'end_s_{name}_{i}',)) as cur:
                        rows = await cur.fetchall()
                    self.assertEqual(1, len(rows))
                    row = rows[0]
                    self.assertEqual(f'end_s_{name}_{i}', row['some_field_text'])
                    self.assertEqual(i, row['some_field_int'])
                    async with con.execute('SELECT COUNT(*) as cnt FROM test') as cur:
                        self.assertEqual(len(names)*cnt*2+3, (await cur.fetchone())['cnt'])

    async def conuser(self, i, delay, conname, dbpath):
        print(f'{conname}: {i} start')
        await asyncio.sleep(delay)
        async with SharedLazyAiosqliteConnection(None, dbpath, conname, timeout=15) as con:
            print(f'{conname}: {i} connec')
            await asyncio.sleep(delay)
            await con.execute('INSERT INTO test (some_field_text, some_field_int) VALUES (?, ?)', (f's_{conname}_{i}', i))
            print(f'{conname}: {i} inser1')
            await asyncio.sleep(delay)
            await con.execute('INSERT INTO test (some_field_text, some_field_int) VALUES (?, ?)', (f'end_s_{conname}_{i}', i))
            print(f'{conname}: {i} inser2')
            await con.commit()
            print(f'{conname}: {i} commit')
