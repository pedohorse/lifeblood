import asyncio
from unittest import IsolatedAsyncioTestCase
from unittest import mock
from pathlib import Path
import sqlite3
from lifeblood.enums import InvocationState, TaskState
from lifeblood.db_misc import sql_init_script
from lifeblood.scheduler.scheduler import Scheduler
from lifeblood.scheduler.pinger import Pinger
from lifeblood.scheduler_message_processor import SchedulerWorkerControlClient
from lifeblood.net_messages.address import AddressChain
from lifeblood.net_messages.impl.tcp_simple_command_message_processor import TcpJsonMessageProcessor
from lifeblood.net_messages.exceptions import MessageTransferError
from lifeblood.scheduler_task_protocol import SchedulerTaskClient
from lifeblood.main_scheduler import create_default_scheduler


def purge_db(recreate=True):
    testdbpath = Path('test_swc1.db')
    if testdbpath.exists():
        testdbpath.unlink()
    if recreate:
        testdbpath.touch()
        with sqlite3.connect('test_swc1.db') as con:
            con.executescript(sql_init_script)


class SchedulerTests(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        purge_db()
        print('settingup done')

    @classmethod
    def tearDownClass(cls) -> None:
        purge_db(recreate=False)
        print('tearingdown done')

    async def test_stopping_normal(self):
        purge_db()
        sched = create_default_scheduler('test_swc1.db', do_broadcasting=False, helpers_minimal_idle_to_ensure=0)
        await sched.start()
        # crudely assert that there are corutines running from scheduler
        self.assertTrue(any([Path(x.get_coro().cr_code.co_filename).parts[-3:-1] == ('lifeblood', 'scheduler') for x in asyncio.all_tasks()]))

        sched.stop()
        self.assertTrue(sched.is_stopping())
        stopping_task = asyncio.create_task(sched.wait_till_stops())
        await asyncio.wait([stopping_task], timeout=16)
        self.assertTrue(sched.is_stopping())
        # crudely assert that there are NO corutines running from scheduler
        self.assertFalse(any([Path(x.get_coro().cr_code.co_filename).parts[-3:-1] == ('lifeblood', 'scheduler') for x in asyncio.all_tasks()]))
        self.assertFalse(any([Path(x.get_coro().cr_code.co_filename).parts[-2:-1] == ('lifeblood',) for x in asyncio.all_tasks()]))

    async def test_stopping_nowait(self):
        """
        tests that scheduler stops even without call to wait_till_stops
        """
        purge_db()
        sched = create_default_scheduler('test_swc1.db', do_broadcasting=False, helpers_minimal_idle_to_ensure=0)
        await sched.start()
        # crudely assert that there are corutines running from scheduler
        self.assertTrue(any([Path(x.get_coro().cr_code.co_filename).parts[-3:-1] == ('lifeblood', 'scheduler') for x in asyncio.all_tasks()]))

        sched.stop()
        self.assertTrue(sched.is_stopping())
        # no wait_stopped()
        await asyncio.sleep(5)  # sleep for a reasonable time to ensure everything stopps
        # crudely assert that there are NO corutines running from scheduler
        self.assertFalse(any([Path(x.get_coro().cr_code.co_filename).parts[-3:-1] == ('lifeblood', 'scheduler') for x in asyncio.all_tasks()]))
        self.assertFalse(any([Path(x.get_coro().cr_code.co_filename).parts[-2:-1] == ('lifeblood',) for x in asyncio.all_tasks()]))

    async def test_message_connection_when_stopping1(self):
        await self._helper_test_connection_when_stopping(try_open_new=False)

    async def test_message_connection_when_stopping2(self):
        await self._helper_test_connection_when_stopping(try_open_new=True)

    async def test_connection_when_stopping1(self):
        await self._helper_test_nonmessage_connection_when_stopping(try_open_new=False)

    async def test_connection_when_stopping2(self):
        await self._helper_test_nonmessage_connection_when_stopping(try_open_new=True)

    async def __send_pulse(self, processor):
        with processor.message_client(AddressChain('127.0.0.1:11848')) as client:
            await client.send_message_as_json({
                'command': {  # WARNING: duplicating pulse implementation here
                    'name': 'pulse',
                    'arguments': {}
                }
            })
            reply = await client.receive_message(timeout=5)

    async def _helper_test_connection_when_stopping(self, try_open_new=False):
        """
        tests that a messaging session that was opened before stopping scheduler
        will be allowed to finish after stop() is called
        if try_open_new is True - a new session will be tried after stop() and before the other session is finished
        and it is expected to fail to be opened
        """
        purge_db()
        sched = create_default_scheduler('test_swc1.db', do_broadcasting=False, helpers_minimal_idle_to_ensure=0, server_addr=('127.0.0.1', 11847, 11848))
        await sched.start()
        proc = TcpJsonMessageProcessor(('127.0.0.1', 11850))
        await proc.start()

        with proc.message_client(AddressChain('127.0.0.1:11848')) as client:
            await client.send_message_as_json({
                'command': {  # WARNING: duplicating pulse implementation here
                    'name': '_pulse3way_',
                    'arguments': {}
                }
            })

            # now stop scheduler
            sched.stop()
            waiter_task = asyncio.create_task(sched.wait_till_stops())
            await asyncio.sleep(2)
            self.assertFalse(waiter_task.done())  # wait should not be done while we still process something

            if try_open_new:
                _good = False
                try:
                    await self.__send_pulse(proc)
                except MessageTransferError:
                    _good = True
                self.assertTrue(_good)

            reply = await client.receive_message(timeout=5)
            await client.send_message_as_json({})
            reply = await client.receive_message(timeout=5)
            self.assertTrue((await reply.message_body_as_json()).get('phase', 2), 'something is not ok')

        await asyncio.wait([waiter_task], timeout=5)
        self.assertFalse(any([Path(x.get_coro().cr_code.co_filename).parts[-3:-1] == ('lifeblood', 'scheduler') for x in asyncio.all_tasks()]))
        self.assertFalse(any([Path(x.get_coro().cr_code.co_filename).parts[-2:-1] == ('lifeblood',) for x in asyncio.all_tasks()]))
        proc.stop()
        await proc.wait_till_stops()

    async def _helper_test_nonmessage_connection_when_stopping(self, try_open_new=False):
        """
        tests that a non-messaging session that was opened before stopping scheduler
        will be allowed to finish after stop() is called
        if try_open_new is True - a new session will be tried after stop() and before the other session is finished
        and it is expected to fail to be opened
        """
        purge_db()
        sched = create_default_scheduler('test_swc1.db', do_broadcasting=False, helpers_minimal_idle_to_ensure=0, server_addr=('127.0.0.1', 11847, 11848))
        await sched.start()

        async with SchedulerTaskClient('127.0.0.1', 11847) as client:

            gen = client._pulse3way_()
            await gen.__anext__()

            # now stop scheduler
            sched.stop()
            waiter_task = asyncio.create_task(sched.wait_till_stops())
            await asyncio.sleep(2)
            self.assertFalse(waiter_task.done())  # wait should not be done while we still process something

            if try_open_new:
                _good = False
                try:
                    async with SchedulerTaskClient('127.0.0.1', 11847) as client1:
                        await client1.pulse()
                except ConnectionError:
                    _good = True
                self.assertTrue(_good)

            await gen.__anext__()

            _good = False
            try:
                await gen.__anext__()
            except StopAsyncIteration:
                _good = True
            self.assertTrue(_good)

        await asyncio.wait([waiter_task], timeout=5)
        self.assertFalse(any([Path(x.get_coro().cr_code.co_filename).parts[-3:-1] == ('lifeblood', 'scheduler') for x in asyncio.all_tasks()]))
        self.assertFalse(any([Path(x.get_coro().cr_code.co_filename).parts[-2:-1] == ('lifeblood',) for x in asyncio.all_tasks()]))

    async def test_get_invocation_workers(self):
        purge_db()

        with mock.patch('lifeblood.scheduler.scheduler.Pinger') as ppatch:
            ppatch.return_value = mock.AsyncMock(Pinger)

            sched = create_default_scheduler('test_swc1.db', do_broadcasting=False, helpers_minimal_idle_to_ensure=0)
            await sched.start()

            with sqlite3.connect('test_swc1.db') as con:
                con.execute('INSERT INTO nodes (type, name, node_object) VALUES ("null", "test", NULL)')
                con.execute('INSERT INTO tasks (node_id, name, state) VALUES (1, "foo", ?)', (TaskState.IN_PROGRESS.value,))
                con.execute('INSERT INTO workers (last_address, ping_state, state, hwid) VALUES ("1.2.3.4:5678", 1, 2, 123456)')
                con.execute('INSERT INTO workers (last_address, ping_state, state, hwid) VALUES ("2.3.4.5:6789", 1, 2, 123457)')
                con.execute('INSERT INTO workers (last_address, ping_state, state, hwid) VALUES ("3.4.5.6:7890", 1, 2, 123457)')
                con.execute('INSERT INTO invocations (task_id, worker_id, node_id, state) VALUES (1, 2, 1, ?)', (InvocationState.IN_PROGRESS.value,))
                con.execute('INSERT INTO invocations (task_id, worker_id, node_id, state) VALUES (1, 3, 1, ?)', (InvocationState.FINISHED.value,))
                con.execute('INSERT INTO invocations (task_id, worker_id, node_id, state) VALUES (1, 1, 1, ?)', (InvocationState.INVOKING.value,))
                con.commit()
            try:

                self.assertEqual(InvocationState.IN_PROGRESS, await sched.get_invocation_state(1))
                self.assertEqual(InvocationState.FINISHED, await sched.get_invocation_state(2))
                self.assertEqual(InvocationState.INVOKING, await sched.get_invocation_state(3))

                self.assertEqual("2.3.4.5:6789", await sched.get_invocation_worker(1))
                self.assertEqual("3.4.5.6:7890", await sched.get_invocation_worker(2))
                self.assertEqual("1.2.3.4:5678", await sched.get_invocation_worker(3))
            finally:
                sched.stop()
                await sched.wait_till_stops()
            