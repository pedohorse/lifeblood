import asyncio
import random
import threading
from unittest import IsolatedAsyncioTestCase
from lifeblood.ui_events import TasksChanged
from lifeblood.ui_protocol_data import TaskDelta
from lifeblood.nethelpers import get_localhost
from lifeblood.enums import TaskState


class UiEventsTest(IsolatedAsyncioTestCase):  # TODO: cannot replicate that very rare "BufferError: Existing exports of data: object cannot be re-sized error" :(
    __saved_references = []
    _rng = random.Random(66613)

    async def _callback(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.__saved_references.append(asyncio.current_task())
        rng_i = threading.current_thread().native_id % 1000

        try:
            print('in')
            event = TasksChanged(123456, [TaskDelta(666 + i,
                                                    active_children_count=(rng_i + i*137) % 100,
                                                    state=list(TaskState)[(rng_i + i*137) % len(TaskState)])
                                          for i in range(self._rng.randint(0, 200))])
            last_print_thres = 0
            for i in range(300):
                if i > last_print_thres:
                    print(i)
                    last_print_thres += 100
                event.event_id = i
                await asyncio.sleep(0.00001)
                writer.write(b'1234567890abcdef'*self._rng.randint(1, 32))
                await asyncio.sleep(0.00001)
                await event.serialize_to_streamwriter(writer)
            await writer.drain()
        except Exception as e:
            print(e)
            raise
        finally:
            writer.close()
            await writer.wait_closed()

            self.__saved_references.remove(asyncio.current_task())

    async def test_drown(self):
        async def _drain(host, port):
            reader, writer = await asyncio.open_connection(host, port)
            total_read = 0
            while not reader.at_eof():
                total_read += len(await reader.read(77 * 1024))
                # print(f'read {total_read}')
                await asyncio.sleep(0.01)

            print(f'done: {total_read}')

        host = get_localhost()
        port = 29837
        server0 = await asyncio.start_server(self._callback, host, port)
        server1 = await asyncio.start_server(self._callback, host, port+1)
        server2 = await asyncio.start_server(self._callback, host, port+2)
        try:
            await asyncio.gather(_drain(host, port),
                                 _drain(host, port+1),
                                 _drain(host, port+2))
        finally:
            server0.close()
            server1.close()
            server2.close()
            await server0.wait_closed()
            await server1.wait_closed()
            await server2.wait_closed()

