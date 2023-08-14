import struct
import asyncio
import aiofiles
from enum import Enum
import pickle
import json

from . import logging
from . import invocationjob
from .taskspawn import TaskSpawn
from .enums import WorkerType, SpawnStatus, WorkerState
from .net_classes import WorkerResources

from typing import TYPE_CHECKING, Optional, Tuple
if TYPE_CHECKING:
    from .scheduler import Scheduler


class SchedulerTaskProtocol(asyncio.StreamReaderProtocol):
    def __init__(self, scheduler: "Scheduler", limit=2**16):
        self.__logger = logging.get_logger('scheduler')
        self.__timeout = 300.0
        self.__reader = asyncio.StreamReader(limit=limit)
        self.__scheduler = scheduler
        self.__saved_references = []
        super(SchedulerTaskProtocol, self).__init__(self.__reader, self.connection_cb)

    async def connection_cb(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        # there is a bug in py <=3.8, callback task can be GCd
        # see https://bugs.python.org/issue46309
        # so we HAVE to save a reference to self somewhere
        self.__saved_references.append(asyncio.current_task())

        #
        #
        # commands

        async def comm_ping():  # if command == b'ping'
            # when worker pings scheduler - scheduler returns the state it thinks the worker is in
            addr = await read_string()
            wid = await self.__scheduler.worker_id_from_address(addr)
            if wid is None:
                state = WorkerState.UNKNOWN
            else:
                state = await self.__scheduler.get_worker_state(wid)
            writer.write(struct.pack('>I', state.value))

        async def comm_pulse():  # elif command == b'pulse':
            writer.write(b'\1')

        async def comm__pulse3way_():  # WARNING: this is for tests only!
            writer.write(b'\1')
            await writer.drain()
            await reader.readexactly(1)
            writer.write(b'\2')

        async def comm_done():  # elif command == b'done':
            tasksize = struct.unpack('>Q', await reader.readexactly(8))[0]
            task = await reader.readexactly(tasksize)
            task = await invocationjob.InvocationJob.deserialize_async(task)
            stdout = await read_string()
            stderr = await read_string()
            await self.__scheduler.task_done_reported(task, stdout, stderr)
            writer.write(b'\1')

        async def comm_dropped():  # elif command == b'dropped':
            tasksize = struct.unpack('>Q', await reader.readexactly(8))[0]
            task = await reader.readexactly(tasksize)
            task = await invocationjob.InvocationJob.deserialize_async(task)
            stdout = await read_string()
            stderr = await read_string()
            await self.__scheduler.task_cancel_reported(task, stdout, stderr)
            writer.write(b'\1')

        async def comm_hello():  # elif command == b'hello':
            # worker reports for duty
            addr = await read_string()
            workertype: WorkerType = WorkerType(struct.unpack('>I', await reader.readexactly(4))[0])
            reslength = struct.unpack('>Q', await reader.readexactly(8))[0]
            worker_hardware: WorkerResources = WorkerResources.deserialize(await reader.readexactly(reslength))
            await self.__scheduler.add_worker(addr, workertype, worker_hardware, assume_active=True)
            writer.write(struct.pack('>Q', self.__scheduler.db_uid()))

        async def comm_bye():  # elif command == b'bye':
            # worker reports he's quitting
            addr = await read_string()
            await self.__scheduler.worker_stopped(addr)
            writer.write(b'\1')

        #
        # commands used mostly by lifeblood_connection
        #
        # spawn a child task for task being processed
        async def comm_spawn():  # elif command == b'spawn':
            tasksize = struct.unpack('>Q', await reader.readexactly(8))[0]
            taskspawn: TaskSpawn = TaskSpawn.deserialize(await reader.readexactly(tasksize))
            ret: Tuple[SpawnStatus, Optional[int]] = await self.__scheduler.spawn_tasks(taskspawn)
            writer.write(struct.pack('>I?Q', ret[0].value, ret[1] is not None, 0 if ret[1] is None else ret[1]))

        async def comm_node_name_to_id():  # elif command == b'nodenametoid':
            nodename = await read_string()
            self.__logger.debug(f'got {nodename}')
            ids = await self.__scheduler.node_name_to_id(nodename)
            self.__logger.debug(f'sending {ids}')
            writer.write(struct.pack('>' + 'Q'*(1+len(ids)), len(ids), *ids))

        async def comm_update_task_attributes():  # elif command == b'tupdateattribs':  # note - this one is the same as in scheduler_ui_protocol...
            task_id, update_data_size, strcount = struct.unpack('>QQQ', await reader.readexactly(24))
            attribs_to_update = await asyncio.get_event_loop().run_in_executor(None, pickle.loads, await reader.readexactly(update_data_size))
            attribs_to_delete = set()
            for _ in range(strcount):
                attribs_to_delete.add(await read_string())
            await self.__scheduler.update_task_attributes(task_id, attribs_to_update, attribs_to_delete)
            writer.write(b'\1')

        async def comm_get_task_state():  # elif command == b'gettaskstate':
            task_id = struct.unpack('>Q', await reader.readexactly(8))[0]
            fields_dict = await self.__scheduler.get_task_fields(task_id)
            data = await asyncio.get_event_loop().run_in_executor(None, str.encode,
                                                                  await asyncio.get_event_loop().run_in_executor(None, json.dumps, fields_dict),
                                                                  'UTF-8')
            writer.write(struct.pack('>Q', len(data)))
            writer.write(data)

        async def comm_task_name_to_id():  # elif command == b'tasknametoid':
            taskname = await read_string()
            self.__logger.debug(f'got {taskname}')
            ids = await self.__scheduler.task_name_to_id(taskname)
            self.__logger.debug(f'sending {ids}')
            writer.write(struct.pack('>' + 'Q'*(1+len(ids)), len(ids), *ids))

        #
        commands = {'ping': comm_ping,
                    'pulse': comm_pulse,
                    '_pulse3way_': comm__pulse3way_,  # WARNING: this is for tests only!
                    'done': comm_done,
                    'dropped': comm_dropped,
                    'hello': comm_hello,
                    'bye': comm_bye,
                    'spawn': comm_spawn,
                    'nodenametoid': comm_node_name_to_id,
                    'tupdateattribs': comm_update_task_attributes,
                    'gettaskstate': comm_get_task_state,
                    'tasknametoid': comm_task_name_to_id,
                    }
        #
        #

        async def read_string() -> str:
            strlen = struct.unpack('>Q', await reader.readexactly(8))[0]
            if strlen == 0:
                return ''
            return (await reader.readexactly(strlen)).decode('UTF-8')

        async def write_string(s: str):
            b = s.encode('UTF-8')
            writer.write(struct.pack('>Q', len(b)))
            writer.write(b)

        try:
            # TODO: see same todo in worker_task_protocol
            prot = await asyncio.wait_for(reader.readexactly(4), self.__timeout)
            if prot != b'\0\0\0\0':
                raise NotImplementedError()

            while True:
                try:
                    command: str = await read_string()
                except asyncio.IncompleteReadError:  # no command sent, connection closed
                    self.__logger.debug('connection closed')
                    break
                self.__logger.debug(f'scheduler got command: {command}')

                if command in commands:
                    await commands[command]()

                #
                # if conn is closed - result will be b'', but in mostl likely totally impossible case it can be unfinished command.
                # so lets just catch all
                elif reader.at_eof():
                    self.__logger.debug('connection closed')
                    return
                else:
                    raise NotImplementedError()
                await writer.drain()

        except asyncio.exceptions.TimeoutError as e:
            self.__logger.warning(f'connection timeout happened ({self.__timeout}s).')
        except ConnectionResetError as e:
            self.__logger.exception('connection was reset. disconnected %s', e)
        except ConnectionError as e:
            self.__logger.exception('connection error. disconnected %s', e)
        except Exception as e:
            self.__logger.exception('unknown error. disconnected %s', e)
            raise
        finally:
            writer.close()
            await writer.wait_closed()
            # according to the note in the beginning of the function - now reference can be cleared
            self.__saved_references.remove(asyncio.current_task())


class SchedulerTaskClient:
    def write_string(self, s: str):
        b = s.encode('UTF-8')
        self.__writer.write(struct.pack('>Q', len(b)))
        self.__writer.write(b)

    def __init__(self, ip: str, port: int):
        self.__logger = logging.get_logger('worker')
        self.__conn_task = asyncio.open_connection(ip, port)
        self.__reader = None  # type: Optional[asyncio.StreamReader]
        self.__writer = None  # type: Optional[asyncio.StreamWriter]

    async def __aenter__(self) -> "SchedulerTaskClient":
        await self._ensure_conn_open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        self.__writer.close()
        await self.__writer.wait_closed()

    async def _ensure_conn_open(self):
        if self.__reader is not None:
            return
        self.__reader, self.__writer = await self.__conn_task
        self.__writer.write(b'\0\0\0\0')

    async def report_task_done(self, task: invocationjob.InvocationJob, stdout_file: str, stderr_file: str):
        await self._ensure_conn_open()
        self.write_string('done')
        taskserialized = await task.serialize_async()
        self.__writer.write(struct.pack('>Q', len(taskserialized)))
        self.__writer.write(taskserialized)
        for std_file in (stdout_file, stderr_file):
            async with aiofiles.open(std_file, 'r') as f:
                self.write_string(await f.read())
        await self.__writer.drain()
        # we DO need a reply to ensure proper sequence of events
        assert await self.__reader.readexactly(1) == b'\1'

    async def report_task_canceled(self, task: invocationjob.InvocationJob, stdout_file: str, stderr_file: str):
        await self._ensure_conn_open()
        self.write_string('dropped')
        taskserialized = await task.serialize_async()
        self.__writer.write(struct.pack('>Q', len(taskserialized)))
        self.__writer.write(taskserialized)
        for std_file in (stdout_file, stderr_file):
            async with aiofiles.open(std_file, 'r') as f:
                self.write_string(await f.read())
        await self.__writer.drain()
        # we DO need a reply to ensure proper sequence of events
        assert await self.__reader.readexactly(1) == b'\1'

    async def ping(self, my_address: str) -> WorkerState:
        """
        remind scheduler about worker's existence and get back what he thinks of us

        :param my_address: address of this worker used to register at scheduler
        :return: worker state that scheduler thinks a worker with given address has
        """
        await self._ensure_conn_open()
        self.write_string('ping')
        try:
            self.write_string(my_address)
            await self.__writer.drain()
            return WorkerState(struct.unpack('>I', await self.__reader.readexactly(4))[0])
        except ConnectionResetError as e:
            self.__logger.error('ping failed. %s', e)
            raise

    async def pulse(self) -> None:
        """
        just ping the scheduler and get back a response, check if it's alive
        check pulse sorta

        :return:
        """
        await self._ensure_conn_open()
        self.write_string('pulse')
        try:
            await self.__writer.drain()
            await self.__reader.readexactly(1)
        except ConnectionResetError as e:
            self.__logger.error('pulse check failed. %s', e)
            raise

    async def _pulse3way_(self):
        """
        FOR TESTS ONLY
        """
        await self._ensure_conn_open()
        self.write_string('_pulse3way_')
        await self.__writer.drain()
        yield
        await self.__reader.readexactly(1)
        yield
        self.__writer.write(b'\1')
        await self.__writer.drain()
        await self.__reader.readexactly(1)

    async def say_hello(self, address_to_advertise: str, worker_type: WorkerType, worker_resources: WorkerResources):
        await self._ensure_conn_open()
        self.write_string('hello')
        self.write_string(address_to_advertise)
        self.__writer.write(struct.pack('>I', worker_type.value))
        resdata = worker_resources.serialize()
        self.__writer.write(struct.pack('>Q', len(resdata)))
        self.__writer.write(resdata)
        await self.__writer.drain()
        # as return we get the database's unique id to distinguish it from others
        return struct.unpack('>Q', await self.__reader.readexactly(8))[0]

    async def say_bye(self, address_of_worker: str):
        await self._ensure_conn_open()
        self.write_string('bye')
        self.write_string(address_of_worker)
        await self.__writer.drain()
        # we DO need a reply to ensure proper sequence of events
        assert await self.__reader.readexactly(1) == b'\1'

    async def spawn(self, taskspawn: TaskSpawn) -> Tuple[SpawnStatus, Optional[int]]:
        await self._ensure_conn_open()
        self.write_string('spawn')
        data_ser = await taskspawn.serialize_async()
        self.__writer.write(struct.pack('>Q', len(data_ser)))
        self.__writer.write(data_ser)
        await self.__writer.drain()
        status, is_null, val = struct.unpack('>I?Q', await self.__reader.readexactly(13))
        return SpawnStatus(status), None if is_null else val
