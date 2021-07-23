import struct
import asyncio
import aiofiles
from enum import Enum

from . import logging
from . import invocationjob
from .taskspawn import TaskSpawn

from typing import TYPE_CHECKING, Optional
if TYPE_CHECKING:
    from .scheduler import Scheduler


class SpawnStatus(Enum):
    SUCCEEDED = 0
    FAILED = 1


class SchedulerTaskProtocol(asyncio.StreamReaderProtocol):
    def __init__(self, scheduler: "Scheduler", limit=2**16):
        self.__logger = logging.getLogger('scheduler')
        self.__timeout = 5.0
        self.__reader = asyncio.StreamReader(limit=limit)
        self.__scheduler = scheduler
        super(SchedulerTaskProtocol, self).__init__(self.__reader, self.connection_cb)

    async def connection_cb(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        async def read_string() -> str:
            strlen = struct.unpack('>Q', await reader.readexactly(8))[0]
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
                command = await asyncio.wait_for(reader.readline(), timeout=self.__timeout)  # type: bytes
                if command.endswith(b'\n'):
                    command = command[:-1]
                self.__logger.debug(f'scheduler got command: {command.decode("UTF-8")}')
                if command == b'ping':
                    writer.write(b'p')
                elif command == b'done':
                    tasksize = struct.unpack('>Q', await reader.readexactly(8))[0]
                    task = await reader.readexactly(tasksize)
                    task = await invocationjob.InvocationJob.deserialize_async(task)
                    stdout = await read_string()
                    stderr = await read_string()
                    await self.__scheduler.task_done_reported(task, stdout, stderr)
                elif command == b'dropped':
                    tasksize = struct.unpack('>Q', await reader.readexactly(8))[0]
                    task = await reader.readexactly(tasksize)
                    task = await invocationjob.InvocationJob.deserialize_async(task)
                    stdout = await read_string()
                    stderr = await read_string()
                    await self.__scheduler.task_cancel_reported(task, stdout, stderr)
                elif command == b'hello':
                    addrsize = await reader.readexactly(4)
                    addrsize = struct.unpack('>I', addrsize)[0]
                    addr = await reader.readexactly(addrsize)
                    await self.__scheduler.add_worker(addr.decode('UTF-8'), assume_active=True)
                #
                # spawn a child task for task being processed
                elif command == b'spawn':
                    tasksize = struct.unpack('>Q', await reader.readexactly(8))[0]
                    taskspawn: TaskSpawn = TaskSpawn.deserialize(await reader.readexactly(tasksize))
                    ret: SpawnStatus = await self.__scheduler.spawn_tasks([taskspawn])
                    writer.write(struct.pack('>I', ret.value))
                #
                elif command == b'nodenametoid':
                    nodename = await read_string()
                    self.__logger.debug(f'got {nodename}')
                    ids = await self.__scheduler.node_name_to_id(nodename)
                    self.__logger.debug(f'sending {ids}')
                    writer.write(struct.pack('>' + 'Q'*(1+len(ids)), len(ids), *ids))
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
            pass
        except ConnectionResetError as e:
            self.__logger.exception('connection was reset. disconnected %s', e)
        except ConnectionError as e:
            self.__logger.exception('connection error. disconnected %s', e)
        except Exception as e:
            self.__logger.exception('unknown error. disconnected %s', e)
            raise


class SchedulerTaskClient:
    async def write_string(self, s: str):
        b = s.encode('UTF-8')
        self.__writer.write(struct.pack('>Q', len(b)))
        self.__writer.write(b)

    def __init__(self, ip: str, port: int):
        self.__logger = logging.getLogger('worker')
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
        self.__writer.write(b'done\n')
        taskserialized = await task.serialize_async()
        self.__writer.write(struct.pack('>Q', len(taskserialized)))
        self.__writer.write(taskserialized)
        for std_file in (stdout_file, stderr_file):
            async with aiofiles.open(std_file, 'r') as f:
                await self.write_string(await f.read())
        await self.__writer.drain()
        # do we need a reply? doesn't seem so

    async def report_task_canceled(self, task: invocationjob.InvocationJob, stdout_file: str, stderr_file: str):
        await self._ensure_conn_open()
        self.__writer.write(b'dropped\n')
        taskserialized = await task.serialize_async()
        self.__writer.write(struct.pack('>Q', len(taskserialized)))
        self.__writer.write(taskserialized)
        for std_file in (stdout_file, stderr_file):
            async with aiofiles.open(std_file, 'r') as f:
                await self.write_string(await f.read())
        await self.__writer.drain()
        # do we need a reply? doesn't seem so

    async def ping(self):
        await self._ensure_conn_open()
        self.__writer.write(b'ping\n')
        try:
            await self.__writer.drain()
            retcode = await self.__reader.readexactly(1)
        except ConnectionResetError as e:
            self.__logger.error('ping failed. %s', e)
        # ignore retcode

    async def say_hello(self, address_to_advertise: str):
        await self._ensure_conn_open()
        self.__writer.write(b'hello\n')
        data = address_to_advertise.encode('UTF-8')
        self.__writer.write(struct.pack('>I', len(data)))
        self.__writer.write(data)
        await self.__writer.drain()

    async def spawn(self, taskspawn: TaskSpawn) -> SpawnStatus:
        await self._ensure_conn_open()
        self.__writer.write(b'spawn\n')
        data_ser = await taskspawn.serialize_async()
        self.__writer.write(struct.pack('>Q', len(data_ser)))
        self.__writer.write(data_ser)
        await self.__writer.drain()
        return SpawnStatus(struct.unpack('>I', await self.__reader.readexactly(4)))
