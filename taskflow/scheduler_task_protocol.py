import struct
import asyncio
import aiofiles

from . import invocationjob

from typing import TYPE_CHECKING, Optional
if TYPE_CHECKING:
    from .scheduler import Scheduler


class SchedulerTaskProtocol(asyncio.StreamReaderProtocol):
    def __init__(self, scheduler: "Scheduler", limit=2**16):
        self.__timeout = 5.0
        self.__reader = asyncio.StreamReader(limit=limit)
        self.__scheduler = scheduler
        super(SchedulerTaskProtocol, self).__init__(self.__reader, self.connection_cb)

    async def connection_cb(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            # TODO: see same todo in worker_task_protocol
            prot = await asyncio.wait_for(reader.readexactly(4), self.__timeout)
            if prot != b'\0\0\0\0':
                raise NotImplementedError()
            command = await asyncio.wait_for(reader.readline(), timeout=self.__timeout)  # type: bytes
            if command.endswith(b'\n'):
                command = command[:-1]
            print(f'scheduler got command: {command.decode("UTF-8")}')
            if command == b'ping':
                writer.write(b'p')
            elif command == b'done':
                tasksize = await reader.readexactly(4)
                tasksize = struct.unpack('>I', tasksize)[0]
                task = await reader.readexactly(tasksize)
                return_code: int = struct.unpack('>I', await reader.readexactly(4))[0]
                task = invocationjob.InvocationJob.deserialize(task)  # TODO: async this
                stdout_size = struct.unpack('>I', await reader.readexactly(4))[0]
                stdout = (await reader.readexactly(stdout_size)).decode('UTF-8')
                stderr_size = struct.unpack('>I', await reader.readexactly(4))[0]
                stderr = (await reader.readexactly(stderr_size)).decode('UTF-8')
                await self.__scheduler.task_done_reported(task, return_code, stdout, stderr)
            elif command == b'hello':
                addrsize = await reader.readexactly(4)
                addrsize = struct.unpack('>I', addrsize)[0]
                addr = await reader.readexactly(addrsize)
                await self.__scheduler.add_worker(addr.decode('UTF-8'))
            elif reader.at_eof():
                print('connection closed')
                return
            else:
                raise NotImplementedError()
            await writer.drain()

        except asyncio.exceptions.TimeoutError as e:
            pass


class SchedulerTaskClient:
    def __init__(self, ip: str, port: int):
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

    async def report_task_done(self, task: invocationjob.InvocationJob, return_code: int, stdout_file: str, stderr_file: str):
        await self._ensure_conn_open()
        self.__writer.writelines([b'done\n'])
        taskserialized = await task.serialize()
        self.__writer.write(struct.pack('>I', len(taskserialized)))
        self.__writer.write(taskserialized)
        self.__writer.write(struct.pack('>I', return_code))
        for std_file in (stdout_file, stderr_file):
            async with aiofiles.open(std_file, 'r') as f:
                filedata = (await f.read()).encode('UTF-8')
                self.__writer.write(struct.pack('>I', len(filedata)))
                self.__writer.write(filedata)
        await self.__writer.drain()
        # do we need a reply? doesn't seem so

    async def ping(self):
        await self._ensure_conn_open()
        self.__writer.write(b'ping\n')
        try:
            await self.__writer.drain()
            retcode = await self.__reader.readexactly(1)
        except ConnectionResetError as e:
            print('ping failed.', e)
        # ignore retcode

    async def say_hello(self, address_to_advertise: str):
        await self._ensure_conn_open()
        self.__writer.write(b'hello\n')
        data = address_to_advertise.encode('UTF-8')
        self.__writer.write(struct.pack('>I', len(data)))
        self.__writer.write(data)
        await self.__writer.drain()
