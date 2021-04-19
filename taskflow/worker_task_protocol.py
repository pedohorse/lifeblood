import asyncio
import aiofiles
from enum import Enum
import struct
from . import invocationjob
from . import nethelpers

import os

from typing import TYPE_CHECKING, Optional
if TYPE_CHECKING:
    from .worker import Worker


class TaskScheduleStatus(Enum):
    SUCCESS = 0
    FAILED = 1
    BUSY = 2
    EMPTY = 3


class TaskExecutionStatus(Enum):
    FINISHED = 0
    RUNNING = 1


class WorkerPingReply(Enum):
    IDLE = 0
    BUSY = 1


class AlreadyRunning(RuntimeError):
    pass


class WorkerTaskServerProtocol(asyncio.StreamReaderProtocol):

    def __init__(self, worker: "Worker", limit=2 ** 16):
        self.__timeout = 5.0
        self.__reader = asyncio.StreamReader(limit=limit)
        self.__worker = worker
        super(WorkerTaskServerProtocol, self).__init__(self.__reader, self.client_connected_cb)

    async def client_connected_cb(self, reader, writer):
        async def read_string() -> str:
            strlen = struct.unpack('>Q', await reader.readexactly(8))[0]
            return (await reader.readexactly(strlen)).decode('UTF-8')

        async def write_string(s: str):
            b = s.encode('UTF-8')
            writer.write(struct.pack('>Q', len(b)))
            writer.write(b)

        try:
            prot = await asyncio.wait_for(reader.readexactly(4), timeout=self.__timeout)
            if prot == b'\0\0\0\0':
                # TODO: either we need timeout here - then add it to all reading operations below
                # TODO: or we don't need it here - then remove wait.
                command = await asyncio.wait_for(reader.readline(), timeout=self.__timeout)  # type: bytes
                if command.endswith(b'\n'):
                    command = command[:-1]
                print(f'got command: {command.decode("UTF-8")}')

                #
                # command ping
                if command == b'ping':  # routine ping, report worker status
                    if self.__worker.is_task_running():
                        pstatus = WorkerPingReply.BUSY.value
                        pvalue = int(await self.__worker.task_status() or 0)
                    else:
                        pstatus = WorkerPingReply.IDLE.value
                        pvalue = 0
                    writer.write(struct.pack('>Bl', pstatus, pvalue))
                    await asyncio.wait_for(writer.drain(), timeout=self.__timeout)
                #
                # command enqueue task
                elif command == b'task':  # scheduler wants us to pick up task
                    tasksize = await reader.readexactly(4)
                    tasksize = struct.unpack('>I', tasksize)[0]
                    task = await reader.readexactly(tasksize)
                    addrsize = await reader.readexactly(4)
                    addrsize = struct.unpack('>I', addrsize)[0]
                    addr = await reader.readexactly(addrsize)
                    task = invocationjob.InvocationJob.deserialize(task)
                    addr = addr.decode('UTF-8')
                    print(f'got task: {task}, reply result to {addr}')
                    try:
                        print('taking the task')
                        await self.__worker.run_task(task, addr)
                        writer.write(bytes([TaskScheduleStatus.SUCCESS.value]))
                    except AlreadyRunning:
                        print('BUSY. rejecting task')
                        writer.write(bytes([TaskScheduleStatus.BUSY.value]))
                    except Exception as e:
                        print('no, cuz', e)
                        writer.write(bytes([TaskScheduleStatus.FAILED.value]))
                #
                # command drop current task
                elif command == b'drop':  # scheduler wants us to drop current task
                    try:
                        await self.__worker.stop_task()
                    except:
                        writer.write(bytes([TaskScheduleStatus.FAILED.value]))
                #
                # command check worker status
                elif command == b'status':  # scheduler wants to know the status of current task
                    raise NotImplementedError()
                #
                # command to get worker's logs
                elif command == b'log':
                    invocation_id = struct.unpack('>Q', await reader.readexactly(8))[0]
                    for logfilepath in (self.__worker.get_log_filepath('output', invocation_id),
                                        self.__worker.get_log_filepath('error', invocation_id)):
                        if not os.path.exists(logfilepath):
                            writer.write(struct.pack('>I', 0))
                            continue
                        async with aiofiles.open(logfilepath, 'r') as f:
                            all_data = (await f.read()).encode('UTF-8')
                            writer.write(struct.pack('>I', len(all_data)))
                            writer.write(all_data)
                    await writer.drain()
                #
                # command get worker's log line by line
                elif command == b'logiter':  # scheduler wants to have the log:
                    raise NotImplementedError()
                    # TODO: redo this using line feeder from nethelpers! also pass invocation id
                    logname = await asyncio.wait_for(reader.readline(), timeout=self.__timeout)
                    if logname.endswith(b'\n'):
                        logname = logname[:-1]
                    logname = logname.decode('UTF-8')
                    logfilepath = self.__worker.get_log_filepath(logname)
                    async with aiofiles.open(logfilepath, 'r') as f:
                        async for line in f:
                            writer.write(b'\x01')
                            writer.write(line)
                        writer.write(b'\x00')
                    await writer.drain()

                # now wait for answer to be delivered
                await writer.drain()
            else:
                raise NotImplementedError()
        except asyncio.TimeoutError:
            print('operation timeout')
            raise
        except EOFError:
            print('connection was abruptly closed')
            raise


class WorkerTaskClient:
    def __init__(self, ip: str, port: int, timeout=30.0):
        self.__conn_task = asyncio.create_task(asyncio.open_connection(ip, port))
        self.__reader = None  # type: Optional[asyncio.StreamReader]
        self.__writer = None  # type: Optional[asyncio.StreamWriter]
        self.__timeout = timeout

    async def __aenter__(self) -> "WorkerTaskClient":
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

    async def ping(self):
        await self._ensure_conn_open()
        self.__writer.write(b'ping\n')
        try:
            await asyncio.wait_for(self.__writer.drain(), self.__timeout)
            pstatus, pvalue = struct.unpack('>Bl', await asyncio.wait_for(self.__reader.readexactly(5), self.__timeout))
            return WorkerPingReply(pstatus), pvalue
        except asyncio.exceptions.TimeoutError:
            print('network error')
            raise

    async def give_task(self, task: invocationjob.InvocationJob, reply_address):
        await self._ensure_conn_open()
        self.__writer.writelines([b'task\n'])
        taskserialized = await task.serialize_async()
        self.__writer.write(struct.pack('>I', len(taskserialized)))
        self.__writer.write(taskserialized)
        # now send where to respond
        # send URI-style link, dont limit urselves to ip
        if isinstance(reply_address, str):
            reply_address = reply_address.encode('UTF-8')
        self.__writer.write(struct.pack('>I', len(reply_address)))
        self.__writer.write(reply_address)
        await self.__writer.drain()
        reply = TaskScheduleStatus(ord(await self.__reader.readexactly(1)))
        return reply

    async def status(self):
        raise NotImplementedError()

    async def get_log(self, invocation_id) -> (str, str):
        self.__writer.write(b'log\n')
        self.__writer.write(struct.pack('>Q', invocation_id))
        await self.__writer.drain()
        stdoutsize = struct.unpack('>I', await self.__reader.readexactly(4))[0]
        stdout = await self.__reader.readexactly(stdoutsize)
        stderrsize = struct.unpack('>I', await self.__reader.readexactly(4))[0]
        stderr = await self.__reader.readexactly(stderrsize)
        return stdout.decode('UTF-8'), stderr.decode('UTF-8')

    async def get_log_iterate(self, logname: str, since=None):  # TODO: type it
        """
        get execution log from the worker
        :param logname: standard lognames are 'out' 'error' and shit like that
        :param since: since when to get logs. if None - get all the log
        :return:  iterable object to read log lines from
        """
        raise NotImplementedError()
        self.__writer.write(b'logiter\n')
        self.__writer.write(logname.encode('UTF-8'))
        await self.__writer.drain()
        return nethelpers.LineDrainer(self.__reader)
