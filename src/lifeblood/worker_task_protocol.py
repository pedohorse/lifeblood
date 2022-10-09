import asyncio
import aiofiles
from enum import Enum
import struct
from .exceptions import NotEnoughResources, ProcessInitializationError
from .environment_resolver import ResolutionImpossibleError
from . import logging
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

    def __init__(self, worker: "Worker", limit: int = 2 ** 16):
        self.__logger = logging.get_logger('worker.protocol')
        self.__timeout = 60.0
        self.__reader = asyncio.StreamReader(limit=limit)
        self.__worker = worker
        self.__saved_references = []
        super(WorkerTaskServerProtocol, self).__init__(self.__reader, self.client_connected_cb)

    async def client_connected_cb(self, reader, writer):
        # there is a bug in py <=3.8, callback task can be GCd
        # see https://bugs.python.org/issue46309
        # so we HAVE to save a reference to self somewhere
        self.__saved_references.append(asyncio.current_task())

        async def read_string() -> str:
            strlen = struct.unpack('>Q', await reader.readexactly(8))[0]
            return (await reader.readexactly(strlen)).decode('UTF-8')

        async def write_string(s: str):
            b = s.encode('UTF-8')
            writer.write(struct.pack('>Q', len(b)))
            writer.write(b)

        try:
            await self.__worker.wait_till_starts()  # important that worker is fully up before we actually start listening to shit

            prot = await asyncio.wait_for(reader.readexactly(4), timeout=self.__timeout)
            if prot == b'\0\0\0\0':
                # TODO: either we need timeout here - then add it to all reading operations below
                # TODO: or we don't need it here - then remove wait.
                command = await asyncio.wait_for(reader.readline(), timeout=self.__timeout)  # type: bytes
                if command.endswith(b'\n'):
                    command = command[:-1]
                self.__logger.debug(f'got command: {command.decode("UTF-8")}')

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
                    self.__logger.debug(f'got task: {task}, reply result to {addr}')
                    try:
                        self.__logger.debug('taking the task')
                        await self.__worker.run_task(task, addr)
                        writer.write(bytes([TaskScheduleStatus.SUCCESS.value]))
                    except AlreadyRunning:
                        self.__logger.debug('BUSY. rejecting task')
                        writer.write(bytes([TaskScheduleStatus.BUSY.value]))
                    except ResolutionImpossibleError:
                        self.__logger.info('Worker failed to resolve required environment. rejecting task')
                        writer.write(bytes([TaskScheduleStatus.FAILED.value]))
                    except ProcessInitializationError:
                        self.__logger.info('Failed to initialize payload process. rejecting task')
                        writer.write(bytes([TaskScheduleStatus.FAILED.value]))
                    except NotEnoughResources:
                        self.__logger.warning('Not enough resources (this is unusual error - scheduler should know our resources). rejecting task')
                        writer.write(bytes([TaskScheduleStatus.FAILED.value]))
                    except Exception as e:
                        self.__logger.exception('no, cuz %s', e)
                        writer.write(bytes([TaskScheduleStatus.FAILED.value]))
                #
                # quit worker
                elif command == b'quit':
                    self.__worker.stop()
                    writer.write(b'\1')
                #
                # command drop/cancel current task
                elif command == b'drop':  # scheduler wants us to drop current task
                    try:
                        await self.__worker.cancel_task()
                    except:
                        self.__logger.exception('task drop failed')
                    writer.write(b'\1')
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
                #
                # command get worker's log line by line
                elif command == b'logiter':  # scheduler wants to have the log:
                    raise NotImplementedError()
                    # TODO: redo this using line feeder from nethelpers! also pass invocation id
                    # logname = await asyncio.wait_for(reader.readline(), timeout=self.__timeout)
                    # if logname.endswith(b'\n'):
                    #     logname = logname[:-1]
                    # logname = logname.decode('UTF-8')
                    # logfilepath = self.__worker.get_log_filepath(logname)
                    # async with aiofiles.open(logfilepath, 'r') as f:
                    #     async for line in f:
                    #         writer.write(b'\x01')
                    #         writer.write(line)
                    #     writer.write(b'\x00')

                # now wait for answer to be delivered
                await writer.drain()
            else:
                raise NotImplementedError()
        except asyncio.TimeoutError:
            self.__logger.error('operation timeout')
            raise
        except EOFError:
            self.__logger.error('connection was abruptly closed')
            raise
        finally:
            writer.close()
            await writer.wait_closed()
            # according to the note in the beginning of the function - now reference can be cleared
            self.__saved_references.remove(asyncio.current_task())


class WorkerTaskClient:
    def __init__(self, ip: str, port: int, timeout=30.0):
        self.__logger = logging.get_logger('scheduler.workerconnection')
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
            self.__logger.error('network error')
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
        await asyncio.wait_for(self.__writer.drain(), self.__timeout)
        reply = TaskScheduleStatus(ord(await asyncio.wait_for(self.__reader.readexactly(1), self.__timeout)))
        return reply

    async def cancel_task(self):
        await self._ensure_conn_open()
        self.__writer.write(b'drop\n')
        await asyncio.wait_for(self.__writer.drain(), self.__timeout)
        assert await self.__reader.readexactly(1) == b'\1'

    async def status(self):
        raise NotImplementedError()

    async def get_log(self, invocation_id) -> (str, str):
        self.__writer.write(b'log\n')
        self.__writer.write(struct.pack('>Q', invocation_id))
        await asyncio.wait_for(self.__writer.drain(), self.__timeout)
        stdoutsize = struct.unpack('>I', await asyncio.wait_for(self.__reader.readexactly(4), self.__timeout))[0]
        stdout = await asyncio.wait_for(self.__reader.readexactly(stdoutsize), self.__timeout)
        stderrsize = struct.unpack('>I', await asyncio.wait_for(self.__reader.readexactly(4), self.__timeout))[0]
        stderr = await asyncio.wait_for(self.__reader.readexactly(stderrsize), self.__timeout)
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
        await asyncio.wait_for(self.__writer.drain(), self.__timeout)
        return nethelpers.LineDrainer(self.__reader)
