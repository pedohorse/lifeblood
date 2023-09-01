import asyncio
import aiofiles
import struct
import json
from .enums import TaskScheduleStatus, TaskExecutionStatus, TaskExecutionStatus, WorkerPingReply, SpawnStatus
from .exceptions import NotEnoughResources, ProcessInitializationError, WorkerNotAvailable, AlreadyRunning, CouldNotNegotiateProtocolVersion
from .scheduler_message_processor import SchedulerExtraControlClient, SchedulerInvocationMessageClient
from .environment_resolver import ResolutionImpossibleError
from .taskspawn import TaskSpawn
from . import logging
from . import invocationjob
from . import nethelpers

import os

from typing import Dict, Optional, Set, Sequence, Tuple, TYPE_CHECKING
if TYPE_CHECKING:
    from .worker import Worker


async def read_string(reader) -> str:
    strlen = struct.unpack('>Q', await reader.readexactly(8))[0]
    if strlen == 0:
        return ''
    return (await reader.readexactly(strlen)).decode('UTF-8')


async def write_string(writer, s: str):
    b = s.encode('UTF-8')
    writer.write(struct.pack('>Q', len(b)))
    writer.write(b)
    

class ProtocolHandler:
    def protocol_version(self) -> Tuple[int, int]:
        raise NotImplementedError()

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        raise NotImplementedError()


class WorkerInvocationProtocolHandlerV10(ProtocolHandler):
    def __init__(self, worker: "Worker"):
        super().__init__()
        self.__worker = worker
        self.__logger = logging.get_logger(f'worker.invoc_protocol_v{".".join(str(i) for i in self.protocol_version())}')

        self.__known_commands = {
            'spawn': self.comm_spawn,
            'tupdateattribs': self.comm_update_attributes,
            'sendinvmessage': self.comm_send_invocation_message,
            'recvinvmessage': self.comm_receive_invocation_message,
        }
        
    def protocol_version(self) -> Tuple[int, int]:
        return 1, 0

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        # TODO: either we need timeout here - then add it to all reading operations below
        # TODO: or we don't need it here - then remove wait.
        command = await read_string(reader)
        self.__logger.debug(f'got command: {command}')

        if command not in self.__known_commands:
            raise NotImplementedError(f'unknown command {command}')
        await self.__known_commands[command](reader, writer)

    #
    # commands
    async def comm_spawn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        tasksize = struct.unpack('>Q', await reader.readexactly(8))[0]
        taskspawn: TaskSpawn = TaskSpawn.deserialize(await reader.readexactly(tasksize))

        with SchedulerExtraControlClient.get_scheduler_control_client(self.__worker.scheduler_message_address(),
                                                                      self.__worker.message_processor()) as client:  # type: SchedulerExtraControlClient
            status, tid = await client.spawn(taskspawn)
        writer.write(struct.pack('>I?Q', status.value, tid is not None, 0 if tid is None else tid))

    async def comm_update_attributes(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        task_id, update_data_size, strcount = struct.unpack('>QQQ', await reader.readexactly(24))
        attribs_to_update = await asyncio.get_event_loop().run_in_executor(None, json.loads,
                                                                           (await reader.readexactly(update_data_size)).decode('UTF-8'))
        attribs_to_delete = set()
        for _ in range(strcount):
            attribs_to_delete.add(await read_string(reader))
        with SchedulerExtraControlClient.get_scheduler_control_client(self.__worker.scheduler_message_address(),
                                                                      self.__worker.message_processor()) as client:  # type: SchedulerExtraControlClient
            await client.update_task_attributes(task_id, attribs_to_update, attribs_to_delete)
        writer.write(b'\1')

    # invocation messaging
    async def comm_send_invocation_message(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        to_inv_id, from_inv_id, data_size, addressee_timeout = struct.unpack('>QQQf', await reader.readexactly(28))
        to_addressee = await read_string(reader)
        data = await reader.readexactly(data_size)
        with SchedulerInvocationMessageClient.get_scheduler_control_client(self.__worker.scheduler_message_address(),
                                                                           self.__worker.message_processor()) as client:  # type: SchedulerInvocationMessageClient
            send_status = await client.send_invocation_message(to_inv_id, to_addressee, from_inv_id, data, addressee_timeout=addressee_timeout)
        await write_string(writer, send_status.value)

    async def comm_receive_invocation_message(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addressee = await read_string(reader)
        poll_interval, = struct.unpack('>f', await reader.readexactly(4))
        data = None
        src_iid = None
        while data is None:
            try:
                src_iid, data = await self.__worker.worker_task_addressee_wait(addressee, timeout=poll_interval)
            except asyncio.TimeoutError:
                # in case of timeout - we keep waiting, but better poke that receiver still lives
                writer.write(struct.pack('>?', False))
                await writer.drain()
                cancelled, = struct.unpack('>?', await reader.readexactly(1))
                if cancelled:
                    return
        assert src_iid is not None
        writer.write(struct.pack('>?QQ', True, src_iid, len(data)))
        writer.write(data)
        await writer.drain()


class WorkerInvocationServerProtocol(asyncio.StreamReaderProtocol):

    def __init__(self, worker: "Worker", protocol_handlers: Sequence[ProtocolHandler], limit: int = 2 ** 16):
        self.__logger = logging.get_logger('worker.invoc_protocol')
        self.__timeout = 300.0
        self.__reader = asyncio.StreamReader(limit=limit)
        self.__worker = worker
        self.__handlers: Dict[Tuple[int, int], ProtocolHandler] = {x.protocol_version(): x for x in protocol_handlers}
        self.__supported_versions = tuple(sorted(self.__handlers.keys(), reverse=True))
        self.__saved_references = []
        super(WorkerInvocationServerProtocol, self).__init__(self.__reader, self.client_connected_cb)

    def supported_protocol_versions(self) -> Tuple[Tuple[int, int], ...]:
        """
        return supported versions, sorted from highest to lowest
        """
        return self.__supported_versions

    async def __negotiate_protocol_version(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> Tuple[int, int]:
        num_protocols, = struct.unpack('>Q', await reader.readexactly(8))
        vers: Set[Tuple[int, int]] = set()
        for _ in range(num_protocols):
            vmaj, vmin = struct.unpack('>II', await reader.readexactly(8))
            assert isinstance(vmaj, int) and isinstance(vmin, int)
            vers.add((vmaj, vmin))

        for ver in self.supported_protocol_versions():
            if ver in vers:
                writer.write(struct.pack('>II', *ver))
                await writer.drain()
                return ver
        raise CouldNotNegotiateProtocolVersion(self.supported_protocol_versions(), tuple(vers))

    async def client_connected_cb(self, reader, writer):
        # there is a bug in py <=3.8, callback task can be GCd
        # see https://bugs.python.org/issue46309
        # so we HAVE to save a reference to self somewhere
        self.__saved_references.append(asyncio.current_task())

        try:
            await self.__worker.wait_till_starts()  # important that worker is fully up before we actually start listening to shit

            prot = await self.__negotiate_protocol_version(reader, writer)
            await self.__handlers[prot].handle(reader, writer)

            # drain just in case
            await writer.drain()
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

#
# class WorkerInvocationClient:
#     def __init__(self, ip: str, port: int, timeout=300.0):
#         self.__logger = logging.get_logger('scheduler.invoc_connection')
#         self.__conn_task = asyncio.create_task(asyncio.open_connection(ip, port))
#         self.__reader = None  # type: Optional[asyncio.StreamReader]
#         self.__writer = None  # type: Optional[asyncio.StreamWriter]
#         self.__timeout = timeout
#
#     async def __aenter__(self) -> "WorkerInvocationClient":
#         await self._ensure_conn_open()
#         return self
#
#     async def __aexit__(self, exc_type, exc_val, exc_tb):
#         await self.close()
#
#     async def close(self):
#         self.__writer.close()
#         await self.__writer.wait_closed()
#
#     async def _ensure_conn_open(self):
#         if self.__reader is not None:
#             return
#         self.__reader, self.__writer = await self.__conn_task
#         self.__writer.write(b'')
#
#     raise NotImplementedError()
