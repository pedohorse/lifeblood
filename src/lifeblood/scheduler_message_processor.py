import os
import asyncio
import aiofiles
from contextlib import contextmanager
from .exceptions import NotEnoughResources, ProcessInitializationError, WorkerNotAvailable
from .environment_resolver import ResolutionImpossibleError
from . import logging
from . import invocationjob
from .exceptions import AlreadyRunning
from .enums import WorkerPingReply, TaskScheduleStatus, WorkerState, WorkerType
from .net_classes import WorkerResources
from .net_messages.impl.tcp_simple_command_message_processor import TcpCommandMessageProcessor
from .net_messages.impl.clients import CommandJsonMessageClient
from .net_messages.address import AddressChain
from .net_messages.messages import Message
from .net_messages.impl.message_haldlers import CommandMessageHandlerBase


from typing import Awaitable, Callable, Dict, Optional, Tuple, TYPE_CHECKING
if TYPE_CHECKING:
    from .scheduler import Scheduler


class SchedulerCommandHandler(CommandMessageHandlerBase):
    def __init__(self, scheduler: "Scheduler"):
        super().__init__()
        self.__scheduler = scheduler

    def command_mapping(self) -> Dict[str, Callable[[dict, CommandJsonMessageClient, Message], Awaitable[None]]]:
        return {
            'pulse': self._command_pulse,
            '_pulse3way_': self._command_pulse3way,  # TODO: remove this when handlers are implemented
            # worker-specific
            'worker.ping': self._command_ping,
            'worker.done': self._command_done,
            'worker.dropped': self._command_dropped,
            'worker.hello': self._command_hello,
            'worker.bye': self._command_bye,
        }

    #
    # commands
    #

    async def _command_ping(self, args: dict, client: CommandJsonMessageClient, original_message: Message):  # 'ping'
        """
        expects keys:
            worker_addr: address of the worker to query
        returns keys:
            state: worker status as seen by scheduler
        """
        # when worker pings scheduler - scheduler returns the state it thinks the worker is in
        addr = args['worker_addr']
        wid = await self.__scheduler.worker_id_from_address(addr)
        if wid is None:
            state = WorkerState.UNKNOWN
        else:
            state = await self.__scheduler.get_worker_state(wid)
        await client.send_message_as_json({'state': state.value})

    async def _command_pulse(self, args: dict, client: CommandJsonMessageClient, original_message: Message):  # 'pulse'
        """
        expects keys:
        returns keys:
            ok: ok is ok
        """
        await client.send_message_as_json({'ok': True})

    async def _command_done(self, args: dict, client: CommandJsonMessageClient, original_message: Message):  # 'done'
        """
        expects keys:
            task: serialized task
            stdout: task's stdout log (str)
            stderr: task's stderr log (str)
        returns keys:
            ok: ok is ok
        """
        task_data = args['task'].encode('latin1')
        task = await invocationjob.InvocationJob.deserialize_async(task_data)

        stdout = args['stdout']
        stderr = args['stderr']
        await self.__scheduler.task_done_reported(task, stdout, stderr)
        await client.send_message_as_json({'ok': True})

    async def _command_dropped(self, args: dict, client: CommandJsonMessageClient, original_message: Message):  # 'dropped'
        """
        expects keys:
        returns keys:
            ok: ok is ok
        """
        task_data = args['task'].encode('latin1')
        task = await invocationjob.InvocationJob.deserialize_async(task_data)

        stdout = args['stdout']
        stderr = args['stderr']
        await self.__scheduler.task_cancel_reported(task, stdout, stderr)
        await client.send_message_as_json({'ok': True})

    async def _command_hello(self, args: dict, client: CommandJsonMessageClient, original_message: Message):  # 'hello'
        """
        worker reports for duty

        expects keys:
            worker_addr: worker address to talk to
            worker_type: worker type enum value
            worker_res: serialized worker resource capabilities
        returns keys:
            db_uid: scheduler's database uid
        """
        addr = args['worker_addr']
        workertype: WorkerType = WorkerType(args['worker_type'])
        res_data = args['worker_res'].encode('latin1')

        worker_hardware: WorkerResources = WorkerResources.deserialize(res_data)
        await self.__scheduler.add_worker(addr, workertype, worker_hardware, assume_active=True)
        await client.send_message_as_json({'db_uid': self.__scheduler.db_uid()})

    async def _command_bye(self, args: dict, client: CommandJsonMessageClient, original_message: Message):  # 'bye'
        """
        worker reports he's quitting

        expects keys:
            worker_addr: worker address to talk to
        returns keys:
            ok: ok is ok
        """
        addr = args['worker_addr']
        await self.__scheduler.worker_stopped(addr)
        await client.send_message_as_json({'ok': True})

    async def _command_pulse3way(self, args: dict, client: CommandJsonMessageClient, original_message: Message):
        """
        TODO: remove this when handlers are implemented
        This command exists for test purposes only
        """
        await client.send_message_as_json({'phase': 1})
        msg2 = await client.receive_message()
        await client.send_message_as_json({'phase': 2})


class SchedulerMessageProcessor(TcpCommandMessageProcessor):
    def __init__(self, scheduler: "Scheduler", listening_address: Tuple[str, int], *, backlog=4096, connection_pool_cache_time=300):
        super().__init__(listening_address,
                         backlog=backlog,
                         connection_pool_cache_time=connection_pool_cache_time,
                         message_handlers=(SchedulerCommandHandler(scheduler),))
        self.__logger = logging.get_logger('scheduler.message_processor')


#
# Client
#


class SchedulerBaseClient:
    def __init__(self, client: CommandJsonMessageClient):
        self.__client = client

    @classmethod
    @contextmanager
    def get_scheduler_control_client(cls, scheduler_address: AddressChain, processor: TcpCommandMessageProcessor) -> "SchedulerBaseClient":
        with processor.message_client(scheduler_address) as message_client:
            yield SchedulerBaseClient(message_client)

    async def pulse(self):
        await self.__client.send_command('pulse', {})
        reply = await self.__client.receive_message()
        assert (await reply.message_body_as_json()).get('ok', False), 'something is not ok'


class SchedulerWorkerControlClient(SchedulerBaseClient):
    def __init__(self, client: CommandJsonMessageClient):
        super().__init__(client)
        self.__client = client

    @classmethod
    @contextmanager
    def get_scheduler_control_client(cls, scheduler_address: AddressChain, processor: TcpCommandMessageProcessor) -> "SchedulerWorkerControlClient":
        with processor.message_client(scheduler_address) as message_client:
            yield SchedulerWorkerControlClient(message_client)

    async def ping(self, addr: AddressChain) -> WorkerState:
        await self.__client.send_command('worker.ping', {
            'worker_addr': str(addr)
        })
        reply = await self.__client.receive_message()
        return WorkerState((await reply.message_body_as_json())['state'])

    async def report_task_done(self, task: invocationjob.InvocationJob, stdout_file: str, stderr_file: str):
        async with aiofiles.open(stdout_file, 'r') as f:
            stdout = await f.read()
        async with aiofiles.open(stderr_file, 'r') as f:
            stderr = await f.read()
        await self.__client.send_command('worker.done', {
            'task': (await task.serialize_async()).decode('latin1'),
            'stdout': stdout,
            'stderr': stderr
        })
        reply = await self.__client.receive_message()
        assert (await reply.message_body_as_json()).get('ok', False), 'something is not ok'

    async def report_task_canceled(self, task: invocationjob.InvocationJob, stdout_file: str, stderr_file: str):
        async with aiofiles.open(stdout_file, 'r') as f:
            stdout = await f.read()
        async with aiofiles.open(stderr_file, 'r') as f:
            stderr = await f.read()
        await self.__client.send_command('worker.dropped', {
            'task': (await task.serialize_async()).decode('latin1'),
            'stdout': stdout,
            'stderr': stderr
        })
        reply = await self.__client.receive_message()
        assert (await reply.message_body_as_json()).get('ok', False), 'something is not ok'

    async def say_hello(self, address_to_advertise: AddressChain, worker_type: WorkerType, worker_resources: WorkerResources) -> int:
        await self.__client.send_command('worker.hello', {
            'worker_addr': str(address_to_advertise),
            'worker_type': worker_type.value,
            'worker_res': worker_resources.serialize().decode('latin1')
        })
        reply = await self.__client.receive_message()
        return (await reply.message_body_as_json())['db_uid']

    async def say_bye(self, address_of_worker: str):
        await self.__client.send_command('worker.bye', {
            'worker_addr': str(address_of_worker)
        })
        reply = await self.__client.receive_message()
        assert (await reply.message_body_as_json()).get('ok', False), 'something is not ok'
