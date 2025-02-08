import asyncio

import aiofiles
from contextlib import contextmanager
from . import invocationjob
from .taskspawn import TaskSpawn
from .enums import WorkerState, WorkerType, SpawnStatus, InvocationMessageResult
from .hardware_resources import HardwareResources
from .worker_metadata import WorkerMetadata
from .net_messages.impl.tcp_simple_command_message_processor import TcpCommandMessageProcessor
from .net_messages.impl.clients import CommandJsonMessageClient
from .net_messages.address import AddressChain
from .net_messages.exceptions import MessageReceiveTimeoutError
from .worker_resource_definition import WorkerResourceDefinition, WorkerDeviceTypeDefinition

from typing import List, Optional, Set, Tuple


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

    async def get_normalized_addresses(self) -> Tuple[AddressChain, AddressChain]:
        """
        TODO: this should be available to ALL clients/processors
        normalized address chain is the one that has all the intermediate addresses
        so that reversed address can be used as is to send messages back
        example of non-normalized address would be
            192.168.0.11:1234|10.0.0.22:2345
        this assumes that target at 192.168.0.11:1234 can send messages to different subnets
        while such address is correct, it cannot be used reversed as return address without additional actions
        a normalized version of this address is something like
            192.168.0.11:1234|10.0.0.11:1234|10.0.0.22:2345

        :returns: tuple of normalized addresses of destination message processor, and a reversed address of this client's processor as seen by destination processor
        """

        await self.__client.send_command('what_is_my_address', {})
        reply = await self.__client.receive_message()
        reply_body = await reply.message_body_as_json()
        assert reply_body.get('ok', False), 'something is not ok'
        return reply.message_source(), reply_body['my_address']


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

    async def report_task_done(self, task: invocationjob.Invocation, stdout_file: str, stderr_file: str):
        async with aiofiles.open(stdout_file, 'r', errors='replace') as f:
            stdout = await f.read()
        async with aiofiles.open(stderr_file, 'r', errors='replace') as f:
            stderr = await f.read()
        await self.__client.send_command('worker.done', {
            'task': await asyncio.get_event_loop().run_in_executor(None, task.serialize_to_data),
            'stdout': stdout,
            'stderr': stderr
        })
        reply = await self.__client.receive_message()
        assert (await reply.message_body_as_json()).get('ok', False), 'something is not ok'

    async def report_task_canceled(self, task: invocationjob.Invocation, stdout_file: str, stderr_file: str):
        async with aiofiles.open(stdout_file, 'r') as f:
            stdout = await f.read()
        async with aiofiles.open(stderr_file, 'r') as f:
            stderr = await f.read()
        await self.__client.send_command('worker.dropped', {
            'task': await asyncio.get_event_loop().run_in_executor(None, task.serialize_to_data),
            'stdout': stdout,
            'stderr': stderr
        })
        reply = await self.__client.receive_message()
        assert (await reply.message_body_as_json()).get('ok', False), 'something is not ok'

    async def report_invocation_progress(self, invocation_id: int, progress: float):
        await self.__client.send_command('worker.progress_report', {
            'invocation_id': invocation_id,
            'progress': progress,
        })
        reply = await self.__client.receive_message()
        assert (await reply.message_body_as_json()).get('ok', False), 'something is not ok'

    async def say_hello(self, address_to_advertise: AddressChain, worker_type: WorkerType, worker_resources: HardwareResources, worker_metadata: WorkerMetadata) -> int:
        await self.__client.send_command('worker.hello', {
            'worker_addr': str(address_to_advertise),
            'worker_type': worker_type.value,
            'worker_res': worker_resources.serialize().decode('latin1'),
            'meta_hostname': worker_metadata.hostname,
        })
        reply = await self.__client.receive_message()
        return (await reply.message_body_as_json())['db_uid']

    async def get_resource_configuration(self) -> Tuple[Tuple[WorkerResourceDefinition, ...], Tuple[WorkerDeviceTypeDefinition, ...]]:
        """
        get list of resources and device types that scheduler defines
        """
        await self.__client.send_command('resource_defs', {})
        reply = await self.__client.receive_message()
        reply_body = await reply.message_body_as_json()
        print(reply_body)
        resources = tuple(WorkerResourceDefinition.from_json_dict(res_data) for res_data in reply_body['resources'])
        devices = tuple(WorkerDeviceTypeDefinition.from_json_dict(dev_data) for dev_data in reply_body['device_types'])
        return resources, devices

    async def say_bye(self, address_of_worker: str):
        await self.__client.send_command('worker.bye', {
            'worker_addr': str(address_of_worker)
        })
        reply = await self.__client.receive_message()
        assert (await reply.message_body_as_json()).get('ok', False), 'something is not ok'


class SchedulerExtraControlClient(SchedulerBaseClient):
    def __init__(self, client: CommandJsonMessageClient):
        super().__init__(client)
        self.__client = client

    @classmethod
    @contextmanager
    def get_scheduler_control_client(cls, scheduler_address: AddressChain, processor: TcpCommandMessageProcessor) -> "SchedulerExtraControlClient":
        with processor.message_client(scheduler_address) as message_client:
            yield SchedulerExtraControlClient(message_client)

    async def spawn(self, task_spawn: TaskSpawn) -> Tuple[SpawnStatus, Optional[int]]:
        await self.__client.send_command('spawn', {
            'task': task_spawn.serialize().decode('latin1')
        })
        reply = await self.__client.receive_message()
        ret_data = await reply.message_body_as_json()
        return SpawnStatus(ret_data['status']), ret_data['task_id']

    async def node_name_to_id(self, name: str) -> List[int]:
        await self.__client.send_command('nodenametoid', {
            'name': name
        })
        reply = await self.__client.receive_message()
        ret_data = await reply.message_body_as_json()
        return list(ret_data['node_ids'])

    async def update_task_attributes(self, task_id: int, attribs_to_update: dict, attribs_to_delete: Set[str]):
        await self.__client.send_command('tupdateattribs', {
            'task_id': task_id,
            'attribs_to_update': attribs_to_update,
            'attribs_to_delete': list(attribs_to_delete),
        })
        reply = await self.__client.receive_message()
        assert (await reply.message_body_as_json()).get('ok')


class SchedulerInvocationMessageClient:
    def __init__(self, client: CommandJsonMessageClient):
        self.__client = client

    @classmethod
    @contextmanager
    def get_scheduler_control_client(cls, scheduler_address: AddressChain, processor: TcpCommandMessageProcessor) -> "SchedulerInvocationMessageClient":
        with processor.message_client(scheduler_address) as message_client:
            yield SchedulerInvocationMessageClient(message_client)

    async def send_invocation_message(self,
                                      destination_invocation_id: int,
                                      destination_addressee: str,
                                      source_invocation_id: Optional[int],
                                      message_body: bytes,
                                      *,
                                      addressee_timeout: float = 90,
                                      overall_timeout: float = 300) -> InvocationMessageResult:
        if overall_timeout < addressee_timeout:
            overall_timeout = addressee_timeout

        await self.__client.send_command('forward_invocation_message', {
            'dst_invoc_id': destination_invocation_id,
            'src_invoc_id': source_invocation_id,
            'addressee': destination_addressee,
            'addressee_timeout': addressee_timeout,
            'overall_timeout': overall_timeout,
            'message_data_raw': message_body.decode('latin1'),
        })
        try:
            return InvocationMessageResult((await (await self.__client.receive_message(timeout=overall_timeout)).message_body_as_json())['result'])
        except MessageReceiveTimeoutError:
            return InvocationMessageResult.ERROR_DELIVERY_TIMEOUT
