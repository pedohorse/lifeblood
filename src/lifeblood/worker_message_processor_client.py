import asyncio
from contextlib import contextmanager
from . import invocationjob
from .enums import WorkerPingReply, TaskScheduleStatus, InvocationMessageResult
from .net_messages.impl.tcp_simple_command_message_processor import TcpCommandMessageProcessor
from .net_messages.impl.clients import CommandJsonMessageClient
from .net_messages.address import AddressChain

from typing import Optional, Tuple


class WorkerControlClient:
    def __init__(self, client: CommandJsonMessageClient):
        self.__client = client

    @classmethod
    @contextmanager
    def get_worker_control_client(cls, worker_address: AddressChain, processor: TcpCommandMessageProcessor) -> "WorkerControlClient":
        with processor.message_client(worker_address) as message_client:
            yield WorkerControlClient(message_client)

    async def give_task(self, task: invocationjob.Invocation, reply_address: Optional[AddressChain] = None) -> Tuple[TaskScheduleStatus, str, str]:
        """
        if reply_address is not given - message source address will be used
        """
        await self.__client.send_command('task', {
            'task': await asyncio.get_event_loop().run_in_executor(None, task.serialize_to_data),
            'reply_to': str(reply_address) if reply_address else None
        })

        reply = await (await self.__client.receive_message()).message_body_as_json()
        return TaskScheduleStatus(reply['status']), reply.get('error_class', ''), reply.get('message', '')

    async def quit_worker(self):
        await self.__client.send_command('quit', {})

        await self.__client.receive_message()

    async def cancel_task(self) -> None:
        await self.__client.send_command('drop', {})

        await self.__client.receive_message()

    async def status(self):
        raise NotImplementedError()

    async def get_log(self, invocation_id) -> Tuple[str, str]:
        await self.__client.send_command('log', {
            'invoc_id': invocation_id
        })

        reply = await (await self.__client.receive_message()).message_body_as_json()
        return str(reply['stdout']), str(reply['stderr'])

    async def send_invocation_message(self,
                                      destination_invocation_id: int,
                                      destination_addressee: str,
                                      source_invocation_id: Optional[int],
                                      message_body: bytes,
                                      addressee_timeout: float,
                                      overall_timeout: float) -> InvocationMessageResult:
        """
        Note that this command, unlike others, does not raise,
        instead it wraps errors into InvocationMessageResult
        """
        await self.__client.send_command('invocation_message', {
            'dst_invoc_id': destination_invocation_id,
            'src_invoc_id': source_invocation_id,
            'addressee': destination_addressee,
            'addressee_timeout': addressee_timeout,
            'message_data_raw': message_body.decode('latin1'),
        })

        reply = await (await self.__client.receive_message(timeout=overall_timeout)).message_body_as_json()
        return InvocationMessageResult(reply['result'])
