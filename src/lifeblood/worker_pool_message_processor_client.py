from contextlib import contextmanager
from .enums import WorkerState
from .net_messages.impl.tcp_simple_command_message_processor import TcpCommandMessageProcessor
from .net_messages.impl.clients import CommandJsonMessageClient
from .net_messages.address import AddressChain


class WorkerPoolControlClient:
    def __init__(self, client: CommandJsonMessageClient):
        self.__client = client

    @classmethod
    @contextmanager
    def get_worker_pool_control_client(cls, scheduler_address: AddressChain, processor: TcpCommandMessageProcessor) -> "WorkerPoolControlClient":
        with processor.message_client(scheduler_address) as message_client:
            yield WorkerPoolControlClient(message_client)

    async def report_state(self, worker_id: int, state: WorkerState):
        await self.__client.send_command('worker.state_report', {
            'worker_id': worker_id,
            'state': state.value
        })
        reply = await self.__client.receive_message()
        assert (await reply.message_body_as_json()).get('ok', False), 'something is not ok'
