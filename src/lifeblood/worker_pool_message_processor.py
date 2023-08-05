from contextlib import contextmanager
from . import logging
from .enums import WorkerState
from .net_messages.tcp_impl.tcp_simple_command_message_processor import TcpCommandMessageProcessor, CommandJsonMessageClient
from .net_messages.address import AddressChain
from .net_messages.messages import Message


from typing import Optional, Tuple, TYPE_CHECKING
if TYPE_CHECKING:
    from .simple_worker_pool import WorkerPool


class WorkerPoolMessageProcessor(TcpCommandMessageProcessor):
    def __init__(self, worker_pool: "WorkerPool", listening_address: Tuple[str, int], *, backlog=4096, connection_pool_cache_time=300):
        super().__init__(listening_address, backlog=backlog, connection_pool_cache_time=connection_pool_cache_time)
        self.__logger = logging.get_logger('workerpool.message_processor')
        self.__worker_pool = worker_pool

    async def should_process(self, orig_message: Message):
        return (await super().should_process(orig_message)) and not self.__worker_pool.is_pool_closed()

    def command_mapping(self):
        return {
            'worker.state_report': self._command_state_report
        }

    #
    # commands
    #

    async def _command_state_report(self, args: dict, client: CommandJsonMessageClient, original_message: Message):
        """
        expects keys:
            worker_id: worker id as assigned by pool
            state: WorkerState
        returns keys:
            ok: ok is ok
        """
        state = WorkerState(args['state'])
        await self.__worker_pool._worker_state_change(args['worker_id'], state)
        await client.send_message_as_json({
            'ok': True
        })


#
# Client
#


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
