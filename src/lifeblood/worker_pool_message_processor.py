from .enums import WorkerState
from .net_messages.impl.tcp_simple_command_message_processor import TcpCommandMessageProcessor
from .net_messages.impl.clients import CommandJsonMessageClient
from .net_messages.address import DirectAddress
from .net_messages.messages import Message
from .net_messages.impl.message_haldlers import CommandMessageHandlerBase
from .simple_worker_pool import SimpleWorkerPool

from typing import Iterable, Tuple, Union


class WorkerPoolMessageHandler(CommandMessageHandlerBase):
    def __init__(self, worker_pool: SimpleWorkerPool):
        super().__init__()
        self.__worker_pool = worker_pool

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


class WorkerPoolMessageProcessor(TcpCommandMessageProcessor):
    def __init__(
            self,
            worker_pool: SimpleWorkerPool,
            listening_address_or_addresses: Union[Tuple[str, int], Iterable[Tuple[str, int]], DirectAddress, Iterable[DirectAddress]],
            *,
            backlog=4096,
            connection_pool_cache_time=300
    ):
        super().__init__(listening_address_or_addresses,
                         backlog=backlog,
                         connection_pool_cache_time=connection_pool_cache_time,
                         message_handlers=(WorkerPoolMessageHandler(worker_pool),))
