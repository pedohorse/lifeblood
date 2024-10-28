from .config import Config
from .enums import WorkerType, ProcessPriorityAdjustment
from .net_messages.address import AddressChain
from .worker_core import WorkerCore
from .worker_message_processor import WorkerMessageProcessor
from .worker_invocation_protocol import WorkerInvocationProtocolHandlerV10, WorkerInvocationServerProtocol

from typing import Optional


class Worker(WorkerCore):
    def __init__(self, scheduler_addr: AddressChain, *,
                 child_priority_adjustment: ProcessPriorityAdjustment = ProcessPriorityAdjustment.NO_CHANGE,
                 worker_type: WorkerType = WorkerType.STANDARD,
                 config: Optional[Config] = None,  # TODO: this should be replaced with config provider with a fixed interface
                 singleshot: bool = False,
                 scheduler_ping_interval: float = 10,
                 scheduler_ping_miss_threshold: int = 6,
                 worker_id: Optional[int] = None,
                 pool_address: Optional[AddressChain] = None,
                 ):
        super().__init__(
            scheduler_addr=scheduler_addr,
            child_priority_adjustment=child_priority_adjustment,
            worker_type=worker_type,
            config=config,
            singleshot=singleshot,
            scheduler_ping_interval=scheduler_ping_interval,
            scheduler_ping_miss_threshold=scheduler_ping_miss_threshold,
            worker_id=worker_id,
            pool_address=pool_address,
            message_processor_factory=WorkerMessageProcessor,
            worker_invocation_protocol_factory=lambda worker: WorkerInvocationServerProtocol(worker, [WorkerInvocationProtocolHandlerV10(worker)]),
        )
