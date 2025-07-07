from ..node_dataprovider_base import NodeDataProvider
from ..basenode_serialization import NodeSerializerBase

from ..scheduler_config_provider_base import SchedulerConfigProviderBase
from ..scheduler_task_protocol import SchedulerTaskProtocol
from ..scheduler_ui_protocol import SchedulerUiProtocol
from ..scheduler_message_processor import SchedulerMessageProcessor

from .scheduler_core import SchedulerCore
from .worker_ping_producer import WorkerPingProducer
from .data_access import DataAccess

from typing import List


class Scheduler(SchedulerCore):
    def __init__(self, *,
                 scheduler_config_provider: SchedulerConfigProviderBase,
                 node_data_provider: NodeDataProvider,
                 node_serializers: List[NodeSerializerBase],
                 ):
        data_access = DataAccess(
            config_provider=scheduler_config_provider,
        )
        super().__init__(
            scheduler_config_provider=scheduler_config_provider,
            node_data_provider=node_data_provider,
            node_serializers=node_serializers,
            message_processor_factory=SchedulerMessageProcessor,
            legacy_task_protocol_factory=SchedulerTaskProtocol,
            ui_protocol_factory=SchedulerUiProtocol,
            data_access=data_access,
            ping_producers=[WorkerPingProducer(self, data_access)]
        )
