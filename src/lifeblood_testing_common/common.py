from lifeblood.scheduler.scheduler import Scheduler
from lifeblood.worker_resource_definition import WorkerResourceDefinition
from lifeblood.basenode_serializer_v2 import NodeSerializerV2
from lifeblood.pluginloader import PluginNodeDataProvider  # TODO: this must be replaced by a testing mocker
from lifeblood_testing_common.scheduler_config_provider_default_override import SchedulerConfigProviderOverrides

from typing import Any, Dict, Optional, Tuple


async def chain(*coros):
    for coro in coros:
        await coro


def create_default_scheduler(
        db_file_path,
        *,
        do_broadcasting: Optional[bool] = None,
        broadcast_interval: Optional[int] = None,
        helpers_minimal_idle_to_ensure=1,
        server_addr: Optional[Tuple[str, int, int]] = None,
        server_ui_addr: Optional[Tuple[str, int]] = None,
        node_per_node_config: Optional[Dict[str, Dict[str, Any]]] = None,
        node_global_config: Optional[Dict[str, Dict[str, Any]]] = None,
        resource_definitions: Optional[Tuple[WorkerResourceDefinition, ...]] = None,
) -> Scheduler:
    legacy_addr = None
    message_addr = None
    if server_addr is not None:
        legacy_addr = (server_addr[0], server_addr[1])
        message_addr = (server_addr[0], server_addr[2])
    config = SchedulerConfigProviderOverrides(
        main_db_location=db_file_path,
        do_broadcast=do_broadcasting,
        broadcast_interval=broadcast_interval,
        minimal_idle_helpers=helpers_minimal_idle_to_ensure,
        legacy_server_address=legacy_addr,
        message_processor_address=message_addr,
        ui_address=server_ui_addr,
        node_per_node_config=node_per_node_config,
        node_global_config=node_global_config,
        resource_definitions=resource_definitions,
    )
    return Scheduler(
        scheduler_config_provider=config,
        node_data_provider=PluginNodeDataProvider(),
        node_serializers=[NodeSerializerV2()],
    )