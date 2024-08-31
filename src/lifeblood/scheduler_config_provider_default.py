import os
from pathlib import Path
from .scheduler_config_provider_base import SchedulerConfigProviderBase
from .worker_resource_definition import WorkerResourceDefinition, WorkerResourceDataType, WorkerDeviceTypeDefinition
from . import defaults
from .nethelpers import all_interfaces

from typing import Optional, Tuple


class SchedulerConfigProviderDefaults(SchedulerConfigProviderBase):
    """
    this subclass implements SOME default values for scheduler configuration
    that subclasses of this class can fall back to
    """

    def main_database_location(self) -> str:
        return os.path.join(os.getcwd(), 'main.db')  # just "main.db" in working directory

    def main_database_connection_timeout(self) -> float:
        return 30

    def node_configuration(self, node_type_id: str) -> dict:
        return {}

    def hardware_resource_definitions(self) -> Tuple[WorkerResourceDefinition, ...]:
        return (
            WorkerResourceDefinition('cpu_count',
                                     WorkerResourceDataType.SHARABLE_COMPUTATIONAL_UNIT,
                                     'CPU core count',
                                     'CPU count'),
            WorkerResourceDefinition('cpu_mem',
                                     WorkerResourceDataType.MEMORY_BYTES,
                                     'RAM amount in bytes',
                                     'CPU ram (GB)'),
        )

    def hardware_device_type_definitions(self) -> Tuple[WorkerDeviceTypeDefinition, ...]:
        return (
            WorkerDeviceTypeDefinition('gpu', (
                WorkerResourceDefinition('mem', WorkerResourceDataType.MEMORY_BYTES, 'gpu device memory (VRAM)', 'Gpu Memory (GB)'),
                WorkerResourceDefinition('opencl_ver', WorkerResourceDataType.GENERIC_FLOAT, 'OpenCL version required', 'OpenCL version', 1.2),
                WorkerResourceDefinition('cuda_cc', WorkerResourceDataType.GENERIC_FLOAT, 'CUDA Compute Capability required', 'CUDA CC', 0.0),
            )),
        )

    def hardware_ban_timeout(self) -> float:
        return 10.0

    def ping_intervals(self) -> Tuple[float, float, float, float]:
        return (
            10,  # interval for active workers (workers doing work)
            30,  # interval for idle workers
            60,  # interval for off/errored workers  (not really used since workers need to report back first)
            5    # dormant state multiplier
        )

    def external_log_location(self) -> Optional[str]:
        return None

    def legacy_server_address(self) -> Optional[Tuple[str, int]]:
        return '0.0.0.0', defaults.scheduler_port()  # use all ifaces by default

    def server_message_addresses(self) -> Tuple[Tuple[str, int], ...]:
        return tuple(
            (iface_ip, defaults.scheduler_message_port()) for iface_ip in all_interfaces()
        )

    def server_ui_address(self) -> Tuple[str, int]:
        return '0.0.0.0', defaults.ui_port()

    def _config_do_broadcast(self) -> Optional[bool]:
        return True

    def _config_broadcast_interval(self) -> Optional[float]:
        return 10.0

    def broadcast_interval(self) -> Optional[float]:
        do_broadcast = self._config_do_broadcast()
        broadcast_interval = self._config_broadcast_interval()
        return broadcast_interval if do_broadcast else None

    def invocation_attempts(self) -> int:
        return 3

    def task_processor_housekeeping_interval(self) -> float:
        return 60.0

    def ignore_node_deserialization_failures(self) -> bool:
        return False

    def scheduler_helpers_minimal(self) -> int:
        return 1

    def node_data_provider_custom_plugins_path(self) -> Path:
        return Path.cwd() / 'custom_plugins'

    def node_data_provider_extra_plugin_paths(self) -> Tuple[Path, ...]:
        return ()
