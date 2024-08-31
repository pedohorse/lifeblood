from .worker_resource_definition import WorkerResourceDefinition, WorkerDeviceTypeDefinition
from pathlib import Path

from typing import Optional, Tuple


class SchedulerConfigProviderBase:
    """
    Scheduler Config Provider interface

    this should be responsible for using config values and making sense of them
    nothing in scheduler should use get_config directly and parse it's values by itself

    Currently, Scheduler Config Providers are supposed to be IMMUTABLE,
     meaning any config changes at runtime are NOT supported, as there are no mechanisms of informing interested parties (config users)
     about the changes
    """
    def main_database_location(self) -> str:
        """
        location of the main task database.
        probably file path, but can be something else if more things are supported
        """
        raise NotImplementedError()

    def main_database_connection_timeout(self) -> float:
        """
        database connection timeout
        """
        raise NotImplementedError()

    def node_configuration(self, node_type_id: str) -> dict:
        """
        arbitrary key-value pairs as defined by the scheduler's node configuration
        some nodes may have configuration stored in scheduler-side config
        nodes can use those configuration values in parameter expressions
        """
        raise NotImplementedError()

    def hardware_resource_definitions(self) -> Tuple[WorkerResourceDefinition, ...]:
        """
        get definitions of generic resources that workers can have
        """
        raise NotImplementedError()

    def hardware_device_type_definitions(self) -> Tuple[WorkerDeviceTypeDefinition, ...]:
        """
        get definitions of hardware devices
        """
        raise NotImplementedError()

    def hardware_ban_timeout(self) -> float:
        """
        on certain submission errors we might want to ban hwid for some time, as it can be assumed
        that consecutive submission attempts will result in the same error (like package resolution error)
        If that happened - particular hardware may be "banned" from participating in scheduling for a certain time period.
        this is that period (in seconds)
        """
        raise NotImplementedError()

    def ping_intervals(self) -> Tuple[float, float, float, float]:
        """
        returns 4 floats that represent:
        - interval for active workers (workers doing work)
        - interval for idle workers
        - interval for off/errored workers  (not really used since workers need to report back first)
        - interval multiplier in dormant state of the scheduler
        """
        raise NotImplementedError()

    def external_log_location(self) -> Optional[str]:
        """
        location where to store worker logs.
        or None (should be default) - in that case logs are stored within the main db
        """
        raise NotImplementedError()

    def legacy_server_address(self) -> Optional[Tuple[str, int]]:
        """
        (address, port) where legacy command server should be listening to
        """
        raise NotImplementedError()

    def server_message_addresses(self) -> Tuple[Tuple[str, int], ...]:
        """
        a number of (address, port) pairs where message command processors should be listening to
        """
        raise NotImplementedError()

    def server_ui_address(self) -> Tuple[str, int]:
        """
        address where UI server should be listening to
        """
        raise NotImplementedError()

    def broadcast_interval(self) -> Optional[float]:
        """
        scheduler may broadcast information about which addresses it listens to

        None -> broadcasting is disabled
        otherwise - interval for broadcasts in seconds
        """
        raise NotImplementedError()

    def invocation_attempts(self) -> int:
        """
        how many times can invocation fail
        """
        raise NotImplementedError()

    def task_processor_housekeeping_interval(self) -> float:
        """
        interval in seconds, when to perform cleanup of potentially dangling data
        this process is not supposed to be run often, and should take little time to complete
        """
        raise NotImplementedError()

    def ignore_node_deserialization_failures(self) -> bool:
        """
        if such errors are ignored - failed nodes will be recreated
        this is generally NOT desired, but can help fix very broken databases
        """
        raise NotImplementedError()

    def scheduler_helpers_minimal(self) -> int:
        """
        how many scheduler helper processes to ensure are always available
        values below 1 should not be legal
        """
        raise NotImplementedError()

    # configuration of Node Data Provider
    def node_data_provider_custom_plugins_path(self) -> Path:
        """
        path to a special package where scheduler stores overrides
        This package always overrides everything else in case of conflict
        """
        raise NotImplementedError()

    def node_data_provider_extra_plugin_paths(self) -> Tuple[Path, ...]:
        """
        extra plugins to load.
        returns a number of paths where to search for packages
        """
        raise NotImplementedError()
