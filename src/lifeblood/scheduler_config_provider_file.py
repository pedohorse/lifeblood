from lifeblood.scheduler_config_provider_default import SchedulerConfigProviderDefaults
from .worker_resource_definition import WorkerResourceDefinition
from . import defaults, paths
from .config import get_config
from .nethelpers import all_interfaces
from .exceptions import SchedulerConfigurationError

from typing import Dict, List, Mapping, Optional, Tuple


class SchedulerConfigProviderFile(SchedulerConfigProviderDefaults):
    """
    this subclass implements SOME default values for scheduler configuration
    that subclasses of this class can fall back to
    """
    def __init__(
            self,
    ):
        self.__config = get_config('scheduler')
        self.__nodes_config = get_config('scheduler.nodes')

        # cache node config mappings to get them fast when processing context is needed
        #  this is the lazy solution
        self.__node_config_cache: Dict[str, dict] = {}

    def main_database_location(self) -> str:
        return self.__config.get_option_noasync('core.database.path', str(paths.default_main_database_location()))

    def node_configuration(self, node_type_id: str) -> Mapping:
        if node_type_id not in self.__node_config_cache:
            self.__node_config_cache[node_type_id] = {
                **dict(get_config('scheduler').get_option_noasync('scheduler.globals', {})),
                **dict(self.__config.get_option_noasync(f'{node_type_id}', {})),
            }

    def hardware_resource_definitions(self) -> Tuple[WorkerResourceDefinition, ...]:
        # resource definitions
        config_resources = self.__config.get_option_noasync('resource_definitions.per_machine', None)
        if config_resources is None:  # use default resource definitions
            return super().hardware_resource_definitions()

        if not isinstance(config_resources, dict):
            raise RuntimeError('bad config schema: resource_definitions.per_machine must be a mapping')  # TODO: turn into config schema error or smth
        conf_2_type_mapping = {
            'int': int,
            'float': float,
            'number': float,
        }
        res_defs = []
        for res_name, res_data in config_resources.items():
            if res_name.startswith('total_'):
                raise RuntimeError('resource name cannot start with "total_"')  # TODO: turn into config schema error or smth
            res_type = conf_2_type_mapping.get(res_data.get('type').lower(), None)
            if res_type is None:
                raise RuntimeError('resource type may be one of "int", "float", "number"')  # TODO: turn into config schema error or smth
            res_defs.append(WorkerResourceDefinition(
                res_name,
                res_type,
                res_data.get('description', ''),
                res_data.get('label', res_name),
            ))
        return tuple(res_defs)

    def hardware_ban_timeout(self) -> float:
        return self.__config.get_option_noasync('data_access.hwid_ban_timeout', super().hardware_ban_timeout())

    def ping_intervals(self) -> Tuple[float, float, float, float]:
        def_vals = super().ping_intervals()
        return (
            self.__config.get_option_noasync('scheduler.pinger.ping_interval', def_vals[0]),
            self.__config.get_option_noasync('scheduler.pinger.ping_idle_interval', def_vals[1]),
            self.__config.get_option_noasync('scheduler.pinger.ping_off_interval', def_vals[2]),
            self.__config.get_option_noasync('scheduler.pinger.dormant_ping_multiplier', def_vals[3]),
        )

    def external_log_location(self) -> Optional[str]:
        use_external = self.__config.get_option_noasync('core.database.store_logs_externally', False)
        path_external = self.__config.get_option_noasync('core.database.store_logs_externally_location', None)
        if use_external and not path_external:
            raise SchedulerConfigurationError('if store_logs_externally is set - store_logs_externally_location must be set too')
        return path_external if use_external else None

    def legacy_server_address(self) -> Optional[Tuple[str, int]]:
        default_addr, default_port = super().legacy_server_address()
        legacy_server_port = self.__config.get_option_noasync(
            'core.legacy_server_port',
            self.__config.get_option_noasync(
                'core.server_port',
                default_port
            )
        )
        legacy_server_ip = self.__config.get_option_noasync('core.server_ip', default_addr)
        return legacy_server_ip, legacy_server_port

    def _expand_catchall_address(self, address: str) -> List[str]:
        ret = []
        if address == '0.0.0.0':  # message_processor address must be addressable, no catchall
            for iface_ip in all_interfaces():
                ret.append(iface_ip)
        else:
            ret.append(address)
        return ret

    def server_message_addresses(self) -> Tuple[Tuple[str, int], ...]:
        default_values = super().server_message_addresses()
        # TODO: add ability to declare multiple addresses
        ret = []
        message_server_ip = self.__config.get_option_noasync('core.server_port', None)
        message_server_port = self.__config.get_option_noasync('core.server_message_port', None)
        if (len(default_values) == 0 and (message_server_ip is None or message_server_port is None)
                or message_server_ip is None and message_server_port is None):
            return default_values
        if message_server_ip is None:
            message_server_ip = default_values[0][0]
        if message_server_port is None:
            message_server_port = default_values[0][1]

        for server_ip in self._expand_catchall_address(message_server_ip):
            addr = (server_ip, message_server_port)
            if addr not in ret:
                ret.append(addr)
        return tuple(ret)

    def server_ui_address(self) -> Tuple[str, int]:
        default_ip, default_port = super().server_ui_address()
        ui_address = (
            self.__config.get_option_noasync('core.ui_ip', default_ip),
            self.__config.get_option_noasync('core.ui_port', default_port)
        )
        return ui_address

    def _config_do_broadcast(self) -> bool:
        return self.__config.get_option_noasync('core.broadcast', super()._config_do_broadcast())

    def _config_broadcast_interval(self) -> float:
        return self.__config.get_option_noasync('core.broadcast_interval', super()._config_broadcast_interval())

    def invocation_attempts(self) -> int:
        return self.__config.get_option_noasync('invocation.default_attempts', super().invocation_attempts())

    def task_processor_housekeeping_interval(self) -> float:
        return self.__config.get_option_noasync('task_processor.housekeeping_interval', super().task_processor_housekeeping_interval())

    def ignore_node_deserialization_failures(self) -> bool:
        return self.__config.get_option_noasync('core.ignore_node_deserialization_failures', super().ignore_node_deserialization_failures())

    def scheduler_helpers_minimal(self) -> int:
        return self.__config.get_option_noasync('core.minimum_idle_helpers', super().scheduler_helpers_minimal())


class SchedulerConfigProviderFileOverrides(SchedulerConfigProviderFile):
    def __init__(
            self,
            *,
            main_db_location: Optional[str] = None,
            main_db_connection_timeout: Optional[float] = None,
            legacy_server_address: Optional[Tuple[str, int]] = None,
            message_processor_address: Optional[Tuple[str, int]] = None,
            ui_address: Optional[Tuple[str, int]] = None,
            do_broadcast: Optional[bool] = None,
            broadcast_interval: Optional[float] = None,
            minimal_idle_helpers: Optional[int] = None,
    ):
        super().__init__()
        self.__main_db_location_override = main_db_location
        self.__main_db_connection_timeout = main_db_connection_timeout
        self.__do_broadcast_override = do_broadcast
        self.__broadcast_interval_override = broadcast_interval
        self.__minimal_idle_helpers = minimal_idle_helpers
        self.__legacy_address = legacy_server_address
        self.__message_processor_address = message_processor_address
        self.__ui_address = ui_address

    def main_database_location(self) -> str:
        return self.__main_db_location_override or super().main_database_location()
    
    def main_database_connection_timeout(self) -> float:
        return self.__main_db_connection_timeout or super().main_database_connection_timeout()
    
    def _config_do_broadcast(self) -> bool:
        return self.__do_broadcast_override if self.__do_broadcast_override is not None else super()._config_do_broadcast()

    def _config_broadcast_interval(self) -> float:
        return self.__broadcast_interval_override if self.__broadcast_interval_override is not None else super()._config_broadcast_interval()

    def scheduler_helpers_minimal(self) -> int:
        return self.__minimal_idle_helpers or super().scheduler_helpers_minimal()

    def legacy_server_address(self) -> Optional[Tuple[str, int]]:
        return self.__legacy_address or super().legacy_server_address()

    def server_message_addresses(self) -> Tuple[Tuple[str, int], ...]:
        if self.__message_processor_address is None:
            return super().server_message_addresses()
        ret = []
        for server_ip in self._expand_catchall_address(self.__message_processor_address[0]):
            addr = (server_ip, self.__message_processor_address[1])
            ret.append(addr)
        return tuple(ret)

    def server_ui_address(self) -> Tuple[str, int]:
        return self.__ui_address or super().server_ui_address()
