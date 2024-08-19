import os
from pathlib import Path
from lifeblood.scheduler_config_provider_default import SchedulerConfigProviderDefaults
from lifeblood.logging import get_logger
from .worker_resource_definition import WorkerResourceDefinition, WorkerResourceDataType, WorkerDeviceTypeDefinition
from . import paths
from .config import Config
from .nethelpers import all_interfaces
from .exceptions import SchedulerConfigurationError
from .config import create_default_user_config_file, get_local_scratch_path
from .text import escape

from typing import Dict, List, Mapping, Optional, Tuple


class SchedulerConfigProviderFile(SchedulerConfigProviderDefaults):
    """
    this subclass implements SOME default values for scheduler configuration
    that subclasses of this class can fall back to

    Note: Plugin provider expects to own underlying configs, meaning:
     - it SHOULD read all underlying files once to avoid inconsistencies if config is changed by outer forces
     - Currently the config is sorta immutable.
        - IF config reread/update is ever implemented - a mechanism to inform config users about the change must be designed!
    """

    @classmethod
    def generate_default_config_text(cls) -> str:
        dcp = SchedulerConfigProviderDefaults()
        ping_interval, ping_idle_interval, ping_off_interval, dormant_ping_multiplier = dcp.ping_intervals()
        return default_config.format(
            server_ip='0.0.0.0',  # special case, this will be transformed into a proper ip:port pair tuple of tuples
            server_port=dcp.legacy_server_address()[1],
            ui_ip=dcp.server_ui_address()[0],
            ui_port=dcp.server_ui_address()[1],
            do_broadcast='true' if dcp.broadcast_interval() is not None else 'false',
            broadcast_interval=dcp.broadcast_interval() or 0,
            scratch_location=escape(get_local_scratch_path(), '\\"'),
            hwid_ban_timeout=dcp.hardware_ban_timeout(),
            ping_interval=ping_interval,
            ping_idle_interval=ping_idle_interval,
            ping_off_interval=ping_off_interval,
            dormant_ping_multiplier=dormant_ping_multiplier,
            store_logs_externally='true' if dcp.external_log_location() is not None else 'false',
            store_logs_externally_location=dcp.external_log_location() or '/path/to/dir/where/to/store/logs',
            invocation_attempts=dcp.invocation_attempts(),
            housekeeping_interval=dcp.task_processor_housekeeping_interval(),
            ignore_node_deserialization_failures='true' if dcp.ignore_node_deserialization_failures() else 'false',
            minimum_idle_helpers=dcp.scheduler_helpers_minimal(),
        )

    def __init__(
            self,
            main_config: Config,
            nodes_config: Config,
    ):
        logger = get_logger('scheduler.config_provider')

        self.__config = main_config
        self.__nodes_config = nodes_config

        # cache node config mappings to get them fast when processing context is needed
        #  this is the lazy solution
        self.__node_config_cache: Dict[str, dict] = {}

        node_plugin_paths = []
        for path_str in os.environ.get('LIFEBLOOD_PLUGIN_PATH', '').split(os.pathsep):
            if path_str == '':  # skip empty paths
                continue
            path = Path(path_str)
            if not path.is_absolute():
                logger.warning(f'plugin path must be absolute, skipping "{path_str}"')
                continue
            node_plugin_paths.append(path)
        self.__node_plugin_paths: Tuple[Path, ...] = tuple(node_plugin_paths)

    def main_database_location(self) -> str:
        return os.path.expanduser(
            self.__config.get_option_noasync('core.database.path', str(paths.default_main_database_location()))
        )

    def node_configuration(self, node_type_id: str) -> Mapping:
        if node_type_id not in self.__node_config_cache:

            self.__node_config_cache[node_type_id] = {
                **dict(self.__config.get_option_noasync(
                    'nodes.globals',
                    self.__config.get_option_noasync('scheduler.globals', {}))  # 'scheduler.globals' is deprecated name
                ),
                **dict(self.__nodes_config.get_option_noasync(f'{node_type_id}', {})),
            }
        return self.__node_config_cache[node_type_id]

    @classmethod
    def __parse_resource_definitions(cls, config_resources) -> Tuple[WorkerResourceDefinition, ...]:
        if not isinstance(config_resources, dict):
            raise RuntimeError('bad config schema: resource_definitions.per_machine must be a mapping')  # TODO: turn into config schema error or smth
        conf_2_type_mapping = {
            'int': WorkerResourceDataType.GENERIC_INT,
            'float': WorkerResourceDataType.GENERIC_FLOAT,
            'number': WorkerResourceDataType.GENERIC_FLOAT,
            'cpu': WorkerResourceDataType.SHARABLE_COMPUTATIONAL_UNIT,
            'mem': WorkerResourceDataType.MEMORY_BYTES,
            'memory': WorkerResourceDataType.MEMORY_BYTES,
        }
        res_defs = []
        for res_name, res_data in sorted(config_resources.items(), key=lambda pair: pair[0]):
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

    def hardware_resource_definitions(self) -> Tuple[WorkerResourceDefinition, ...]:
        # resource definitions
        config_resources = self.__config.get_option_noasync('resource_definitions.per_machine', None)
        if config_resources is None:  # use default resource definitions
            return super().hardware_resource_definitions()

        return self.__parse_resource_definitions(config_resources)

    def hardware_device_type_definitions(self) -> Tuple[WorkerDeviceTypeDefinition, ...]:
        config_devices = self.__config.get_option_noasync('device_type_definitions', None)
        if config_devices is None:  # use default resource definitions
            return super().hardware_device_type_definitions()
        if not isinstance(config_devices, dict):
            raise RuntimeError('bad config schema: resource_definitions.per_machine must be a mapping')  # TODO: turn into config schema error or smth

        dev_defs = []
        for dev_type_name, dev_type_res_config in sorted(config_devices.items(), key=lambda pair: pair[0]):
            dev_defs.append(WorkerDeviceTypeDefinition(
                dev_type_name,
                self.__parse_resource_definitions(dev_type_res_config),
            ))

        return tuple(dev_defs)

    def hardware_ban_timeout(self) -> float:
        return self.__config.get_option_noasync('data_access.hwid_ban_timeout', super().hardware_ban_timeout())

    def ping_intervals(self) -> Tuple[float, float, float, float]:
        def_vals = super().ping_intervals()
        return (
            self.__config.get_option_noasync('pinger.ping_interval', def_vals[0]),
            self.__config.get_option_noasync('pinger.ping_idle_interval', def_vals[1]),
            self.__config.get_option_noasync('pinger.ping_off_interval', def_vals[2]),
            self.__config.get_option_noasync('pinger.dormant_ping_multiplier', def_vals[3]),
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

    @staticmethod
    def _expand_catchall_address(address: str) -> List[str]:
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
        return self.__config.get_option_noasync('task_processor.invocation_attempts', super().invocation_attempts())

    def task_processor_housekeeping_interval(self) -> float:
        return self.__config.get_option_noasync('task_processor.housekeeping_interval', super().task_processor_housekeeping_interval())

    def ignore_node_deserialization_failures(self) -> bool:
        return self.__config.get_option_noasync('core.ignore_node_deserialization_failures', super().ignore_node_deserialization_failures())

    def scheduler_helpers_minimal(self) -> int:
        return self.__config.get_option_noasync('core.minimum_idle_helpers', super().scheduler_helpers_minimal())

    def node_data_provider_custom_plugins_path(self) -> Path:
        return paths.config_path('', 'custom_plugins')

    def node_data_provider_extra_plugin_paths(self) -> Tuple[Path, ...]:
        return self.__node_plugin_paths


class SchedulerConfigProviderFileOverrides(SchedulerConfigProviderFile):
    def __init__(
            self,
            main_config: Config,
            nodes_config: Config,
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
        super().__init__(
            main_config=main_config,
            nodes_config=nodes_config,
        )
        self.__main_db_location_override = main_db_location
        self.__main_db_connection_timeout = main_db_connection_timeout
        self.__do_broadcast_override = do_broadcast
        self.__broadcast_interval_override = broadcast_interval
        self.__minimal_idle_helpers = minimal_idle_helpers
        self.__legacy_address = legacy_server_address
        self.__message_processor_address = message_processor_address
        self.__ui_address = ui_address

    def main_database_location(self) -> str:
        return os.path.expanduser(self.__main_db_location_override) if self.__main_db_location_override is not None else super().main_database_location()
    
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


default_config = '''
## you can uncomment stuff below to specify some static values
## 
# [core]
# server_ip = "{server_ip}"
# server_port = {server_port}
# ui_ip = "{ui_ip}"
# ui_port = {ui_port}

## you can turn off scheduler broadcasting if you want to manually configure viewer and workers to connect
## to a specific address
# broadcast = {do_broadcast}
# broadcast_interval = {broadcast_interval}

## more fine-tuning options
# ignore_node_deserialization_failures = {ignore_node_deserialization_failures}
# minimum_idle_helpers = {minimum_idle_helpers}

[core.database]
## you can specify default database path, 
##  but this can be overridden with command line argument --db-path
# path = "/path/to/database.db"


[nodes.globals]
## entries from this section will be available to any node from config[key] 
##
## if you use more than 1 machine - you must change this to a network location shared among all workers
## by default it's set to scheduler's machine local temp path, and will only work for 1 machine setup 
global_scratch_location = "{scratch_location}"


# [data_access]
# hwid_ban_timeout = {hwid_ban_timeout}


# [task_processor]
# invocation_attempts = {invocation_attempts}
# housekeeping_interval = {housekeeping_interval}

# [pinger]
# ping_interval = {ping_interval}
# ping_idle_interval = {ping_idle_interval}
# ping_off_interval = {ping_off_interval}
# dormant_ping_multiplier = {dormant_ping_multiplier}

## uncomment line below to store task logs outside of the database
##  it works in a way that all NEW logs will be saved according to settings below
##  existing logs will be kept where they are
##  external logs will ALWAYS be looked for in location specified by store_logs_externally_location
##  so if you have ANY logs saved externally - you must keep store_logs_externally_location defined in the config, 
##    or those logs will be inaccessible
##  but you can safely move logs and change location in config accordingly, but be sure scheduler is not accessing them at that time
# [core.database]
# store_logs_externally = {store_logs_externally}
# store_logs_externally_location = {store_logs_externally_location}


# [resource_definitions.per_machine]
## you can define custom per-machine resources.
## default are cpu_count and cpu_mem that represents the number of CPU cores and main memory size.
## If you override this value - ALL default definitions will be override, so you need to add them back
## if you want to keep them
## defaults
# cpu_count.type = "cpu"
# cpu_count.description = "CPU core count"
# cpu_count.label = "CPU count"
# cpu_mem.type = "mem"
# cpu_mem.description = "RAM amount in bytes"
# cpu_mem.label = "CPU ram (GB)"
## add your own resources
# my_resource1.type = "float"
# my_resource1.description = "the amount of rubber ducks that fit into the chassis"

## you can define custom devices that machines can have.
## if you do so - you will override the default devices such as gpu,
## therefore you need to explicitly define them here
## you can do that by uncommenting the section below
## defaults
# [device_type_definitions.gpu]
# mem.type = "mem" 
# mem.description = "gpu device memory (VRAM)"
# mem.label = "Gpu Memory (GB)"
# opencl_ver.type = "float"
# opencl_ver.description = "OpenCL version required"
# opencl_ver.label = "OpenCL version"
# opencl_ver.default = 1.2
# cuda_cc.type = "float"
# cuda_cc.description = "CUDA Compute Capability required"
# cuda_cc.label = 'CUDA CC'
## add your own device devinitions like this:
# [device_type_definitions.plumbus]
# shmee.type = "int"
# shmee.description = "Shmee count"
# shmee.label = "Shmee"
# shmee.default = 2
# doognol.type = "float"
# doognol.description = "Fraction of deFunbling the doongnols"
# doognol.label = "Doongol (Zhi)"
# doognol.default = 0.5
'''
