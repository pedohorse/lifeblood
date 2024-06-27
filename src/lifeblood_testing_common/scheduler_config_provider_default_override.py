from lifeblood.scheduler_config_provider_default import SchedulerConfigProviderDefaults
from lifeblood.nethelpers import all_interfaces

from typing import List, Optional, Tuple


class SchedulerConfigProviderOverrides(SchedulerConfigProviderDefaults):
    def __init__(
            self,
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

    def _config_do_broadcast(self) -> Optional[bool]:
        return self.__do_broadcast_override if self.__do_broadcast_override is not None else super()._config_do_broadcast()

    def _config_broadcast_interval(self) -> Optional[float]:
        return self.__broadcast_interval_override if self.__broadcast_interval_override is not None else super()._config_broadcast_interval()

    def scheduler_helpers_minimal(self) -> int:
        return self.__minimal_idle_helpers or super().scheduler_helpers_minimal()

    def legacy_server_address(self) -> Optional[Tuple[str, int]]:
        return self.__legacy_address or super().legacy_server_address()

    def _expand_catchall_address(self, address: str) -> List[str]:
        ret = []
        if address == '0.0.0.0':  # message_processor address must be addressable, no catchall
            for iface_ip in all_interfaces():
                ret.append(iface_ip)
        else:
            ret.append(address)
        return ret

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
