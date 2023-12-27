from .base import TypeMetadata
from .node_dataprovider_base import NodeDataProvider
from .plugin_info import PluginInfo

from typing import Optional, TYPE_CHECKING, Tuple, Set


class NodeTypePluginMetadata:
    def __init__(self, plugin_info: PluginInfo):
        self.__package_name = plugin_info.package_name()
        self.__category = plugin_info.category()

    @property
    def package_name(self) -> Optional[str]:
        return self.__package_name

    @property
    def category(self) -> str:
        return self.__category


class NodeTypeMetadata(TypeMetadata):
    def __init__(self, node_data_provider: NodeDataProvider, node_type_name: str):
        self.__type_name = node_type_name
        node_class = node_data_provider.node_class(node_type_name)
        self.__plugin_info = NodeTypePluginMetadata(node_class.my_plugin())
        self.__label = node_class.label()
        self.__tags = set(node_class.tags())
        self.__description = node_class.description()
        self.__settings_names = tuple(node_data_provider.node_settings_names(node_type_name))

    @property
    def type_name(self) -> str:
        return self.__type_name

    @property
    def plugin_info(self) -> NodeTypePluginMetadata:
        return self.__plugin_info

    @property
    def label(self) -> Optional[str]:
        return self.__label

    @property
    def tags(self) -> Set[str]:
        return self.__tags

    @property
    def description(self) -> str:
        return self.__description

    @property
    def settings_names(self) -> Tuple[str, ...]:
        return self.__settings_names
