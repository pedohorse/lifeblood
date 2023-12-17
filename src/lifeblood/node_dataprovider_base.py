from pathlib import Path
from .basenode import BaseNode
from .snippets import NodeSnippetData

from typing import Any, Callable, Dict, Optional, Set, Tuple, Type, Union


class NodeDataProvider:
    def node_settings_names(self, type_name: str) -> Set[str]:
        raise NotImplementedError()

    def node_settings(self, type_name: str, settings_name: str) -> dict:
        raise NotImplementedError()

    def node_type_names(self) -> Set[str]:
        raise NotImplementedError()

    def node_class(self, type_name) -> Type[BaseNode]:
        raise NotImplementedError()

    def node_factory(self, node_type: str) -> Callable[[str], BaseNode]:
        raise NotImplementedError()

    def has_node_factory(self, node_type: str) -> bool:
        raise NotImplementedError()

    def node_preset_packages(self) -> Set[str]:
        raise NotImplementedError()

    # node presets -
    def node_preset_names(self, package_name: str) -> Set[str]:
        raise NotImplementedError()

    def node_preset(self, package_name: str, preset_name: str) -> NodeSnippetData:
        raise NotImplementedError()

    def add_settings_to_existing_package(self, package_name_or_path: Union[str, Path], node_type_name: str, settings_name: str, settings: Dict[str, Any]):
        raise NotImplementedError()

    def set_settings_as_default(self, node_type_name: str, settings_name: Optional[str]):
        raise NotImplementedError()
