from pathlib import Path

from typing import Optional


class PluginInfo:
    """
    class for getting information about a plugin

    """
    def __init__(self, file_path, parent_package=None):
        self.__file_path = Path(file_path)

        self.__parent_package = Path(parent_package) if parent_package is not None else None
        self.__parent_package_data = None
        if self.__parent_package is not None:
            self.__parent_package_data = self.__parent_package / 'data'
            if not self.__parent_package_data.exists():
                self.__parent_package_data = None

        self.__package_name = None

    def package_name(self) -> Optional[str]:
        if self.__parent_package is None:
            return None
        if self.__package_name is None:
            self.__package_name = self.__parent_package.name
        return self.__package_name

    def package_root(self) -> Optional[Path]:
        return self.__parent_package

    def package_data(self) -> Optional[Path]:
        return self.__parent_package_data

    def node_definition_file_path(self) -> Path:
        return self.__file_path

    def __str__(self):
        return f'Plugin from {self.node_definition_file_path()}, part of {self.package_name()}'
