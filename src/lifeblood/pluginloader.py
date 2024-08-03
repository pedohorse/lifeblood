import os
import sys
import re
import hashlib
import importlib.util
import platform
import toml
from pathlib import Path

from .basenode import BaseNode
from .node_dataprovider_base import NodeDataProvider
from .snippets import NodeSnippetData
from . import logging, plugin_info

from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union, Set, Sequence


class PluginNodeDataProvider(NodeDataProvider):
    __instance = None

    def __init__(self, plugin_paths: Sequence[Tuple[Path, str]]):
        """
        Plugin Node Data Provider will search for plugins in given locations.
        """
        if self.__instance is not None:
            # TODO: not very nice design, since it modifies os.environ,
            #  so for now i force single instance, but this needs refactoring!
            raise RuntimeError("cannot have more than one PluginNodeDataProvider instance, as it manages global state")

        self.__plugins = {}
        self.__presets: Dict[str, Dict[str, NodeSnippetData]] = {}
        # map of node type -2->
        #   preset_name -2->
        #     dict of parameter name -2-> value
        self.__nodes_settings: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.__default_settings_config: Dict[str, str] = {}
        self.__plugin_file_hashes = {}

        # package is identified by it's path, but it's handy to address them by short names
        # short name is generated from dir name. packages can have same dir names, but then
        # only first one will get into this locations dict
        self.__package_locations: Dict[str, Path] = {}

        self.logger = logging.get_logger('plugin_loader')

        # now do initial scannings

        self.logger.info('loading core plugins')
        self.__plugins = {}

        self.__loaded_package_paths: List[Path] = []
        # load all plugins
        for plugin_path, plugin_category in reversed(plugin_paths):
            for filepath in plugin_path.iterdir():
                if filepath.is_dir():
                    self._install_package(filepath, plugin_category)
                    self.__loaded_package_paths.insert(0, filepath)
                else:
                    if filepath.suffix != '.py':
                        continue
                    self._install_node(filepath, plugin_category)

        self.logger.info('loaded node types:\n\t' + '\n\t'.join(self.__plugins.keys()))
        self.logger.info('loaded node presets:\n\t' + '\n\t'.join(f'{pkg}::{label}' for pkg, pkgdata in self.__presets.items() for label in pkgdata.keys()))

        # check default settings
        bad_defaults = []
        for node_type, settings_name in self.__default_settings_config.items():
            if settings_name not in self.__nodes_settings.get(node_type, {}):
                self.logger.warning(f'"{settings_name}" is set as default for "{node_type}", but no such settings is loaded')
                bad_defaults.append(node_type)
                continue

    def _install_node(self, filepath: Path, plugin_category: str, parent_package=None):
        """

        :param filepath:
        :param plugin_category:
        :param parent_package: path to the base of the package, if this plugin is part of one, else - None
        :return:
        """
        filebasename = filepath.stem

        # calc module hash
        hasher = hashlib.md5()
        with open(filepath, 'rb') as f:
            hasher.update(f.read())
        plugin_hash = hasher.hexdigest()

        modpath = f'lifeblood.nodeplugins.{plugin_category}.{filebasename}'
        mod_spec = importlib.util.spec_from_file_location(modpath, filepath)
        try:
            mod = importlib.util.module_from_spec(mod_spec)
            mod_spec.loader.exec_module(mod)
            pluginfo = plugin_info.PluginInfo(filepath, plugin_hash, plugin_category, parent_package)
            mod._plugin_info = pluginfo
        except:
            self.logger.exception(f'failed to load plugin "{filebasename}". skipping.')
            return
        for requred_attr in ('node_class',):
            if not hasattr(mod, requred_attr):
                self.logger.error(f'error loading plugin "{filebasename}". '
                                  f'required method {requred_attr} is missing.')
                return
        node_class = mod.node_class()
        node_class._plugin_data = pluginfo
        self.__plugins[node_class.type_name()] = mod
        self.__plugin_file_hashes[mod.node_class().type_name()] = plugin_hash

        # TODO: what if it's overriding existing module?
        sys.modules[modpath] = mod

    def _install_package(self, package_path: Path, plugin_category: str):
        """
        package structure:
            [package_name:dir]
            |_bin
            | |_any                 <- this is always added to PATH
            | |_system-arch1        <- these are added to PATH only if system+arch match
            | |_system-arch2        <-/
            |_python
            | |_X           <- these are added to PYTHONPATH based on X.Y
            | |_X.Y         <-/
            |_nodes
            | |_node1.py    <- these are loaded as usual node plugins
            | |_node2.py    <-/
            |_data          <- just a convenient place to store shit, can be accessed with data from plugin
            |_settings      <- for future saved nodes settings
            | |_defaults.toml  <- config containing default node settings for node types
            | |_node_type_name1
            | | |_settings1.lbs
            | | |_settings2.lbs
            | |_node_type_name2
            | | |_settings1.lbs
            | | |_settings2.lbs
            |_whatever_file1.lol
            |_whatever_dir1
              |_whatever_file2.lol

        :param package_path:
        :param plugin_category:
        :return:
        """
        package_name = package_path.name
        if package_name not in self.__package_locations:  # read logic of this up
            self.__package_locations[package_name] = package_path
        # add extra bin paths
        extra_bins = []
        for subbin in (f'{platform.system().lower()}-{platform.machine().lower()}', 'any'):
            bin_base_path = package_path / 'bin' / subbin
            if not bin_base_path.exists():
                continue
            extra_bins.append(bin_base_path)
        if extra_bins:
            os.environ['PATH'] = os.pathsep.join(str(x) for x in extra_bins) + os.environ['PATH']  # TODO: this should only be accessible to that one plugin

        # install extra python modules
        python_base_path = package_path / 'python'
        if python_base_path.exists():
            sysver = sys.version_info
            pyvers = [tuple(int(y) for y in x.name.split('.')) for x in python_base_path.iterdir() if x.name.isdigit() or re.match(r'^\d+\.\d+$', x.name)]
            pyvers = [x for x in pyvers if x[0] == sysver.major
                                           and (len(x) < 2 or x[1] == sysver.minor)
                                           and (len(x) < 3 or x[2] == sysver.micro)]
            pyvers = sorted(pyvers, key=lambda x: len(x), reverse=True)
            for pyver in pyvers:
                extra_python_str = str(python_base_path / '.'.join(str(x) for x in pyver))
                sys.path.append(extra_python_str)

                # TODO: this is questionable, this will affect all child processes, we don't want that, this should only be accessible to that one plugin
                os.environ['PYTHONPATH'] = os.pathsep.join((extra_python_str, os.environ['PYTHONPATH'])) if 'PYTHONPATH' in os.environ else extra_python_str

        # install nodes
        nodes_path = package_path / 'nodes'
        if nodes_path.exists():
            for filepath in nodes_path.iterdir():
                if filepath.suffix != '.py':
                    continue
                self._install_node(filepath, plugin_category, package_path)

        # install presets
        presets_path = package_path / 'presets'
        if presets_path.exists():
            for filepath in presets_path.iterdir():
                if filepath.suffix != '.lbp':
                    continue
                try:
                    with open(filepath, 'rb') as f:
                        snippet = NodeSnippetData.deserialize(f.read())
                    snippet.add_tag('preset')
                except Exception as e:
                    self.logger.error(f'failed to load snippet "{filepath.stem}", error: {str(e)}')
                    continue

                if package_name not in self.__presets:
                    self.__presets[package_name] = {}
                self.__presets[package_name][snippet.label] = snippet

        # install node settings
        settings_path = package_path / 'settings'
        if settings_path.exists():
            for nodetype_path in settings_path.iterdir():
                if not nodetype_path.is_dir():
                    # ignore files, just look for dirs
                    continue
                if nodetype_path.name not in self.__nodes_settings:
                    self.__nodes_settings[nodetype_path.name] = {}
                for preset_filepath in nodetype_path.iterdir():
                    if preset_filepath.suffix != '.lbs':
                        continue
                    try:
                        with open(preset_filepath, 'r') as f:
                            self.__nodes_settings[nodetype_path.name][preset_filepath.stem] = toml.load(f)
                    except Exception as e:
                        self.logger.error(f'failed to load settings {nodetype_path.name}/{preset_filepath.stem}, error: {str(e)}')

            settings_defaults_config_path = settings_path / 'defaults.toml'
            if settings_defaults_config_path.exists():
                with open(settings_defaults_config_path) as f:
                    settings_defaults = toml.load(f)
                self.__default_settings_config.update(settings_defaults)

    def plugin_hash(self, plugin_name) -> str:
        return self.__plugin_file_hashes[plugin_name]

    def node_settings_names(self, type_name: str) -> Set[str]:
        if type_name not in self.__nodes_settings:
            return set()
        return set(self.__nodes_settings[type_name].keys())

    def node_settings(self, type_name: str, settings_name: str) -> dict:
        return self.__nodes_settings[type_name][settings_name]

    def node_type_names(self) -> Set[str]:
        return set(self.__plugins.keys())

    def node_class(self, type_name) -> Type[BaseNode]:
        return self.__plugins[type_name].node_class()

    def node_factory(self, node_type: str) -> Callable[[str], BaseNode]:
        def constructor(node_name) -> BaseNode:
            node = self.node_class(node_type)(node_name)
            if (settings_name := self.__default_settings_config.get(node_type)) and settings_name is not None:
                if node_type not in self.__nodes_settings or settings_name not in self.__nodes_settings[node_type]:
                    self.logger.warning(f'node type "{node_type}" has default setting "{settings_name}", but the setting itself is missing')
                else:
                    node.apply_settings(self.__nodes_settings[node_type][settings_name])
            return node

        return constructor

    def has_node_factory(self, node_type: str) -> bool:
        return node_type in self.node_type_names()

    def node_preset_packages(self) -> Set[str]:
        return set(self.__presets.keys())

    # node presets -
    def node_preset_names(self, package_name: str) -> Set[str]:
        return set(self.__presets[package_name])

    def node_preset(self, package_name: str, preset_name: str) -> NodeSnippetData:
        return self.__presets[package_name][preset_name]

    def __expand_package_path(self, package_name_or_path) -> Path:
        if isinstance(package_name_or_path, str) and package_name_or_path in self.__package_locations:
            package_name_or_path = self.__package_locations[package_name_or_path]
        else:
            package_name_or_path = Path(package_name_or_path)
        if package_name_or_path not in self.__package_locations.values():
            raise RuntimeError('no package with that name or path found')
        return package_name_or_path

    def loaded_packages_paths(self) -> Tuple[Path, ...]:
        return tuple(self.__loaded_package_paths)

    def add_settings_to_existing_package(self, package_name_or_path: Union[str, Path], node_type_name: str, settings_name: str, settings: Dict[str, Any]):
        package_name_or_path = self.__expand_package_path(package_name_or_path)

        # at this point package_name_or_path is path
        assert package_name_or_path.exists()
        base_path = package_name_or_path / 'settings' / node_type_name
        if not base_path.exists():
            base_path.mkdir(parents=True, exist_ok=True)
        with open(base_path / (settings_name + '.lbs'), 'w') as f:
            toml.dump(settings, f)
        if package_name_or_path not in self.__loaded_package_paths:
            return  # if we modified not loaded package - no need to update configuration
        # add to settings
        # TODO: fix, settings taking effect depends on loading order.
        #  so far it's only used for changing leftmost package, so the problem does not emerge
        if package_name_or_path == self.__loaded_package_paths[0]:
            self.__nodes_settings.setdefault(node_type_name, {})[settings_name] = settings
        else:
            raise NotImplementedError("TBD")

    def set_settings_as_default_in_existing_package(self, package_name_or_path: Union[str, Path], node_type_name: str, settings_name: Optional[str]):
        """

        :param package_name_or_path: existing package path to set defaults in
        :param node_type_name:
        :param settings_name: if None - unset any defaults
        :return:
        """
        package_name_or_path = self.__expand_package_path(package_name_or_path)

        if node_type_name not in self.__nodes_settings:
            raise RuntimeError(f'node type "{self.__nodes_settings}" is unknown')
        if settings_name is not None and settings_name not in self.__nodes_settings[node_type_name]:
            raise RuntimeError(f'node type "{self.__nodes_settings}" doesn\'t have settings "{settings_name}"')
        config_path = package_name_or_path / 'settings' / 'defaults.toml'
        # read existing
        settings_defaults = {}
        if config_path.exists():
            # TODO: refactor this module to work with Package representations
            #  so that when something is updated in any package - final data can be layered fast
            with open(config_path, 'r') as f:
                settings_defaults = toml.load(f)
        if settings_name is None:
            settings_defaults.pop(node_type_name)
        else:
            settings_defaults[node_type_name] = settings_name
        # save config
        config_path.parent.mkdir(parents=True, exist_ok=True)  # ensure it exists
        with open(config_path, 'w') as f:
            toml.dump(settings_defaults, f)

        if package_name_or_path not in self.__loaded_package_paths:
            return  # if we modified not loaded package - no need to update configuration

        # TODO: fix, settings taking effect depends on loading order.
        #  so far it's only used for changing leftmost package, so the problem does not emerge
        if package_name_or_path == self.__loaded_package_paths[0]:
            if settings_name is None:
                if node_type_name in self.__default_settings_config:
                    self.__default_settings_config.pop(node_type_name)  # TODO: this is incorrect in case other packages have opinions on that
            else:
                self.__default_settings_config[node_type_name] = settings_name
        else:
            raise NotImplementedError("TBD")
