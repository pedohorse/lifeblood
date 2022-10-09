import os
import sys
import re
import hashlib
import importlib.util
import platform
import toml
from pathlib import Path

from .snippets import NodeSnippetData
from . import logging, plugin_info, paths

from typing import List, Tuple, Dict, Any, Union, Optional


plugins = {}
presets: Dict[str, Dict[str, NodeSnippetData]] = {}
# map of node type -2->
#   preset_name -2->
#     dict of parameter name -2-> value
nodes_settings: Dict[str, Dict[str, Dict[str, Any]]] = {}
default_settings_config: Dict[str, str] = {}
__plugin_file_hashes = {}

# package is identified by it's path, but it's handy to address them by short names
# short name is generated from dir name. packages can have same dir names, but then
# only first one will get into this locations dict
__package_locations: Dict[str, Path] = {}

logger = logging.get_logger('plugin_loader')


def _install_node(filepath, plugin_category, parent_package=None):
    """

    :param filepath:
    :param plugin_category:
    :param parent_package: path to the base of the package, if this plugin is part of one, else - None
    :return:
    """
    filename = os.path.basename(filepath)
    filebasename, fileext = os.path.splitext(filename)

    modpath = f'lifeblood.nodeplugins.{plugin_category}.{filebasename}'
    mod_spec = importlib.util.spec_from_file_location(modpath, filepath)
    try:
        mod = importlib.util.module_from_spec(mod_spec)
        mod_spec.loader.exec_module(mod)
        mod._plugin_info = plugin_info.PluginInfo(filepath, parent_package)
    except:
        logger.exception(f'failed to load plugin "{filebasename}". skipping.')
    for requred_attr in ('node_class',):
        if not hasattr(mod, requred_attr):
            logger.error(f'error loading plugin "{filebasename}". '
                         f'required method {requred_attr} is missing.')
            continue
    plugins[mod.node_class().type_name()] = mod
    hasher = hashlib.md5()
    with open(filepath, 'rb') as f:
        hasher.update(f.read())
    __plugin_file_hashes[mod.node_class().type_name()] = hasher.hexdigest()
    sys.modules[modpath] = mod


def _install_package(package_path, plugin_category):
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
        |_settings      <- for future saved nodes settings. not implemented yet
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
    package_name = os.path.basename(package_path)
    global __package_locations
    if package_name not in __package_locations:  # read logic of this up
        __package_locations[package_name] = Path(package_path)
    # add extra bin paths
    extra_bins = []
    for subbin in (f'{platform.system().lower()}-{platform.machine().lower()}', 'any'):
        bin_base_path = os.path.join(package_path, 'bin', subbin)
        if not os.path.exists(bin_base_path):
            continue
        extra_bins.append(bin_base_path)
    if extra_bins:
        os.environ['PATH'] = os.pathsep.join(extra_bins) + os.environ['PATH']

    # install extra python modules
    python_base_path = os.path.join(package_path, 'python')
    if os.path.exists(python_base_path):
        sysver = sys.version_info
        pyvers = [tuple(int(y) for y in x.split('.')) for x in os.listdir(python_base_path) if x.isdigit() or re.match(r'^\d+\.\d+$', x)]
        pyvers = [x for x in pyvers if x[0] == sysver.major
                                       and (len(x) < 2 or x[1] == sysver.minor)
                                       and (len(x) < 3 or x[2] == sysver.micro)]
        pyvers = sorted(pyvers, key=lambda x: len(x), reverse=True)
        for pyver in pyvers:
            extra_python = os.path.join(python_base_path, '.'.join(str(x) for x in pyver))
            sys.path.append(extra_python)
            os.environ['PYTHONPATH'] = os.pathsep.join((extra_python, os.environ['PYTHONPATH']))

    # install nodes
    nodes_path = os.path.join(package_path, 'nodes')
    if os.path.exists(nodes_path):
        for filename in os.listdir(nodes_path):
            filebasename, fileext = os.path.splitext(filename)
            if fileext != '.py':
                continue
            _install_node(os.path.join(nodes_path, filename), plugin_category, package_path)

    # install presets
    presets_path = os.path.join(package_path, 'presets')
    if os.path.exists(presets_path):
        for filename in os.listdir(presets_path):
            filebasename, fileext = os.path.splitext(filename)
            if fileext != '.lbp':
                continue
            try:
                with open(os.path.join(presets_path, filename), 'rb') as f:
                    snippet = NodeSnippetData.deserialize(f.read())
                snippet.add_tag('preset')
            except Exception as e:
                logger.error(f'failed to load snippet {filebasename}, error: {str(e)}')
                continue

            if package_name not in presets:
                presets[package_name] = {}
            presets[package_name][snippet.label] = snippet

    # install node settings
    settings_path = os.path.join(package_path, 'settings')
    if os.path.exists(settings_path):
        for nodetype_name in os.listdir(settings_path):
            if nodetype_name not in nodes_settings:
                nodes_settings[nodetype_name] = {}
            nodetype_path = os.path.join(settings_path, nodetype_name)
            for preset_filename in os.listdir(nodetype_path):
                preset_name, fileext = os.path.splitext(preset_filename)
                if fileext != '.lbs':
                    continue
                try:
                    with open(os.path.join(nodetype_path, preset_filename), 'r') as f:
                        nodes_settings[nodetype_name][preset_name] = toml.load(f)
                except Exception as e:
                    logger.error(f'failed to load settings {nodetype_name}/{preset_name}, error: {str(e)}')


def init():
    logger.info('loading core plugins')
    global plugins
    plugins = {}
    plugin_paths: List[Tuple[str, str]] = []  # list of tuples of path to dir, plugin category
    core_plugins_path = os.path.join(os.path.dirname(__file__), 'core_nodes')
    stock_plugins_path = os.path.join(os.path.dirname(__file__), 'stock_nodes')
    custom_plugins_path = paths.config_path('', 'custom_plugins')
    plugin_paths.append((core_plugins_path, 'core'))
    plugin_paths.append((stock_plugins_path, 'stock'))
    (custom_plugins_path/'custom_default').mkdir(parents=True, exist_ok=True)

    plugin_paths.append((str(custom_plugins_path), 'user'))


    for plugin_path, plugin_category in plugin_paths:
        for filename in os.listdir(plugin_path):
            filepath = os.path.join(plugin_path, filename)
            if os.path.isdir(filepath):
                _install_package(filepath, plugin_category)
            else:
                filebasename, fileext = os.path.splitext(filename)
                if fileext != '.py':
                    continue
                _install_node(filepath, plugin_category)

    logger.info('loaded node types:\n\t' + '\n\t'.join(plugins.keys()))
    logger.info('loaded node presets:\n\t' + '\n\t'.join(f'{pkg}::{label}' for pkg, pkgdata in presets.items() for label in pkgdata.keys()))

    # load default settings
    default_settings_config_path = paths.config_path('defaults.toml', 'scheduler.nodes')
    global default_settings_config
    if default_settings_config_path.exists():
        with open(default_settings_config_path) as f:
            default_settings_config = toml.load(f)

        bad_defaults = []
        for node_type, settings_name in default_settings_config.items():
            if settings_name not in nodes_settings.get(node_type, {}):
                logger.warning(f'"{settings_name}" is set as default for "{node_type}", but no such settings is loaded')
                bad_defaults.append(node_type)
                continue


def plugin_hash(plugin_name) -> str:
    return __plugin_file_hashes[plugin_name]


def add_settings_to_existing_package(package_name_or_path: Union[str, Path], node_type_name: str, settings_name: str, settings: Dict[str, Any]):

    if isinstance(package_name_or_path, str) and package_name_or_path in __package_locations:
        package_name_or_path = __package_locations[package_name_or_path]
    else:
        package_name_or_path = Path(package_name_or_path)
    if package_name_or_path not in __package_locations.values():
        raise RuntimeError('no package with that name or pathfound')

    # at this point package_name_or_path is path
    assert(package_name_or_path.exists())
    base_path = package_name_or_path / 'settings' / node_type_name
    if not base_path.exists():
        base_path.mkdir(parents=True, exist_ok=True)
    with open(base_path / (settings_name + '.lbs'), 'w') as f:
        toml.dump(settings, f)

    # add to settings
    nodes_settings.setdefault(node_type_name, {})[settings_name] = settings


def set_settings_as_default(node_type_name: str, settings_name: Optional[str]):
    """

    :param node_type_name:
    :param settings_name: if None - unset any defaults
    :return:
    """
    if node_type_name not in nodes_settings:
        raise RuntimeError(f'node type "{nodes_settings}" is unknown')
    if settings_name is not None and settings_name not in nodes_settings[node_type_name]:
        raise RuntimeError(f'node type "{nodes_settings}" doesn\'t have settings "{settings_name}"')
    if settings_name is None and node_type_name in default_settings_config:
        del default_settings_config[node_type_name]
    else:
        default_settings_config[node_type_name] = settings_name
    with open(paths.config_path('defaults.toml', 'scheduler.nodes'), 'w') as f:
        toml.dump(default_settings_config, f)


def create_node(type_name: str, name, scheduler_parent, node_id):
    """
    this function is a global node creation point.
    it has to be available somewhere global, so plugins loaded from dynamically created modules have an entry point for pickle
    """
    if type_name not in plugins:
        if type_name == 'basenode':  # debug case! base class should never be created directly!
            logger.warning('creating BASENODE. if it\'s not for debug/test purposes - it\'s bad!')
            from .basenode import BaseNode
            node = BaseNode(name)
        raise RuntimeError('unknown plugin')
    node = plugins[type_name].node_class()(name)
    # now set defaults, before parent is set to prevent ui callbacks to parent
    if type_name in default_settings_config:
        node.apply_settings(default_settings_config[type_name])
    node._set_parent(scheduler_parent, node_id)
    return node
