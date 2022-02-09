import os
import sys
import re
import hashlib
import importlib.util

from . import logging

from typing import List, Tuple


plugins = {}
__plugin_file_hashes = {}

logger = logging.get_logger('plugin_loader')


def _install_node(filepath, plugin_category):
    filename = os.path.basename(filepath)
    filebasename, fileext = os.path.splitext(filename)

    modpath = f'lifeblood.nodeplugins.{plugin_category}.{filebasename}'
    mod_spec = importlib.util.spec_from_file_location(modpath, filepath)
    try:
        mod = importlib.util.module_from_spec(mod_spec)
        mod_spec.loader.exec_module(mod)
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
        |_python
        | |_X           <- these are added to PYTHONPATH based on X.Y
        | |_X.Y         <-/
        |_nodes
        | |_node1.py    <- these are loaded as usual node plugins
        | |_node2.py    <-/
        |_whatever_file1.lol
        |_whatever_dir1
          |_whatever_file2.lol

    :param package_path:
    :param plugin_category:
    :return:
    """
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
            sys.path.append(os.path.join(python_base_path, '.'.join(str(x) for x in pyver)))

    # install nodes
    nodes_path = os.path.join(package_path, 'nodes')
    if os.path.exists(nodes_path):
        for filename in os.listdir(nodes_path):
            filebasename, fileext = os.path.splitext(filename)
            if fileext != '.py':
                continue
            _install_node(os.path.join(nodes_path, filename), plugin_category)



def init():
    logger.info('loading core plugins')
    global plugins
    plugins = {}
    plugin_paths: List[Tuple[str, str]] = []  # list of tuples of path to dir, plugin category
    core_plugins_path = os.path.join(os.path.dirname(__file__), 'core_nodes')
    stock_plugins_path = os.path.join(os.path.dirname(__file__), 'stock_nodes')
    plugin_paths.append((core_plugins_path, 'core'))
    plugin_paths.append((stock_plugins_path, 'stock'))
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

    logger.info('loaded plugins:\n' + '\n\t'.join(plugins.keys()))


def plugin_hash(plugin_name) -> str:
    return __plugin_file_hashes[plugin_name]


def create_node(plugin_name: str, name, scheduler_parent, node_id):
    """
    this function is a global node creation point.
    it has to be available somewhere global, so plugins loaded from dynamically created modules have an entry point for pickle
    """
    if plugin_name not in plugins:
        if plugin_name == 'basenode':  # debug case! base class should never be created directly!
            logger.warning('creating BASENODE. if it\'s not for debug/test purposes - it\'s bad!')
            from .basenode import BaseNode
            node = BaseNode(name)
        raise RuntimeError('unknown plugin')
    node = plugins[plugin_name].node_class()(name)
    node._set_parent(scheduler_parent, node_id)
    return node
