import os
import sys
import hashlib
import importlib.util

from . import logging

from typing import List, Tuple


plugins = {}
__plugin_file_hashes = {}

logger = logging.get_logger('plugin_loader')


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
            filebasename, fileext = os.path.splitext(filename)
            if fileext != '.py':
                continue
            modpath = f'lifeblood.nodeplugins.{plugin_category}.{filebasename}'
            mod_spec = importlib.util.spec_from_file_location(modpath,
                                                              os.path.join(plugin_path, filename))
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
            with open(os.path.join(plugin_path, filename), 'rb') as f:
                hasher.update(f.read())
            __plugin_file_hashes[mod.node_class().type_name()] = hasher.hexdigest()
            sys.modules[modpath] = mod
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
