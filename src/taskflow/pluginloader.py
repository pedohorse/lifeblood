import os
import importlib.util

from . import logging

plugins = {}

logger = logging.getLogger('plugin_loader')


def init():
    logger.info('loading core plugins')
    global plugins
    plugins = {}
    plugin_paths = []
    core_plugins_path = os.path.join(os.path.dirname(__file__), 'core_nodes')
    stock_plugins_path = os.path.join(os.path.dirname(__file__), 'stock_nodes')
    plugin_paths.append(core_plugins_path)
    plugin_paths.append(stock_plugins_path)
    for plugin_path in plugin_paths:
        for filename in os.listdir(plugin_path):
            filebasename, fileext = os.path.splitext(filename)
            if fileext != '.py':
                continue
            mod_spec = importlib.util.spec_from_file_location(f'taskflow.coreplugins.{filebasename}',
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
            plugins[filebasename] = mod
    logger.info('loaded plugins:\n' + '\n\t'.join(plugins.keys()))


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
