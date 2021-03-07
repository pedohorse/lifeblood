import os
import importlib.util


plugins = {}


def init():
    print('loading core plugins')
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
            mod = importlib.util.module_from_spec(mod_spec)
            mod_spec.loader.exec_module(mod)
            for requred_attr in ('create_node_object',):
                if not hasattr(mod, requred_attr):
                    print(f'error loading plugin "{filebasename}". '
                          f'required method {requred_attr} is missing.')
                    continue
            plugins[filebasename] = mod
    print('loaded plugins:\n', '\n\t'.join(plugins.keys()))


def create_node(plugin_name: str, name, scheduler_parent):
    if plugin_name not in plugins:
        if plugin_name == 'basenode':  # debug case! base class should never be created directly!
            print('creating BASENODE. if it\'s not for debug/test purposes - it\'s bad!')
            from .basenode import BaseNode
            return BaseNode(name, scheduler_parent)
        raise RuntimeError('unknown plugin')
    return plugins[plugin_name].create_node_object(name, scheduler_parent)

