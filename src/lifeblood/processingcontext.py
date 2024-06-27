from types import MappingProxyType

from .attribute_serialization import deserialize_attributes_core
from .environment_resolver import EnvironmentResolverArguments

from typing import Dict, Optional, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from .basenode import BaseNode
    from .uidata import Parameter


class ProcessingContext:
    class TaskWrapper:
        def __init__(self, task_dict: dict):
            self.__attributes = deserialize_attributes_core(task_dict.get('attributes', '{}'))
            self.__stuff = task_dict

        def __getitem__(self, item):
            return self.__attributes[item]

        def __getattr__(self, item):
            if item in self.__stuff:
                return self.__stuff[item]
            raise AttributeError(f'task has no field {item}')

        def get(self, item, default):
            return self.__attributes.get(item, default)

    class NodeWrapper:
        def __init__(self, node: "BaseNode", context: "ProcessingContext"):
            self.__parameters: Dict[str, "Parameter"] = {x.name(): x for x in node.get_ui().parameters()}
            self.__attrs = {'name': node.name(), 'label': node.label()}
            self.__context = context

        def __getitem__(self, item):
            return self.__parameters[item].value(self.__context)

        def __getattr__(self, item):
            if item in self.__attrs:
                return self.__attrs[item]
            raise AttributeError(f'node has no field {item}')

    class ConfigWrapper:
        def __init__(self, node_config):
            self.__node_config = node_config

        def get(self, key, default=None):
            return self.__node_config.get(key, default)

        def __getitem__(self, item):
            return self.get(item)

    def __init__(self, node: "BaseNode", task_dict: dict, node_config: Dict[str, Union[str, int, float, list, dict]]):
        """
        All information node can access during processing.
        This is read-only.
        All modifications are to be done through ProcessingResult

        :param node_config: extra mapping that node can access through parameter expressions
        """
        task_dict = dict(task_dict)
        self.__task_attributes = deserialize_attributes_core(task_dict.get('attributes', '{}'))
        self.__task_dict = task_dict
        self.__task_wrapper = ProcessingContext.TaskWrapper(task_dict)
        self.__node_wrapper = ProcessingContext.NodeWrapper(node, self)
        self.__env_args = EnvironmentResolverArguments.deserialize(task_dict.get('environment_resolver_data')) if task_dict.get('environment_resolver_data') is not None else None
        self.__conf_wrapper = ProcessingContext.ConfigWrapper(node_config)
        self.__node = node

    def param_value(self, param_name: str):
        return self.__node.get_ui().parameter(param_name).value(self)

    def locals(self):
        """
        locals to be available during expression evaluation
        node - represents current node
            node['paramname'] returns the value of parameter paramname
            node.name returns node name
            node.label returns node's label
        task - represents task, for which expression is being evaluated
            task['attrname'] returns the value of attrname attribute of current task
            task.fieldname returns task database field called fieldname
        config - general config for this particular node type
            config['entryname'] of config.get('entryname', defaultval) returns entryname from config, or defaultval if entryname does not exist

        :return:
        """
        return {'task': self.__task_wrapper, 'node': self.__node_wrapper, 'config': self.__conf_wrapper}

    def task_attribute(self, attrib_name: str):
        return self.__task_attributes[attrib_name]

    def task_has_attribute(self, attrib_name: str):
        return attrib_name in self.__task_attributes

    def task_attributes(self) -> MappingProxyType:
        return MappingProxyType(self.__task_attributes)

    def task_environment_resolver_arguments(self) -> Optional[EnvironmentResolverArguments]:
        return self.__env_args

    def task_field(self, field_name: str, default_value=None):
        return self.__task_dict.get(field_name, default_value)

    def task_has_field(self, field_name: str):
        return field_name in self.__task_dict

    def task_id(self):
        return self.__task_dict.get('id')

    def task_name(self):
        return self.__task_dict.get('name')
