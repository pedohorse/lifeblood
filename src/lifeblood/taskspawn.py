"""
this module contains TaskSpawn class used from scheduler nodes and
worker jobs to spawn new tasks
"""
import pickle
import asyncio
from io import BytesIO
from .environment_resolver import EnvironmentResolverArguments

from typing import Optional, Tuple, List, Iterable


class Unpickler(pickle.Unpickler):
    def find_class(self, module, name):
        if module in ('lifeblood_connection', 'lifeblood_runtime.submitting'):  # TODO: this becomes dirty as it scales... make this more generic!
            if name == 'TaskSpawn':
                return TaskSpawn
            elif name == 'NewTask':
                return NewTask
            elif name == 'EnvironmentResolverArguments':
                return EnvironmentResolverArguments
        return super(Unpickler, self).find_class(module, name)


class TaskSpawn:
    def __init__(self, name: str, source_invocation_id: Optional[int] = None, env_args: Optional[EnvironmentResolverArguments] = None, task_attributes: dict = None):
        """

        :param name:
        :param source_invocation_id:
        :param env_args: if None - will inherit parent's. if no parent and none, or if empty arguments- default worker's env wrapper will be used
        :param task_attributes:
        """
        self.__name = name
        self.__attributes = dict(task_attributes or {})
        self.__env_args = env_args
        self.__forced_node_task_id_pair = None
        self.__from_invocation_id = source_invocation_id
        self.__output = 'spawned'
        self._create_as_spawned = True
        self.__extra_groups = []
        self.__default_priority = None

    def create_as_spawned(self):
        return self._create_as_spawned

    def force_set_node_task_id(self, node_id, task_id):
        self.__forced_node_task_id_pair = (node_id, task_id)

    def forced_node_task_id(self) -> Optional[Tuple[int, int]]:
        return self.__forced_node_task_id_pair

    def source_invocation_id(self):
        return self.__from_invocation_id

    def set_node_output_name(self, new_name):
        self.__output = new_name

    def node_output_name(self) -> str:
        return self.__output

    def name(self) -> str:
        return self.__name

    def default_priority(self) -> Optional[float]:
        """
        This priority will be used only in case this task requires a default group creation
        If this task has nonempty list of groups to be assigned to - this default priority is

        :return: default priority
        """
        return self.__default_priority

    def add_extra_group_name(self, group_name: str) -> None:
        self.__extra_groups.append(group_name)

    def add_extra_group_names(self, group_names: Iterable[str]) -> None:
        self.__extra_groups += group_names

    def extra_group_names(self) -> List[str]:
        return self.__extra_groups

    def set_default_priority(self, priority: float):
        """
        This priority will be used only in case this task requires a default group creation
        If this task has nonempty list of groups to be assigned to - this default priority is
        """
        self.__default_priority = priority

    def set_name(self, name):
        self.__name = name

    def set_attribute(self, attr_name, attr_value):
        self.__attributes[attr_name] = attr_value

    def remove_attribute(self, attr_name):
        del self.__attributes[attr_name]

    def attribute_value(self, attr_name):
        return self.__attributes.get(attr_name, None)

    def _attributes(self):
        return self.__attributes

    def environment_arguments(self):
        return self.__env_args

    def set_environment_resolver(self, resolver_name, resolver_arguments):
        self.__env_args = EnvironmentResolverArguments(resolver_name, resolver_arguments)

    def serialize(self) -> bytes:
        return pickle.dumps(self)

    async def serialize_async(self) -> bytes:
        return await asyncio.get_event_loop().run_in_executor(None, self.serialize)

    @classmethod
    def deserialize(cls, data: bytes) -> "TaskSpawn":
        deserializer = Unpickler(BytesIO(data))
        return deserializer.load()

    @classmethod
    async def deserialize_async(cls, data: bytes) -> "TaskSpawn":
        return await asyncio.get_event_loop().run_in_executor(None, cls.deserialize, data)


class NewTask(TaskSpawn):
    def __init__(self, name: str, node_id: int, env_args: Optional[EnvironmentResolverArguments] = None, task_attributes: Optional[dict] = None, priority: float = 50.0):
        super(NewTask, self).__init__(name, None, env_args, task_attributes)
        self.set_node_output_name('main')
        self.force_set_node_task_id(node_id, None)
        self._create_as_spawned = False
        self.set_default_priority(priority)
