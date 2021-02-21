"""
this module contains TaskSpawn class used from scheduler nodes and
worker jobs to spawn new tasks
"""
import pickle
import asyncio


class TaskSpawn:
    def __init__(self, name: str, parent_task_id: int, **attribs):
        self.__name = name
        self.__attributes = attribs
        self.__parent = parent_task_id

    def name(self) -> str:
        return self.__name

    def set_name(self, name):
        self.__name = name

    def set_attribute(self, attr_name, attr_value):
        self.__attributes[attr_name] = attr_value

    def remove_attribute(self, attr_name):
        del self.__attributes[attr_name]

    def attribute_value(self, attr_name):
        return self.__attributes.get(attr_name, None)

    def parent_task_id(self) -> int:
        return self.__parent

    def _attributes(self):
        return self.__attributes

    async def serialize(self) -> bytes:
        return await asyncio.get_event_loop().run_in_executor(None, pickle.dumps, self)

    @classmethod
    def deserialize(cls, data) -> "TaskSpawn":
        return pickle.loads(data)
