"""
this module contains TaskSpawn class used from scheduler nodes and
worker jobs to spawn new tasks
"""
import pickle


class TaskSpawn:
    def __init__(self, name, **attribs):
        self.__name = name
        self.__attributes = attribs

    def name(self):
        return self.__name

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

    def serialize(self) -> bytes:
        return pickle.dumps(self)

    @classmethod
    def deserialize(cls, data) -> "TaskSpawn":
        return pickle.loads(data)
