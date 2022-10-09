import uuid
import psutil
import pickle
import copy
from .misc import get_unique_machine_id

from typing import TYPE_CHECKING, Type
if TYPE_CHECKING:
    from .basenode import BaseNode


class NodeTypeMetadata:
    def __init__(self, node_type: Type["BaseNode"]):
        from . import pluginloader  # here cuz it should only be created from lifeblood, but can be used from viewer too
        self.type_name = node_type.type_name()
        self.label = node_type.label()
        self.tags = set(node_type.tags())
        self.description = node_type.description()
        self.settings_names = tuple(pluginloader.nodes_settings.get(node_type.type_name(), {}).keys())


class WorkerResources:
    __res_names = ('cpu_count', 'cpu_mem', 'gpu_count', 'gpu_mem')  # name of all main resources
    __resource_epsilon = 1e-5

    def __init__(self):
        """

        NOTE: because of float resource rounding - it's not safe to rely on consistency of summing/subtracting a lot of resources
        """
        self.hwid = get_unique_machine_id()
        self.cpu_count = psutil.cpu_count()  # note, cpu/gpu count can be float by design, but we don't like "almost" round float values
        self.cpu_mem = psutil.virtual_memory().total
        self.gpu_count = 0  # TODO: implement this
        self.gpu_mem = 0
        self.total_cpu_count = self.cpu_count
        self.total_cpu_mem = self.cpu_mem
        self.total_gpu_count = self.gpu_count
        self.total_gpu_mem = self.gpu_mem

        for rname in self.__res_names:  # sanity check
            assert hasattr(self, rname)
            assert hasattr(self, f'total_{rname}')

    def serialize(self) -> bytes:
        return pickle.dumps(self)

    @classmethod
    def deserialize(cls, data) -> "WorkerResources":
        return pickle.loads(data)

    def is_valid(self):
        return all(getattr(self, res) >= 0 for res in self.__res_names)
        # return self.cpu_count >= 0 and \
        #        self.cpu_mem >= 0 and \
        #        self.gpu_count >= 0 and \
        #        self.gpu_mem >= 0

    def __repr__(self):
        return f'<cpu: {self.cpu_count}/{self.total_cpu_count}, mem: {self.cpu_mem}/{self.total_cpu_mem}, gpu: {self.gpu_count}/{self.total_gpu_count}, gmm: {self.gpu_mem}/{self.total_gpu_mem}>'

    def __lt__(self, other):
        if not isinstance(other, WorkerResources):
            return other > self
        return any(getattr(self, res) < getattr(other, res) for res in self.__res_names)
        # return self.cpu_count < other.cpu_count or \
        #        self.cpu_mem < other.cpu_mem or \
        #        self.gpu_count < other.gpu_count or \
        #        self.gpu_mem < other.gpu_mem

    def __eq__(self, other):
        """
        note - this does NOT compare totals, only actual resources
        :param other:
        :return:
        """
        if not isinstance(other, WorkerResources):
            return other == self
        return all(getattr(self, res) == getattr(other, res) for res in self.__res_names)
        # return self.cpu_count == other.cpu_count and \
        #        self.cpu_mem == other.cpu_mem and \
        #        self.gpu_count == other.gpu_count and \
        #        self.gpu_mem == other.gpu_mem

    def __ne__(self, other):
        return not (self == other)

    def __le__(self, other):
        return self < other or self == other

    def __sub__(self, other):
        res = copy.copy(self)
        for resname in self.__res_names:
            val = getattr(res, resname) - getattr(other, resname)
            if isinstance(val, float):
                rounded_val = round(val)
                if abs(val - rounded_val) <= self.__resource_epsilon:
                    val = rounded_val
            setattr(res, resname, val)
        # res.cpu_count -= other.cpu_count
        # res.cpu_mem -= other.cpu_mem
        # res.gpu_count -= other.gpu_count
        # res.gpu_mem -= other.gpu_mem
        return res

    def __add__(self, other):
        res = copy.copy(self)
        for resname in self.__res_names:
            val = getattr(res, resname) - getattr(other, resname)
            if isinstance(val, float):
                rounded_val = round(val)
                if abs(val - rounded_val) <= self.__resource_epsilon:
                    val = rounded_val
            setattr(res, resname, val)
        # res.cpu_count += other.cpu_count
        # res.cpu_mem += other.cpu_mem
        # res.gpu_count += other.gpu_count
        # res.gpu_mem += other.gpu_mem
        return res

