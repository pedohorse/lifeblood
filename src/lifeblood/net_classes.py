import uuid
import psutil
import pickle
import copy
import re
from .base import TypeMetadata
from .misc import get_unique_machine_id
from .logging import get_logger

from typing import Optional, TYPE_CHECKING, Tuple, Type, Union, Set
if TYPE_CHECKING:
    from .basenode import BaseNode

__logger = get_logger('worker_resources')


__mem_parse_re = re.compile(r'^\s*(\d+(?:\.\d+)?)\s*([BKMGTP]?)\s*$')


def _try_parse_mem_spec(s: Union[str, int], default: Optional[int] = None):
    if not isinstance(s, str):
        return s
    match = __mem_parse_re.match(s)
    if not match:
        __logger.warning(f'could not parse "{s}", using default')
        return default
    bytes_count = float(match.group(1))
    coeff = match.group(2)

    coeff_map = {'B': 1,
                 'K': 10**3,
                 'M': 10**6,
                 'G': 10**9,
                 'T': 10**12,
                 'P': 10**15}

    if coeff not in coeff_map:
        __logger.warning(f'could not parse "{s}", wtf is "{coeff}"? using default')
        return default

    if coeff:
        mult = coeff_map[coeff]

        # this way we limit float op errors
        if mult > 10**6:
            mult //= 10**6
            bytes_count = int(bytes_count * 10**6)

        bytes_count = bytes_count * mult

    return int(bytes_count)


class NodeTypeMetadata(TypeMetadata):
    def __init__(self, node_type: Type["BaseNode"]):
        from . import pluginloader  # here cuz it should only be created from lifeblood, but can be used from viewer too
        self.__type_name = node_type.type_name()
        self.__label = node_type.label()
        self.__tags = set(node_type.tags())
        self.__description = node_type.description()
        self.__settings_names = tuple(pluginloader.nodes_settings.get(node_type.type_name(), {}).keys())

    @property
    def type_name(self) -> str:
        return self.__type_name

    @property
    def label(self) -> Optional[str]:
        return self.__label

    @property
    def tags(self) -> Set[str]:
        return self.__tags

    @property
    def description(self) -> str:
        return self.__description

    @property
    def settings_names(self) -> Tuple[str, ...]:
        return self.__settings_names


class WorkerResources:
    __res_names = ('cpu_count', 'cpu_mem', 'gpu_count', 'gpu_mem')  # name of all main resources
    __resource_epsilon = 1e-5

    def __init__(self,
                 cpu_count: Union[int, float, None] = None, cpu_mem: Optional[int] = None,
                 gpu_count: Union[int, float, None] = None, gpu_mem: Optional[int] = None):
        """

        NOTE: because of float resource rounding - it's not safe to rely on consistency of summing/subtracting a lot of resources
        """
        self.hwid = get_unique_machine_id()
        self.cpu_count = cpu_count if cpu_count is not None else psutil.cpu_count()  # note, cpu/gpu count can be float by design, but we don't like "almost" round float values
        self.cpu_mem = _try_parse_mem_spec(cpu_mem) if cpu_mem is not None else psutil.virtual_memory().total
        self.gpu_count = gpu_count if gpu_count is not None else 0  # TODO: implement GPU autodetection
        self.gpu_mem = _try_parse_mem_spec(gpu_mem) if gpu_mem is not None else 0
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


