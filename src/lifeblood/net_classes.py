import uuid
import psutil
import pickle
from typing import TYPE_CHECKING, Type
if TYPE_CHECKING:
    from basenode import BaseNode


class NodeTypeMetadata:
    def __init__(self, node_type: Type["BaseNode"]):
        self.type_name = node_type.type_name()
        self.label = node_type.label()
        self.tags = set(node_type.tags())
        self.description = node_type.description()


class WorkerResources:
    def __init__(self):
        self.hwid = uuid.getnode()
        self.cpu_count = psutil.cpu_count()
        self.mem_size = psutil.virtual_memory().total
        self.gpu_count = 0  # TODO: implement this
        self.gmem_size = 0

    def serialize(self) -> bytes:
        return pickle.dumps(self)

    @classmethod
    def deserialize(cls, data) -> "WorkerResources":
        return pickle.loads(data)