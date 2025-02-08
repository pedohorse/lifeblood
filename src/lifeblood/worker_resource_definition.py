from dataclasses import dataclass
from enum import Enum

from typing import Union, Tuple


class WorkerResourceDataType(Enum):
    """
    used for both defining actual data type,
    and a hint for UI how to better display the resource
    """
    GENERIC_FLOAT = 0
    GENERIC_INT = 1
    SHARABLE_COMPUTATIONAL_UNIT = 2
    MEMORY_BYTES = 3


@dataclass
class WorkerResourceDefinition:
    name: str
    type: WorkerResourceDataType
    description: str
    label: str  # nicer looking user facing name
    default: Union[float, int] = 0

    def to_json_dict(self) -> dict:
        return {
            'name': self.name,
            'type': self.type.value,
            'description': self.description,
            'label': self.label,
            'default': self.default,
        }

    @classmethod
    def from_json_dict(cls, data: dict) -> "WorkerResourceDefinition":
        return WorkerResourceDefinition(
            data['name'],
            WorkerResourceDataType(data['type']),
            data['description'],
            data['label'],
            data['default'],
        )


@dataclass
class WorkerDeviceTypeDefinition:
    name: str
    resources: Tuple[WorkerResourceDefinition, ...]

    def to_json_dict(self) -> dict:
        return {
            'name': self.name,
            'res': [x.to_json_dict() for x in self.resources]
        }

    @classmethod
    def from_json_dict(cls, data: dict) -> "WorkerDeviceTypeDefinition":
        return WorkerDeviceTypeDefinition(
            data['name'],
            tuple(WorkerResourceDefinition.from_json_dict(res) for res in data['res']),
        )
