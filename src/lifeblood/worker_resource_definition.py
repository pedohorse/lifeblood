from dataclasses import dataclass
from enum import Enum

from typing import Union


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
