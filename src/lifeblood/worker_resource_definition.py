from dataclasses import dataclass

from typing import Type


@dataclass
class WorkerResourceDefinition:
    name: str
    type: Type
    description: str
    label: str  # nicer looking user facing name
