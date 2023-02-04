import time
from dataclasses import dataclass, field
from .ui_protocol_data import TaskData, UiData
from .enums import UIEventType

from typing import Union


@dataclass
class SchedulerEvent:
    timestamp: float = field(default_factory=lambda: time.time(), init=False)
    event_id: int
    event_type: UIEventType


@dataclass
class TaskEvent(SchedulerEvent):
    task_data: Union[TaskData, int]  # task data, or task_id depending on UIEventType


@dataclass
class DataUpdateEvent(SchedulerEvent):
    ui_data: UiData
