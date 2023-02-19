import time
from dataclasses import dataclass, field
from .ui_protocol_data import TaskData, TaskBatchData, UiData
from .enums import UIEventType

from typing import Union


@dataclass
class SchedulerEvent:
    # some OS (like windows) do not have proper time() resolution, so time_ns is used instead
    timestamp: float = field(default_factory=lambda: time.time_ns(), init=False)
    event_id: int
    event_type: UIEventType


@dataclass
class TaskEvent(SchedulerEvent):
    task_data: Union[TaskData, int]  # task data, or task_id depending on UIEventType


@dataclass
class DataUpdateEvent(SchedulerEvent):
    ui_data: UiData


@dataclass
class TaskGroupFullUpdate(SchedulerEvent):
    task_batch_data: TaskBatchData


@dataclass
class TaskGraphUpdate(SchedulerEvent):
    task_batch_data: TaskBatchData
