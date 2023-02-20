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

    def __hash__(self):
        return hash((self.event_id, self.timestamp, self.event_type))


class SchedulerEventAutoId(SchedulerEvent):
    event_id: int = field(default=-1, init=False)


@dataclass
class TaskEvent(SchedulerEventAutoId):
    pass


@dataclass
class TaskFullState(TaskEvent):
    task_data: TaskBatchData
    event_type: UIEventType = field(default=UIEventType.FULL_STATE, init=False)


@dataclass
class TaskUpdate(TaskEvent):
    task_data: TaskData
    event_type: UIEventType = field(default=UIEventType.UPDATE, init=False)


@dataclass
class TasksUpdate(TaskEvent):
    task_data: TaskBatchData
    event_type: UIEventType = field(default=UIEventType.UPDATE, init=False)


@dataclass
class TaskDeleted(TaskEvent):
    task_data: int  # task data, or task_id depending on UIEventType
    event_type: UIEventType = field(default=UIEventType.DELETE, init=False)
