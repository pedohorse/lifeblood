import time
from io import BufferedIOBase
import struct
from dataclasses import dataclass, field
from .buffered_connection import BufferedReader
from .ui_protocol_data import TaskData, TaskBatchData, UiData
from .buffer_serializable import IBufferSerializable
from .enums import UIEventType

from typing import Union


@dataclass
class SchedulerEvent(IBufferSerializable):
    # some OS (like windows) do not have proper time() resolution, so time_ns is used instead
    timestamp: float = field(default_factory=lambda: time.time_ns(), init=False)
    event_id: int
    event_type: UIEventType

    def __hash__(self):
        return hash((self.event_id, self.timestamp, self.event_type))

@dataclass
class SchedulerEventAutoId(SchedulerEvent):
    event_id: int = field(default=-1, init=False)


@dataclass
class TaskEvent(SchedulerEventAutoId):
    pass


@dataclass
class TaskFullState(TaskEvent):
    task_data: TaskBatchData
    event_type: UIEventType = field(default=UIEventType.FULL_STATE, init=False)

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QQI', self.event_id, self.timestamp, self.event_type.value))
        self.task_data.serialize(stream)

    @classmethod
    def deserialize(cls, stream: BufferedReader) -> "TaskFullState":
        event_id, timestamp, event_type_raw = struct.unpack('>QQI', stream.readexactly(20))
        task_data = TaskBatchData.deserialize(stream)
        event = TaskFullState(task_data)
        assert event.event_type == UIEventType(event_type_raw)
        event.timestamp = timestamp
        event.event_id = event_id
        return event


@dataclass
class TaskUpdate(TaskEvent):
    task_data: TaskData
    event_type: UIEventType = field(default=UIEventType.UPDATE, init=False)

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QQI', self.event_id, self.timestamp, self.event_type.value))
        self.task_data.serialize(stream)

    @classmethod
    def deserialize(cls, stream: BufferedReader) -> "TaskUpdate":
        event_id, timestamp, event_type_raw = struct.unpack('>QQI', stream.readexactly(20))
        task_data = TaskData.deserialize(stream)
        event = TaskUpdate(task_data)
        assert event.event_type == UIEventType(event_type_raw)
        event.timestamp = timestamp
        event.event_id = event_id
        return event


@dataclass
class TasksUpdate(TaskEvent):
    task_data: TaskBatchData
    event_type: UIEventType = field(default=UIEventType.UPDATE, init=False)

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QQI', self.event_id, self.timestamp, self.event_type.value))
        self.task_data.serialize(stream)

    @classmethod
    def deserialize(cls, stream: BufferedReader) -> "TasksUpdate":
        event_id, timestamp, event_type_raw = struct.unpack('>QQI', stream.readexactly(20))
        task_data = TaskBatchData.deserialize(stream)
        event = TasksUpdate(task_data)
        assert event.event_type == UIEventType(event_type_raw)
        event.timestamp = timestamp
        event.event_id = event_id
        return event


@dataclass
class TaskDeleted(TaskEvent):
    task_id: int  # task data, or task_id depending on UIEventType
    event_type: UIEventType = field(default=UIEventType.DELETE, init=False)

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QQIQ', self.event_id, self.timestamp, self.event_type.value, self.task_id))

    @classmethod
    def deserialize(cls, stream: BufferedReader) -> "TaskDeleted":
        event_id, timestamp, event_type_raw, task_id = struct.unpack('>QQIQ', stream.readexactly(20))
        event = TaskDeleted(task_id)
        assert event.event_type == UIEventType(event_type_raw)
        event.timestamp = timestamp
        event.event_id = event_id
        return event
