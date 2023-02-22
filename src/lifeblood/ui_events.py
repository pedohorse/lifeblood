import time
from io import BufferedIOBase
import struct
from dataclasses import dataclass, field
from .buffered_connection import BufferedReader
from .ui_protocol_data import TaskData, TaskBatchData, UiData
from .buffer_serializable import IBufferSerializable
from .enums import UIEventType

from typing import Tuple, Union


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
class TaskUpdated(TaskEvent):
    task_data: TaskData
    event_type: UIEventType = field(default=UIEventType.UPDATE, init=False)

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QQI', self.event_id, self.timestamp, self.event_type.value))
        self.task_data.serialize(stream)

    @classmethod
    def deserialize(cls, stream: BufferedReader) -> "TaskUpdated":
        event_id, timestamp, event_type_raw = struct.unpack('>QQI', stream.readexactly(20))
        task_data = TaskData.deserialize(stream)
        event = TaskUpdated(task_data)
        assert event.event_type == UIEventType(event_type_raw)
        event.timestamp = timestamp
        event.event_id = event_id
        return event


@dataclass
class TasksUpdated(TaskEvent):
    task_data: TaskBatchData
    event_type: UIEventType = field(default=UIEventType.UPDATE, init=False)

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QQI', self.event_id, self.timestamp, self.event_type.value))
        self.task_data.serialize(stream)

    @classmethod
    def deserialize(cls, stream: BufferedReader) -> "TasksUpdated":
        event_id, timestamp, event_type_raw = struct.unpack('>QQI', stream.readexactly(20))
        task_data = TaskBatchData.deserialize(stream)
        event = TasksUpdated(task_data)
        assert event.event_type == UIEventType(event_type_raw)
        event.timestamp = timestamp
        event.event_id = event_id
        return event


@dataclass
class TasksDeleted(TaskEvent):
    task_ids: Tuple[int, ...]  # task data, or task_id depending on UIEventType
    event_type: UIEventType = field(default=UIEventType.DELETE, init=False)

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QQIQ', self.event_id, self.timestamp, self.event_type.value, len(self.task_ids)))
        for task_id in self.task_ids:
            stream.write(struct.pack('>Q', task_id))

    @classmethod
    def deserialize(cls, stream: BufferedReader) -> "TaskDeleted":
        event_id, timestamp, event_type_raw, task_count = struct.unpack('>QQIQ', stream.readexactly(20))
        task_ids = struct.unpack('>' + 'Q' * task_count, stream.readexactly(8 * task_count))
        event = TasksDeleted(task_ids)
        assert event.event_type == UIEventType(event_type_raw)
        event.timestamp = timestamp
        event.event_id = event_id
        return event
