import time
from io import BufferedIOBase
import struct
from dataclasses import dataclass, field
from .buffered_connection import BufferedReader
from .ui_protocol_data import TaskData, TaskBatchData, UiData
from .buffer_serializable import IBufferSerializable
from .enums import UIEventType

from typing import ClassVar, Dict, Iterable, Tuple, Type, Union


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
    """
    this is a base class for events
    """
    __subclass_to_id: ClassVar[Dict[Type["TaskEvent"], int]] = {}
    __id_to_subclass: ClassVar[Dict[int, Type["TaskEvent"]]] = {}
    __next_subclass_id: ClassVar[int] = 0

    @classmethod
    def register_subclasses(cls, subclasses_list: Iterable[Type["TaskEvent"]]):
        for subclass in subclasses_list:
            sid = cls.__next_subclass_id
            cls.__next_subclass_id += 1
            cls.__subclass_to_id[subclass] = sid
            cls.__id_to_subclass[sid] = subclass

    def serialize(self, stream: BufferedIOBase):
        """
        we serialize only the part that we know
        """
        stream.write(struct.pack('>BQQI', self.__subclass_to_id[type(self)], self.event_id, self.timestamp, self.event_type.value))

    @classmethod
    def deserialize(cls, stream: BufferedReader) -> "TaskEvent":
        """
        unlike all other deserializations, here we will deserialize into a subclass
        """
        sid, event_id, timestamp, event_type_raw = struct.unpack('>BQQI', stream.readexactly(20))
        event_type = UIEventType(event_type_raw)
        base_event = TaskEvent(event_type)
        base_event.event_id = event_id
        base_event.timestamp = timestamp

        subclass = cls.__id_to_subclass[sid]
        subclass._deserialize_part(base_event, stream)

    @classmethod
    def _deserialize_part(cls, base_event: "TaskEvent", stream: BufferedReader) -> "TaskEvent":
        raise NotImplementedError()


@dataclass
class TaskFullState(TaskEvent):
    task_data: TaskBatchData
    event_type: UIEventType = field(default=UIEventType.FULL_STATE, init=False)

    def serialize(self, stream: BufferedIOBase):
        super().serialize(stream)
        self.task_data.serialize(stream)

    @classmethod
    def _deserialize_part(cls, base_event: TaskEvent, stream: BufferedReader) -> "TaskFullState":
        task_data = TaskBatchData.deserialize(stream)
        event = TaskFullState(task_data)
        assert event.event_type == base_event.event_type == UIEventType.FULL_STATE
        event.timestamp = base_event.timestamp
        event.event_id = base_event.event_id
        return event


@dataclass
class TaskUpdated(TaskEvent):
    task_data: TaskData
    event_type: UIEventType = field(default=UIEventType.UPDATE, init=False)

    def serialize(self, stream: BufferedIOBase):
        super().serialize(stream)
        self.task_data.serialize(stream)

    @classmethod
    def _deserialize_part(cls, base_event: TaskEvent, stream: BufferedReader) -> "TaskUpdated":
        task_data = TaskData.deserialize(stream)
        event = TaskUpdated(task_data)
        assert event.event_type == base_event.event_type == UIEventType.UPDATE
        event.timestamp = base_event.timestamp
        event.event_id = base_event.event_id
        return event


@dataclass
class TasksUpdated(TaskEvent):
    task_data: TaskBatchData
    event_type: UIEventType = field(default=UIEventType.UPDATE, init=False)

    def serialize(self, stream: BufferedIOBase):
        super().serialize(stream)
        self.task_data.serialize(stream)

    @classmethod
    def _deserialize_part(cls, base_event: TaskEvent, stream: BufferedReader) -> "TasksUpdated":
        task_data = TaskBatchData.deserialize(stream)
        event = TasksUpdated(task_data)
        assert event.event_type == base_event.event_type == UIEventType.UPDATE
        event.timestamp = base_event.timestamp
        event.event_id = base_event.event_id
        return event


@dataclass
class TasksDeleted(TaskEvent):
    task_ids: Tuple[int, ...]  # task data, or task_id depending on UIEventType
    event_type: UIEventType = field(default=UIEventType.DELETE, init=False)

    def serialize(self, stream: BufferedIOBase):
        super().serialize(stream)
        stream.write(struct.pack('>Q', len(self.task_ids)))
        for task_id in self.task_ids:
            stream.write(struct.pack('>Q', task_id))

    @classmethod
    def _deserialize_part(cls, base_event: TaskEvent, stream: BufferedReader) -> "TasksDeleted":
        task_count, = struct.unpack('>Q', stream.readexactly(8))
        task_ids = struct.unpack('>' + 'Q' * task_count, stream.readexactly(8 * task_count))
        event = TasksDeleted(task_ids)
        assert event.event_type == base_event.event_type == UIEventType.DELETE
        event.timestamp = base_event.timestamp
        event.event_id = base_event.event_id
        return event


TaskEvent.register_subclasses([TaskFullState, TaskUpdated, TasksUpdated, TasksDeleted])
