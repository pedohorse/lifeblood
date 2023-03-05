import time
from io import BufferedIOBase
import struct
from dataclasses import dataclass, field
from .buffered_connection import BufferedReader
from .ui_protocol_data import TaskData, TaskDelta, TaskBatchData, UiData
from .buffer_serializable import IBufferSerializable
from .enums import UIEventType

from typing import ClassVar, Dict, Iterable, List, Tuple, Type, Union


@dataclass
class SchedulerEvent(IBufferSerializable):
    # some OS (like windows) do not have proper time() resolution, so time_ns is used instead
    timestamp: float = field(default_factory=lambda: time.time_ns(), init=False)
    event_id: int
    event_type: UIEventType
    database_uid: int

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
        stream.write(struct.pack('>BQQQI', self.__subclass_to_id[type(self)], self.database_uid, self.event_id, self.timestamp, self.event_type.value))

    @classmethod
    def deserialize(cls, stream: BufferedReader) -> "TaskEvent":
        """
        unlike all other deserializations, here we will deserialize into a subclass
        """
        sid, dbuid, event_id, timestamp, event_type_raw = struct.unpack('>BQQQI', stream.readexactly(29))
        event_type = UIEventType(event_type_raw)
        base_event = TaskEvent(event_type, dbuid)
        base_event.event_id = event_id
        base_event.timestamp = timestamp

        subclass = cls.__id_to_subclass[sid]
        return subclass._deserialize_part(base_event, stream)

    @classmethod
    def _deserialize_part(cls, base_event: "TaskEvent", stream: BufferedReader) -> "TaskEvent":
        raise NotImplementedError()

    def tiny_repr(self) -> str:
        return f'<{type(self).__name__}:{self.event_id}.{self.event_type.name}'


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
        event = TaskFullState(base_event.database_uid, task_data)
        assert event.event_type == base_event.event_type == UIEventType.FULL_STATE
        event.timestamp = base_event.timestamp
        event.event_id = base_event.event_id
        return event

    def tiny_repr(self) -> str:
        return super().tiny_repr() + f':{self.task_data.tiny_repr()}>'


@dataclass
class TasksChanged(TaskEvent):
    task_deltas: List[TaskDelta]
    event_type: UIEventType = field(default=UIEventType.UPDATE, init=False)

    def serialize(self, stream: BufferedIOBase):
        super().serialize(stream)
        stream.write(struct.pack('>Q', len(self.task_deltas)))
        for task_delta in self.task_deltas:
            task_delta.serialize(stream)

    @classmethod
    def _deserialize_part(cls, base_event: "TaskEvent", stream: BufferedReader) -> "TaskEvent":
        task_deltas = []
        num_deltas, = struct.unpack('>Q', stream.readexactly(8))
        for _ in range(num_deltas):
            task_deltas.append(TaskDelta.deserialize(stream))
        event = TasksChanged(base_event.database_uid, task_deltas)
        assert event.event_type == base_event.event_type == UIEventType.UPDATE
        event.timestamp = base_event.timestamp
        event.event_id = base_event.event_id
        return event

    def tiny_repr(self) -> str:
        return super().tiny_repr() + f':[{",".join(str(x.id) for x in self.task_deltas)}]>'


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
        event = TasksUpdated(base_event.database_uid, task_data)
        assert event.event_type == base_event.event_type == UIEventType.UPDATE
        event.timestamp = base_event.timestamp
        event.event_id = base_event.event_id
        return event

    def tiny_repr(self) -> str:
        return super().tiny_repr() + f':{self.task_data.tiny_repr()}>'

@dataclass
class TasksRemoved(TaskEvent):
    task_ids: Tuple[int, ...]  # task data, or task_id depending on UIEventType
    event_type: UIEventType = field(default=UIEventType.DELETE, init=False)

    def serialize(self, stream: BufferedIOBase):
        super().serialize(stream)
        stream.write(struct.pack('>Q', len(self.task_ids)))
        for task_id in self.task_ids:
            stream.write(struct.pack('>Q', task_id))

    @classmethod
    def _deserialize_part(cls, base_event: TaskEvent, stream: BufferedReader) -> "TasksRemoved":
        task_count, = struct.unpack('>Q', stream.readexactly(8))
        task_ids = struct.unpack('>' + 'Q' * task_count, stream.readexactly(8 * task_count))
        event = TasksRemoved(base_event.database_uid, task_ids)
        assert event.event_type == base_event.event_type == UIEventType.DELETE
        event.timestamp = base_event.timestamp
        event.event_id = base_event.event_id
        return event

    def tiny_repr(self) -> str:
        return super().tiny_repr() + f':{self.task_ids}>'


TaskEvent.register_subclasses([TaskFullState, TasksChanged, TasksUpdated, TasksRemoved])
