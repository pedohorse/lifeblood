import io

import lz4.frame
import struct
from io import BytesIO, BufferedIOBase
from .buffered_connection import BufferedReader
from .buffer_serializable import IBufferSerializable
from .enums import TaskState, WorkerState, WorkerType, TaskGroupArchivedState, InvocationState
from dataclasses import dataclass

from typing import Dict, List, Tuple, Type, Optional, Set, Union


def _serialize_string(s: str, stream: BufferedIOBase) -> int:
    bstr = s.encode('UTF-8')
    stream.write(struct.pack('>Q', len(bstr)))
    return stream.write(bstr)


def _deserialize_string(stream: BufferedReader) -> str:
    bsize, = struct.unpack('>Q', stream.readexactly(8))
    if bsize == 0:
        return ''
    return bytes(stream.readexactly(bsize)).decode('UTF-8')


class DataNotSet:
    pass


@dataclass
class TaskDelta(IBufferSerializable):
    id: int
    parent_id: Union[Optional[int], Type[DataNotSet]] = DataNotSet
    children_count: Union[int, Type[DataNotSet]] = DataNotSet                   # TODO: remove, add to getting individual task details
    active_children_count: Union[int, Type[DataNotSet]] = DataNotSet            # TODO: remove, add to getting individual task details
    state: Union[TaskState, Type[DataNotSet]] = DataNotSet                      # TODO: remove, add to getting individual task details
    state_details: Union[Optional[str], Type[DataNotSet]] = DataNotSet          # TODO: remove, add to getting individual task details
    paused: Union[bool, Type[DataNotSet]] = DataNotSet
    node_id: Union[int, Type[DataNotSet]] = DataNotSet
    node_input_name: Union[Optional[str], Type[DataNotSet]] = DataNotSet
    node_output_name: Union[Optional[str], Type[DataNotSet]] = DataNotSet
    name: Union[str, Type[DataNotSet]] = DataNotSet
    split_level: Union[int, Type[DataNotSet]] = DataNotSet                      # TODO: is this needed for UI?
    work_data_invocation_attempt: Union[int, Type[DataNotSet]] = DataNotSet     # TODO: remove, add to getting individual task details
    progress: Union[Optional[float], Type[DataNotSet]] = DataNotSet
    split_origin_task_id: Union[Optional[int], Type[DataNotSet]] = DataNotSet   # used by UI to draw new tasks from the split_origin_task_id task
    split_id: Union[Optional[int], Type[DataNotSet]] = DataNotSet               # TODO: is this needed for UI?
    invocation_id: Union[Optional[int], Type[DataNotSet]] = DataNotSet
    groups: Union[Set[str], Type[DataNotSet]] = DataNotSet

    def serialize(self, stream: BufferedIOBase):
        data_has = struct.pack(
            '>?????????????????',
            *(x is not DataNotSet for x in
              (self.parent_id, self.children_count, self.active_children_count, self.state, self.state_details,
               self.paused, self.node_id, self.node_input_name, self.node_output_name, self.name, self.split_level,
               self.work_data_invocation_attempt, self.progress, self.split_origin_task_id, self.split_id,
               self.invocation_id, self.groups)))
        stream.write(data_has)

        pattern_parts = ['>Q']
        pack_stuff = [self.id]
        if self.parent_id is not DataNotSet:
            pattern_parts.append('?Q')
            pack_stuff.extend([self.parent_id is not None, self.parent_id or 0])
        if self.children_count is not DataNotSet:
            pattern_parts.append('Q')
            pack_stuff.append(self.children_count)
        if self.active_children_count is not DataNotSet:
            pattern_parts.append('Q')
            pack_stuff.append(self.active_children_count)
        if self.state is not DataNotSet:
            pattern_parts.append('I')
            pack_stuff.append(self.state.value)
        if self.paused is not DataNotSet:
            pattern_parts.append('?')
            pack_stuff.append(self.paused)
        if self.node_id is not DataNotSet:
            pattern_parts.append('Q')
            pack_stuff.append(self.node_id)
        if self.split_level is not DataNotSet:
            pattern_parts.append('Q')
            pack_stuff.append(self.split_level)
        if self.work_data_invocation_attempt is not DataNotSet:
            pattern_parts.append('Q')
            pack_stuff.append(self.work_data_invocation_attempt)
        if self.progress is not DataNotSet:
            pattern_parts.append('?d')
            pack_stuff.extend([self.progress is not None, self.progress or 0.0])
        if self.split_origin_task_id is not DataNotSet:
            pattern_parts.append('?Q')
            pack_stuff.extend([self.split_origin_task_id is not None, self.split_origin_task_id or 0])
        if self.split_id is not DataNotSet:
            pattern_parts.append('?Q')
            pack_stuff.extend([self.split_id is not None, self.split_id or 0])
        if self.invocation_id is not DataNotSet:
            pattern_parts.append('?Q')
            pack_stuff.extend([self.invocation_id is not None, self.invocation_id or 0])
        if self.groups is not DataNotSet:
            pattern_parts.append('Q')
            pack_stuff.append(len(self.groups))

        data = struct.pack(''.join(pattern_parts), *pack_stuff)
        stream.write(data)

        if self.state_details is not DataNotSet:
            stream.write(struct.pack('>?', self.state_details is not None))
            if self.state_details is not None:
                _serialize_string(self.state_details, stream)
        if self.node_input_name is not DataNotSet:
            stream.write(struct.pack('>?', self.node_input_name is not None))
            if self.node_input_name is not None:
                _serialize_string(self.node_input_name, stream)
        if self.node_output_name is not DataNotSet:
            stream.write(struct.pack('>?', self.node_output_name is not None))
            if self.node_output_name is not None:
                _serialize_string(self.node_output_name, stream)
        if self.name is not DataNotSet:
            _serialize_string(self.name, stream)
        if self.groups is not DataNotSet:
            for group in self.groups:
                _serialize_string(group, stream)

    @classmethod
    def deserialize(cls, stream: BufferedReader):
        has_parent_id, has_children_count, \
            has_active_children_count, has_state, has_state_details, has_paused, has_node_id, has_node_input_name, \
            has_node_output_name, has_name, has_split_level, has_work_data_invocation_attempt, \
            has_progress, has_split_origin_task_id, has_split_id, has_invocation_id, has_groups = \
                struct.unpack('>?????????????????', stream.readexactly(17))

        task_id, = struct.unpack('>Q', stream.readexactly(8))
        parent_id = DataNotSet
        if has_parent_id:
            is_not_none, parent_id = struct.unpack('>?Q', stream.readexactly(9))
            if not is_not_none:
                parent_id = None
        children_count = DataNotSet
        if has_children_count:
            children_count, = struct.unpack('>Q', stream.readexactly(8))
        active_children_count = DataNotSet
        if has_active_children_count:
            active_children_count, = struct.unpack('>Q', stream.readexactly(8))
        state = DataNotSet
        if has_state:
            _state_raw, = struct.unpack('>I', stream.readexactly(4))
            state = TaskState(_state_raw)
        paused = DataNotSet
        if has_paused:
            paused, = struct.unpack('>?', stream.readexactly(1))
        node_id = DataNotSet
        if has_node_id:
            node_id, = struct.unpack('>Q', stream.readexactly(8))
        split_level = DataNotSet
        if has_split_level:
            split_level, = struct.unpack('>Q', stream.readexactly(8))
        work_data_invocation_attempt = DataNotSet
        if has_work_data_invocation_attempt:
            work_data_invocation_attempt, = struct.unpack('>Q', stream.readexactly(8))
        progress = DataNotSet
        if has_progress:
            is_not_none, progress = struct.unpack('>?d', stream.readexactly(9))
            if not is_not_none:
                progress = None
        split_origin_task_id = DataNotSet
        if has_split_origin_task_id:
            is_not_none, split_origin_task_id = struct.unpack('>?Q', stream.readexactly(9))
            if not is_not_none:
                split_origin_task_id = None
        split_id = DataNotSet
        if has_split_id:
            is_not_none, split_id = struct.unpack('>?Q', stream.readexactly(9))
            if not is_not_none:
                split_id = None
        invocation_id = DataNotSet
        if has_invocation_id:
            is_not_none, invocation_id = struct.unpack('>?Q', stream.readexactly(9))
            if not is_not_none:
                invocation_id = None
        group_count = 0
        if has_groups:
            group_count, = struct.unpack('>Q', stream.readexactly(8))

        state_details = DataNotSet
        if has_state_details:
            state_details = None
            is_not_none, = struct.unpack('>?', stream.readexactly(1))
            if is_not_none:
                state_details = _deserialize_string(stream)

        node_input_name = DataNotSet
        if has_node_input_name:
            node_input_name = None
            has_node_input_name, = struct.unpack('>?', stream.readexactly(1))
            if has_node_input_name:
                node_input_name = _deserialize_string(stream)
        node_output_name = DataNotSet
        if has_node_output_name:
            node_output_name = None
            has_node_output_name, = struct.unpack('>?', stream.readexactly(1))
            if has_node_output_name:
                node_output_name = _deserialize_string(stream)
        task_name = DataNotSet
        if has_name:
            task_name = _deserialize_string(stream)
        groups = DataNotSet
        if has_groups:
            groups = set()
            for i in range(group_count):
                group_name = _deserialize_string(stream)
                groups.add(group_name)

        return TaskDelta(task_id, parent_id, children_count, active_children_count, state, state_details, paused,
                         node_id, node_input_name, node_output_name, task_name, split_level, work_data_invocation_attempt,
                         progress, split_origin_task_id, split_id, invocation_id, groups)

    def __repr__(self):
        parts = []
        for field, val in self.__dict__.items():
            if val is DataNotSet:
                continue
            parts.append(f'{field}={val}')
        return f'TaskDelta({", ".join(parts)})'

    def tiny_repr(self):
        return f'{type(self).__name__}:[{self.id}]'


@dataclass
class TaskData(IBufferSerializable):
    id: int
    parent_id: Optional[int]
    children_count: int
    active_children_count: int
    state: TaskState
    state_details: Optional[str]
    paused: bool
    node_id: int
    node_input_name: Optional[str]
    node_output_name: Optional[str]
    name: str
    split_level: int
    work_data_invocation_attempt: int
    progress: Optional[float]
    split_origin_task_id: Optional[int]
    split_id: Optional[int]
    invocation_id: Optional[int]
    groups: Set[str]

    def serialize(self, stream: BufferedIOBase):
        #                    i?pcaspnsw?p?s?s?ig
        data = struct.pack('>Q?QQQI?QQQ?d?Q?Q?QQ', self.id, self.parent_id is not None, self.parent_id or 0, self.children_count,
                           self.active_children_count, self.state.value, self.paused, self.node_id, self.split_level,
                           self.work_data_invocation_attempt, self.progress is not None, self.progress or 0.0,
                           self.split_origin_task_id is not None, self.split_origin_task_id or 0,
                           self.split_id is not None, self.split_id or 0,
                           self.invocation_id is not None, self.invocation_id or 0, len(self.groups))
        stream.write(data)
        stream.write(struct.pack('>?', self.state_details is not None))
        if self.state_details is not None:
            _serialize_string(self.state_details, stream)
        stream.write(struct.pack('>?', self.node_input_name is not None))
        if self.node_input_name is not None:
            _serialize_string(self.node_input_name, stream)
        stream.write(struct.pack('>?', self.node_output_name is not None))
        if self.node_output_name is not None:
            _serialize_string(self.node_output_name, stream)
        _serialize_string(self.name, stream)
        for group in self.groups:
            _serialize_string(group, stream)

    @classmethod
    def deserialize(cls, stream: BufferedReader) -> "TaskData":
        offset = 106  # 8 + 1 + 8*3 + 4 + 1 + 8*3 + 1+8 + 1+8 + 1+8 + 1+8 + 8
        task_id, has_parent_id, task_parent_id, task_children_count, task_active_children_count, \
            task_state_value, task_paused, task_node_id, task_split_level, task_work_data_invocation_attempt, \
            has_progress, progress, \
            has_task_split_origin_task_id, task_split_origin_task_id, \
            has_task_split_id, task_split_id, \
            has_task_invocation_id, task_invocation_id, group_count = struct.unpack('>Q?QQQI?QQQ?d?Q?Q?QQ', stream.readexactly(offset))

        has_state_details, = struct.unpack('>?', stream.readexactly(1))
        if has_state_details:
            state_details = _deserialize_string(stream)
        else:
            state_details = None
        has_node_input_name, = struct.unpack('>?', stream.readexactly(1))
        if has_node_input_name:
            node_input_name = _deserialize_string(stream)
        else:
            node_input_name = None
        has_node_output_name, = struct.unpack('>?', stream.readexactly(1))
        if has_node_output_name:
            node_output_name = _deserialize_string(stream)
        else:
            node_output_name = None
        task_name = _deserialize_string(stream)

        groups = set()
        for i in range(group_count):
            group_name = _deserialize_string(stream)
            groups.add(group_name)

        return TaskData(task_id, task_parent_id if has_parent_id else None, task_children_count, task_active_children_count,
                        TaskState(task_state_value), state_details, task_paused, task_node_id, node_input_name, node_output_name,
                        task_name, task_split_level, task_work_data_invocation_attempt,
                        progress if has_progress else None,
                        task_split_origin_task_id if has_task_split_origin_task_id else None,
                        task_split_id if has_task_split_id else None,
                        task_invocation_id if has_task_invocation_id else None, groups)

    def tiny_repr(self):
        return f'{type(self).__name__}:[{self.id}]'


@dataclass
class TaskBatchData(IBufferSerializable):
    db_uid: int
    tasks: Dict[int, TaskData]

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QQ', self.db_uid, len(self.tasks)))
        for task in self.tasks.values():
            task.serialize(stream)

    @classmethod
    def deserialize(cls, stream: BufferedIOBase) -> "TaskBatchData":
        db_uid, tasks_count, = struct.unpack('>QQ', stream.readexactly(16))
        tasks = {}
        for i in range(tasks_count):
            task_data = TaskData.deserialize(stream)
            tasks[task_data.id] = task_data
        return TaskBatchData(db_uid, tasks)

    def tiny_repr(self):
        return f'{type(self).__name__}:[{",".join(str(x) for x in self.tasks.keys())}]'


@dataclass
class NodeData(IBufferSerializable):
    id: int
    name: str
    type: str

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>Q', self.id))
        _serialize_string(self.name, stream)
        _serialize_string(self.type, stream)

    @classmethod
    def deserialize(cls, stream: BufferedReader) -> "NodeData":
        node_id, = struct.unpack('>Q', stream.readexactly(8))
        node_name = _deserialize_string(stream)
        node_type = _deserialize_string(stream)
        return NodeData(node_id, node_name, node_type)


@dataclass
class NodeConnectionData(IBufferSerializable):
    connection_id: int
    in_id: int
    in_name: str
    out_id: int
    out_name: str

    def serialize(self, stream: BufferedIOBase):
        chunk = struct.pack('>QQQ', self.connection_id, self.in_id, self.out_id)
        stream.write(chunk)
        _serialize_string(self.in_name, stream)
        _serialize_string(self.out_name, stream)

    @classmethod
    def deserialize(cls, stream: BufferedReader) -> "NodeConnectionData":
        connection_id, in_id, out_id = struct.unpack('>QQQ', stream.readexactly(24))
        in_name = _deserialize_string(stream)
        out_name = _deserialize_string(stream)
        return NodeConnectionData(connection_id, in_id, in_name, out_id, out_name)


@dataclass
class NodeGraphStructureData(IBufferSerializable):
    db_uid: int
    nodes: Dict[int, NodeData]
    connections: Dict[int, NodeConnectionData]

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QQQ', self.db_uid, len(self.nodes), len(self.connections)))
        for node in self.nodes.values():
            node.serialize(stream)
        for connection in self.connections.values():
            connection.serialize(stream)

    @classmethod
    def deserialize(cls, stream: BufferedReader) -> "NodeGraphStructureData":
        db_uid, nodes_count, connections_count = struct.unpack('>QQQ', stream.readexactly(24))
        nodes = {}
        connections = {}
        for i in range(nodes_count):
            node_data = NodeData.deserialize(stream)
            nodes[node_data.id] = node_data
        for i in range(connections_count):
            conn_data = NodeConnectionData.deserialize(stream)
            connections[conn_data.connection_id] = conn_data
        return NodeGraphStructureData(db_uid, nodes, connections)


@dataclass
class WorkerResources(IBufferSerializable):
    cpu_count: float
    total_cpu_count: float
    cpu_mem: int
    total_cpu_mem: int
    gpu_count: float
    total_gpu_count: float
    gpu_mem: int
    total_gpu_mem: int

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>ddQQddQQ', self.cpu_count, self.total_cpu_count, self.cpu_mem, self.total_cpu_mem,
                                 self.gpu_count, self.total_gpu_count, self.gpu_mem, self.total_gpu_mem))

    @classmethod
    def deserialize(cls, stream: BufferedReader) -> "WorkerResources":
        cpu_count, total_cpu_count, cpu_mem, total_cpu_mem, \
        gpu_count, total_gpu_count, gpu_mem, total_gpu_mem = struct.unpack('>ddQQddQQ', stream.readexactly(64))
        return WorkerResources(cpu_count, total_cpu_count, cpu_mem, total_cpu_mem,
                               gpu_count, total_gpu_count, gpu_mem, total_gpu_mem)


@dataclass
class WorkerData(IBufferSerializable):
    id: int
    worker_resources: WorkerResources
    hwid: str
    last_address: str
    last_seen_timestamp: int
    state: WorkerState
    type: WorkerType
    current_invocation_node_id: Optional[int]
    current_invocation_task_id: Optional[int]
    current_invocation_id: Optional[int]
    current_invocation_progress: Optional[float]
    groups: Set[str]

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QQII?Q?Q?Q?dQ', self.id, self.last_seen_timestamp, self.state.value, self.type.value,
                                 self.current_invocation_node_id is not None, self.current_invocation_node_id or 0,
                                 self.current_invocation_task_id is not None, self.current_invocation_task_id or 0,
                                 self.current_invocation_id is not None, self.current_invocation_id or 0,
                                 self.current_invocation_progress is not None, self.current_invocation_progress or 0.0,
                                 len(self.groups)))
        self.worker_resources.serialize(stream)
        _serialize_string(self.hwid, stream)
        _serialize_string(self.last_address, stream)
        for group in self.groups:
            _serialize_string(group, stream)

    @classmethod
    def deserialize(cls, stream: BufferedReader) -> "WorkerData":
        worker_id, last_seen_timestamp, state_value, type_value, \
            has_current_invocation_node_id, current_invocation_node_id, \
            has_current_invocation_task_id, current_invocation_task_id, \
            has_current_invocation_id, current_invocation_id, \
            has_current_invocation_progress, current_invocation_progress, \
            groups_count = struct.unpack('>QQII?Q?Q?Q?dQ', stream.readexactly(68))
        if not has_current_invocation_node_id:
            current_invocation_node_id = None
        if not has_current_invocation_task_id:
            current_invocation_task_id = None
        if not has_current_invocation_id:
            current_invocation_id = None
        if not has_current_invocation_progress:
            current_invocation_progress = None
        worker_resources = WorkerResources.deserialize(stream)
        hwid = _deserialize_string(stream)
        last_address = _deserialize_string(stream)
        groups = set()
        for i in range(groups_count):
            groups.add(_deserialize_string(stream))
        return WorkerData(worker_id, worker_resources, hwid, last_address, last_seen_timestamp, WorkerState(state_value), WorkerType(type_value),
                          current_invocation_node_id, current_invocation_task_id, current_invocation_id, current_invocation_progress, groups)


@dataclass
class WorkerBatchData(IBufferSerializable):
    db_uid: int
    workers: Dict[int, WorkerData]

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QQ', self.db_uid, len(self.workers)))
        for task in self.workers.values():
            task.serialize(stream)

    @classmethod
    def deserialize(cls, stream: BufferedReader) -> "WorkerBatchData":
        db_uid, tasks_count, = struct.unpack('>QQ', stream.readexactly(16))
        workers = {}
        for i in range(tasks_count):
            task_data = WorkerData.deserialize(stream)
            workers[task_data.id] = task_data
        return WorkerBatchData(db_uid, workers)


@dataclass
class TaskGroupStatisticsData(IBufferSerializable):
    tasks_done: int
    tasks_in_progress: int
    tasks_with_error: int
    tasks_total: int

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QQQQ', self.tasks_done or 0, self.tasks_in_progress or 0, self.tasks_with_error or 0, self.tasks_total or 0))

    @classmethod
    def deserialize(cls, stream: BufferedReader) -> "TaskGroupStatisticsData":
        tasks_done, tasks_in_progress, tasks_with_error, tasks_total = struct.unpack('>QQQQ', stream.readexactly(32))
        return TaskGroupStatisticsData(tasks_done, tasks_in_progress, tasks_with_error, tasks_total)


@dataclass
class TaskGroupData(IBufferSerializable):
    name: str
    creation_timestamp: int
    state: TaskGroupArchivedState
    priority: float
    statistics: Optional[TaskGroupStatisticsData]

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QId?', self.creation_timestamp, self.state.value, self.priority, self.statistics is not None))
        _serialize_string(self.name, stream)
        if self.statistics is not None:
            self.statistics.serialize(stream)

    @classmethod
    def deserialize(cls, stream: BufferedReader) -> "TaskGroupData":
        ctimestamp, state_value, priority, has_statistics = struct.unpack('>QId?', stream.readexactly(21))
        name = _deserialize_string(stream)
        if has_statistics:
            statistics = TaskGroupStatisticsData.deserialize(stream)
        else:
            statistics = None
        return TaskGroupData(name, ctimestamp, TaskGroupArchivedState(state_value), priority, statistics)


@dataclass
class TaskGroupBatchData(IBufferSerializable):
    db_uid: int
    task_groups: Dict[str, TaskGroupData]

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QQ', self.db_uid, len(self.task_groups)))
        for task_group in self.task_groups.values():
            task_group.serialize(stream)

    @classmethod
    def deserialize(cls, stream: BufferedReader) -> "TaskGroupBatchData":
        db_uid, tasks_count, = struct.unpack('>QQ', stream.readexactly(16))
        task_groups = {}
        for i in range(tasks_count):
            group_data = TaskGroupData.deserialize(stream)
            task_groups[group_data.name] = group_data
        return TaskGroupBatchData(db_uid, task_groups)


@dataclass
class UiData(IBufferSerializable):  # Deprecated, should not be used any more
    db_uid: int
    graph_data: Optional[NodeGraphStructureData]
    tasks: Optional[TaskBatchData]
    workers: Optional[WorkerBatchData]
    task_groups: Optional[TaskGroupBatchData]

    def serialize(self, stream):
        buffer = BytesIO()

        buffer.write(struct.pack('>Q', self.db_uid))
        for data in (self.graph_data, self.tasks, self.workers, self.task_groups):
            buffer.write(struct.pack('>?', data is not None))
            if data is not None:
                data.serialize(buffer)

        lzdata = lz4.frame.compress(buffer.getbuffer())
        stream.write(struct.pack('>Q', len(lzdata)))
        stream.write(lzdata)

    @classmethod
    def deserialize(cls, stream: BufferedReader) -> "UiData":
        buffer = BytesIO(lz4.frame.decompress(stream.readexactly(struct.unpack('>Q', stream.readexactly(8))[0])))
        reader = BufferedReader(io.BufferedReader(buffer), 8192)

        db_uid, = struct.unpack('>Q', reader.readexactly(8))
        datas = []
        for data_type in (NodeGraphStructureData, TaskBatchData, WorkerBatchData, TaskGroupBatchData):
            if struct.unpack('>?', reader.readexactly(1))[0]:
                datas.append(data_type.deserialize(reader))
            else:
                datas.append(None)

        assert len(datas), 4
        return UiData(db_uid, datas[0], datas[1], datas[2], datas[3])

    def __repr__(self):
        return f'{self.graph_data} :::: {self.tasks}'  # TODO: this is not a good representation at all


@dataclass
class IncompleteInvocationLogData(IBufferSerializable):
    invocation_id: int
    worker_id: int
    invocation_runtime: Optional[float]

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QQ?d', self.invocation_id, self.worker_id, self.invocation_runtime is not None, self.invocation_runtime or 0.0))

    @classmethod
    def deserialize(cls, stream: BufferedReader):
        i_id, w_id, has_i_rt, i_rt = struct.unpack('>QQ?d', stream.readexactly(25))
        if not has_i_rt:
            i_rt = None
        return IncompleteInvocationLogData(i_id, w_id, i_rt)


@dataclass
class InvocationLogData(IBufferSerializable):
    invocation_id: int
    worker_id: int
    invocation_runtime: Optional[float]
    task_id: int
    node_id: int
    invocation_state: InvocationState
    return_code: Optional[int]
    stdout: str
    stderr: str

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QQ?dQQI?Q', self.invocation_id, self.worker_id, self.invocation_runtime is not None,
                                 self.invocation_runtime or 0.0, self.task_id, self.node_id, self.invocation_state.value,
                                 self.return_code is not None, self.return_code or 0))
        _serialize_string(self.stdout, stream)
        _serialize_string(self.stderr, stream)

    @classmethod
    def deserialize(cls, stream: BufferedReader):
        i_id, w_id, has_i_rt, i_rt, t_id, n_id, i_s_raw, has_ret_code, ret_code = struct.unpack('>QQ?dQQI?Q', stream.readexactly(54))
        if not has_i_rt:
            i_rt = None
        if not has_ret_code:
            ret_code = None
        stdout = _deserialize_string(stream)
        stderr = _deserialize_string(stream)
        return InvocationLogData(i_id, w_id, i_rt, t_id, n_id, InvocationState(i_s_raw), ret_code, stdout, stderr)
