import io

import lz4.frame
import struct
from io import BytesIO, BufferedIOBase
from .buffered_connection import BufferedReader
from .buffer_serializable import IBufferSerializable
from .enums import TaskState, WorkerState, WorkerType, TaskGroupArchivedState
from dataclasses import dataclass

from typing import Dict, List, Tuple, Optional, Set


def _serialize_string(s: str, stream: BufferedIOBase) -> int:
    bstr = s.encode('UTF-8')
    stream.write(struct.pack('>Q', len(bstr)))
    return stream.write(bstr)


def _deserialize_string(stream: BufferedReader) -> str:
    bsize, = struct.unpack('>Q', stream.readexactly(8))
    return bytes(stream.readexactly(bsize)).decode('UTF-8')


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
