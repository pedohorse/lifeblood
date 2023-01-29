import lz4.frame
import struct
import asyncio
from io import BytesIO, BufferedIOBase
import pickle
from .enums import UIDataType, TaskState, WorkerState, WorkerType, TaskGroupArchivedState
from dataclasses import dataclass

from typing import List, Tuple, Optional


async def create_uidata(db_uid, ui_nodes, ui_connections, ui_tasks, ui_workers, all_task_groups):
    return UiData(db_uid, ui_nodes, ui_connections, ui_tasks, ui_workers, all_task_groups)


def _serialize_string(s: str, stream: BufferedIOBase) -> int:
    bstr = s.encode('UTF-8')
    stream.write(struct.pack('>Q', len(bstr)))
    return stream.write(bstr)


def _deserialize_string(stream: BufferedIOBase) -> str:
    bsize, = struct.unpack('>Q', stream.read(8))
    return bytes(stream.read(bsize)).decode('UTF-8')


class IBufferSerializable:
    def serialize(self, stream: BufferedIOBase):
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, stream: BufferedIOBase):
        raise NotImplementedError()


@dataclass
class TaskData(IBufferSerializable):
    id: int
    parent_id: int
    children_count: int
    active_children_count: int
    state: TaskState
    state_details: str
    paused: bool
    node_id: int
    node_input_name: str
    node_output_name: str
    name: str
    split_level: int
    work_data_invocation_attempt: int
    split_origin_task_id: int
    split_id: int
    invocation_id: int
    groups: List[str]

    def serialize(self, stream: BufferedIOBase):
        #                    ipcaspnswssig
        data = struct.pack('>QQQQI?QQQQQQQ', self.id, self.parent_id, self.children_count, self.active_children_count,
                           self.state.value, self.paused, self.node_id, self.split_level, self.work_data_invocation_attempt,
                           self.split_origin_task_id, self.split_id, self.invocation_id, len(self.groups))
        written_size = stream.write(data)
        data_parts = [_serialize_string(self.state_details, stream), _serialize_string(self.node_input_name, stream),
                      _serialize_string(self.node_output_name, stream), _serialize_string(self.name, stream),
                      *(_serialize_string(x, stream) for x in self.groups)]
        total_size = sum(len(x) for x in data_parts)
        for chunk in data_parts:
            written_size += stream.write(chunk)
        if total_size != written_size:
            raise RuntimeError('inconsistent write!')

    @classmethod
    def deserialize(cls, stream: BufferedIOBase) -> "TaskData":
        offset = 93  # 8*4 + 4 + 1 + 8*7
        task_id, task_parent_id, task_children_count, task_active_children_count, \
        task_state_value, task_paused, task_node_id, task_split_level, task_work_data_invocation_attempt, \
        task_split_origin_task_id, task_split_id, task_invocation_id, group_count = struct.unpack('>QQQQI?QQQQQQQ', stream.read(offset))

        state_details = _deserialize_string(stream)
        node_input_name = _deserialize_string(stream)
        node_output_name = _deserialize_string(stream)
        task_name = _deserialize_string(stream)

        groups = []
        for i in range(group_count):
            group_name = _deserialize_string(stream)
            groups.append(group_name)

        return TaskData(task_id, task_parent_id, task_children_count, task_active_children_count,
                        TaskState(task_state_value), state_details, task_paused, task_node_id, node_input_name, node_output_name,
                        task_name, task_split_level, task_work_data_invocation_attempt,
                        task_split_origin_task_id, task_split_id, task_invocation_id, groups)


@dataclass
class TaskBatchData(IBufferSerializable):
    tasks: List[TaskData]

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>Q', len(self.tasks)))
        for task in self.tasks:
            task.serialize(stream)

    @classmethod
    def deserialize(cls, stream: BufferedIOBase) -> "TaskBatchData":
        tasks_count, = struct.unpack('>Q', stream.read(8))
        tasks = []
        for i in range(tasks_count):
            tasks.append(TaskData.deserialize(stream))
        return TaskBatchData(tasks)


@dataclass
class NodeData(IBufferSerializable):
    id: int
    type: str

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>Q', self.id))
        _serialize_string(self.type, stream)

    @classmethod
    def deserialize(cls, stream: BufferedIOBase) -> "NodeData":
        node_id, = struct.unpack('>Q', stream.read(8))
        node_type = _deserialize_string(stream)
        return NodeData(node_id, node_type)


@dataclass
class NodeConnectionData(IBufferSerializable):
    in_id: int
    in_name: str
    out_id: int
    out_name: str

    def serialize(self, stream: BufferedIOBase):
        chunk = struct.pack('>QQ', self.in_id, self.out_id)
        stream.write(chunk)
        _serialize_string(self.in_name, stream)
        _serialize_string(self.out_name, stream)

    @classmethod
    def deserialize(cls, stream: BufferedIOBase) -> "NodeConnectionData":
        in_id, out_id = struct.unpack('>QQ', stream.read(16))
        in_name = _deserialize_string(stream)
        out_name = _deserialize_string(stream)
        return NodeConnectionData(in_id, in_name, out_id, out_name)


@dataclass
class NodeGraphStructureData(IBufferSerializable):
    nodes: List[NodeData]
    connections: List[NodeConnectionData]

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QQ', len(self.nodes), len(self.connections)))
        for node in self.nodes:
            node.serialize(stream)
        for connection in self.connections:
            connection.serialize(stream)

    @classmethod
    def deserialize(cls, stream: BufferedIOBase) -> "NodeGraphStructureData":
        nodes_count, connections_count = struct.unpack('>QQ', stream.read(16))
        nodes = []
        connections = []
        for i in range(nodes_count):
            nodes.append(NodeData.deserialize(stream))
        for i in range(connections_count):
            connections.append(NodeConnectionData.deserialize(stream))
        return NodeGraphStructureData(nodes, connections)


@dataclass
class WorkerResources(IBufferSerializable):
    cpu_count: int
    total_cpu_count: int
    cpu_mem: int
    total_cpu_mem: int
    gpu_count: int
    total_gpu_count: int
    gpu_mem: int
    total_gpu_mem: int

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QQQQQQQQ', self.cpu_count, self.total_cpu_count, self.cpu_mem, self.total_cpu_mem,
                                 self.gpu_count, self.total_gpu_count, self.gpu_mem, self.total_gpu_mem))

    @classmethod
    def deserialize(cls, stream: BufferedIOBase) -> "WorkerResources":
        cpu_count, total_cpu_count, cpu_mem, total_cpu_mem, \
        gpu_count, total_gpu_count, gpu_mem, total_gpu_mem = struct.unpack('>QQQQQQQQ', stream.read(64))
        return WorkerResources(cpu_count, total_cpu_count, cpu_mem, total_cpu_mem,
                               gpu_count, total_gpu_count, gpu_mem, total_gpu_mem)


@dataclass
class WorkerData(IBufferSerializable):
    id: int
    worker_resources: WorkerResources
    hwid: str
    last_address: str
    state: WorkerState
    type: WorkerType
    current_invocation_node_id: int
    current_invocation_task_id: int
    current_invocation_id: int
    groups: List[str]

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QIIQQQQ', self.id, self.state.value, self.type.value, self.current_invocation_node_id,
                                 self.current_invocation_task_id, self.current_invocation_id, len(self.groups)))
        self.worker_resources.serialize(stream)
        _serialize_string(self.hwid, stream)
        _serialize_string(self.last_address, stream)
        for group in self.groups:
            _serialize_string(group, stream)

    @classmethod
    def deserialize(cls, stream: BufferedIOBase) -> "WorkerData":
        worker_id, state_value, type_value, current_invocation_node_id, \
        current_invocation_task_id, current_invocation_id, groups_count = struct.unpack('>QIIQQQQ', stream.read(48))
        worker_resources = WorkerResources.deserialize(stream)
        hwid = _deserialize_string(stream)
        last_address = _deserialize_string(stream)
        groups = []
        for i in range(groups_count):
            groups.append(_deserialize_string(stream))
        return WorkerData(worker_id, worker_resources, hwid, last_address, WorkerState(state_value), WorkerType(type_value),
                          current_invocation_node_id, current_invocation_task_id, current_invocation_id, groups)


@dataclass
class TaskGroupStatisticsData(IBufferSerializable):
    tasks_done: int
    tasks_in_progress: int
    tasks_with_error: int
    tasks_total: int

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QQQQ',))

    @classmethod
    def deserialize(cls, stream) -> "TaskGroupStatisticsData":
        tasks_done, tasks_in_progress, tasks_with_error, tasks_total = struct.unpack('>QQQQ', stream.read(24))
        return TaskGroupStatisticsData(tasks_done, tasks_in_progress, tasks_with_error, tasks_total)


@dataclass
class TaskGroupData(IBufferSerializable):
    name: str
    creation_timestamp: int
    state: TaskGroupArchivedState
    priority: float
    statistics: TaskGroupStatisticsData

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QId', self.creation_timestamp, self.state.value, self.priority))
        _serialize_string(self.name, stream)
        self.statistics.serialize(stream)

    @classmethod
    def deserialize(cls, stream: BufferedIOBase) -> "TaskGroupData":
        ctimestamp, state_value, priority = struct.unpack('>QId', stream.read(20))
        name = _deserialize_string(stream)
        statistics = TaskGroupStatisticsData.deserialize(stream)
        return TaskGroupData(name, ctimestamp, TaskGroupArchivedState(state_value), priority, statistics)


@dataclass
class UiData(IBufferSerializable):
    event_type: UIDataType
    db_uid: int
    graph_data: Optional[NodeGraphStructureData]
    tasks: Optional[TaskBatchData]
    workers: Optional[List[WorkerData]]
    task_groups: Optional[List[TaskGroupData]]

    def serialize(self, stream):
        buffer = BytesIO()

        buffer.write(struct.pack('>IQ', self.event_type.value,  self.db_uid))
        for data in (self.graph_data, self.tasks):
            buffer.write(struct.pack('>?', data is not None))
            if data is not None:
                data.serialize(buffer)
        for data_list in (self.workers, self.task_groups):
            buffer.write(struct.pack('>?', data_list is not None))
            if data_list is not None:
                buffer.write(struct.pack('>Q', len(data_list)))
                for data in data_list:
                    data.serialize(buffer)

        lzdata = lz4.frame.compress(buffer.getbuffer())
        stream.write(struct.pack('>Q', len(lzdata)))
        stream.write(lzdata)

    @classmethod
    def deserialize(cls, stream: BufferedIOBase) -> "UiData":
        buffer = BytesIO(lz4.frame.decompress(stream.read(struct.unpack('>Q', stream.read(8))[0])))

        event_type_value, db_uid = struct.unpack('>IQ', buffer.read(12))
        datas = []
        for data_type in (NodeGraphStructureData, TaskBatchData):
            if struct.unpack('>?', buffer.read(1))[0]:
                datas.append(data_type.deserialize(buffer))
            else:
                datas.append(None)
        for data_type in (WorkerData, TaskGroupData):
            if struct.unpack('>?', buffer.read(1))[0]:
                data_count, = struct.unpack('>Q', buffer.read(8))
                data_list = []
                for _ in range(data_count):
                    data_list.append(data_type.deserialize(buffer))
                datas.append(data_list)
            else:
                datas.append(None)
        assert len(datas), 4
        return UiData(UIDataType(event_type_value), db_uid, datas[0], datas[1], datas[2], datas[3])

    async def serialize_to_streamwriter(self, stream: asyncio.StreamWriter, compress=False):
        buffer = BytesIO()
        if not compress:
            stream.write(b'\0\0\0\0')
            await asyncio.get_event_loop().run_in_executor(None, self.serialize, stream)
        else:
            stream.write(b'lz4\0')
            await asyncio.get_event_loop().run_in_executor(None, self.serialize, stream)

    def __repr__(self):
        return f'{self.graph_data} :::: {self.tasks}'  # TODO: this is not a good representation at all

    @classmethod
    def deserialize_to_streamwriter_noasync(cls, stream):
        pass

    @classmethod
    def deserialize_noasync(cls, data: bytes) -> "UiData":
        import socket
        socket.socket().makefile()
        cmp = data[:3]
        if cmp == b'lz4':
            return pickle.loads(lz4.frame.decompress(data[3:]))
        elif cmp == b'\0\0\0':
            return pickle.loads(data[3:])
        raise NotImplementedError(f'data compression format {repr(cmp)} is not implemented')

