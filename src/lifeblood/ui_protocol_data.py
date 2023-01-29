import lz4.frame
import struct
import asyncio
from io import BytesIO, BufferedIOBase
import pickle
from .enums import UIDataType, TaskState, WorkerState, WorkerType, TaskGroupArchivedState
from dataclasses import dataclass

from typing import Dict, List, Tuple, Optional, Set


# scheduler helpers
async def create_uidata_from_raw(db_uid, ui_nodes, ui_connections, ui_tasks, ui_workers, all_task_groups):
    """
    helper function to create structured data from raw dicts
    """
    asyncio.get_event_loop().t
    UiData(UIDataType.FULL, db_uid, )
    #return UiData(db_uid, ui_nodes, ui_connections, ui_tasks, ui_workers, all_task_groups)


def _create_uidata_from_raw_noasync(event_type: UIDataType, db_uid, ui_nodes, ui_connections, ui_tasks, ui_workers, all_task_groups):
    if ui_nodes is None or ui_connections is None:
        if ui_connections is not None or ui_connections is not None:
            raise RuntimeError('both ui_nodes and ui_connections must be none, or not none')
    nodes = {}
    connections = {}
    for node_id, node_raw in ui_nodes.items:
        assert node_id == node_raw['id']
        node_data = NodeData(node_id, node_raw['type'])
        nodes[node_id] = node_data
    for conn_id, con_raw in ui_connections:
        assert conn_id == con_raw['id']
        conn_data = NodeConnectionData(conn_id, con_raw['node_id_in'], con_raw['in_name'], con_raw['node_id_out'], con_raw['out_name'])
        connections[conn_id] = conn_data

    tasks = {}
    for task_id, task_raw in ui_tasks:
        assert task_id == task_raw['id']
        task_data = TaskData(task_id, task_raw['parent_id'], task_raw['children_count'], task_raw['active_children_count'],
                             TaskState(task_raw['state']), task_raw['state_details'], task_raw['paused'] != 0, task_raw['node_id'],
                             task_raw['node_input_name'], task_raw['node_output_name'], task_raw['name'], task_raw['split_level'],
                             task_raw['progress'], task_raw['work_data_invocation_attempt'], task_raw['origin_task_id'],
                             task_raw['split_id'], task_raw['invoc_id'], task_raw['groups'])
        tasks[task_id] = task_data

    workers = {}
    for worker_id, worker_raw in ui_workers:
        assert worker_id == worker_raw['id']
        res = WorkerResources(worker_raw['cpu_count'], worker_raw['total_cpu_count'],
                              worker_raw['cpu_mem'], worker_raw['total_cpu_mem'],
                              worker_raw['gpu_count'], worker_raw['total_gpu_count'],
                              worker_raw['gpu_mem'], worker_raw['total_gpu_mem'])
        workers[worker_id] = WorkerData(worker_id, res, worker_raw['hwid'], worker_raw['last_address'], worker_raw['last_seen'],
                                        WorkerState(worker_raw['state']), WorkerType(worker_raw['worker_type']),
                                        worker_raw['node_id'], worker_raw['task_id'], worker_raw['invoc_id'], worker_raw['progress'],
                                        worker_raw['groups'])

    task_groups = {}
    for group_name, group_raw in all_task_groups:
        assert group_name == group_raw['name']
        stat = TaskGroupStatisticsData(group_raw['tdone'], group_raw['tprog'], group_raw['terr'], group_raw['tall'])
        task_groups[group_name] = TaskGroupData(group_name, group_raw['ctime'], TaskGroupArchivedState(group_raw['state']),
                                                group_raw['priority'], stat)
    return UiData(event_type, db_uid, NodeGraphStructureData(nodes, connections), TaskBatchData(tasks), WorkerBatchData(workers), task_groups)

##


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
    progress: float
    split_origin_task_id: int
    split_id: int
    invocation_id: int
    groups: Set[str]

    def serialize(self, stream: BufferedIOBase):
        #                    ipcaspnswpssig
        data = struct.pack('>QQQQI?QQQdQQQQ', self.id, self.parent_id, self.children_count, self.active_children_count,
                           self.state.value, self.paused, self.node_id, self.split_level, self.work_data_invocation_attempt,
                           self.progress, self.split_origin_task_id, self.split_id, self.invocation_id, len(self.groups))
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
            task_state_value, task_paused, task_node_id, task_split_level, task_work_data_invocation_attempt, progress, \
            task_split_origin_task_id, task_split_id, task_invocation_id, group_count = struct.unpack('>QQQQI?QQQdQQQQ', stream.read(offset))

        state_details = _deserialize_string(stream)
        node_input_name = _deserialize_string(stream)
        node_output_name = _deserialize_string(stream)
        task_name = _deserialize_string(stream)

        groups = set()
        for i in range(group_count):
            group_name = _deserialize_string(stream)
            groups.update(group_name)

        return TaskData(task_id, task_parent_id, task_children_count, task_active_children_count,
                        TaskState(task_state_value), state_details, task_paused, task_node_id, node_input_name, node_output_name,
                        task_name, task_split_level, task_work_data_invocation_attempt, progress,
                        task_split_origin_task_id, task_split_id, task_invocation_id, groups)


@dataclass
class TaskBatchData(IBufferSerializable):
    tasks: Dict[int, TaskData]

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>Q', len(self.tasks)))
        for task in self.tasks.values():
            task.serialize(stream)

    @classmethod
    def deserialize(cls, stream: BufferedIOBase) -> "TaskBatchData":
        tasks_count, = struct.unpack('>Q', stream.read(8))
        tasks = {}
        for i in range(tasks_count):
            task_data = TaskData.deserialize(stream)
            tasks[task_data.id] = task_data
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
    def deserialize(cls, stream: BufferedIOBase) -> "NodeConnectionData":
        connection_id, in_id, out_id = struct.unpack('>QQQ', stream.read(16))
        in_name = _deserialize_string(stream)
        out_name = _deserialize_string(stream)
        return NodeConnectionData(connection_id, in_id, in_name, out_id, out_name)


@dataclass
class NodeGraphStructureData(IBufferSerializable):
    nodes: Dict[int, NodeData]
    connections: Dict[int, NodeConnectionData]

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>QQ', len(self.nodes), len(self.connections)))
        for node in self.nodes.values():
            node.serialize(stream)
        for connection in self.connections.values():
            connection.serialize(stream)

    @classmethod
    def deserialize(cls, stream: BufferedIOBase) -> "NodeGraphStructureData":
        nodes_count, connections_count = struct.unpack('>QQ', stream.read(16))
        nodes = {}
        connections = {}
        for i in range(nodes_count):
            node_data = NodeData.deserialize(stream)
            nodes[node_data.id] = node_data
        for i in range(connections_count):
            conn_data = NodeConnectionData.deserialize(stream)
            connections[conn_data.connection_id] = conn_data
        return NodeGraphStructureData(nodes, connections)


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
    def deserialize(cls, stream: BufferedIOBase) -> "WorkerData":
        worker_id, last_seen_timestamp, state_value, type_value, \
            has_current_invocation_node_id, current_invocation_node_id, \
            has_current_invocation_task_id, current_invocation_task_id, \
            has_current_invocation_id, current_invocation_id, \
            has_current_invocation_progress, current_invocation_progress, \
            groups_count = struct.unpack('>QQII?Q?Q?Q?dQ', stream.read(68))
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
            groups.update(_deserialize_string(stream))
        return WorkerData(worker_id, worker_resources, hwid, last_address, last_seen_timestamp, WorkerState(state_value), WorkerType(type_value),
                          current_invocation_node_id, current_invocation_task_id, current_invocation_id, current_invocation_progress, groups)


@dataclass
class WorkerBatchData(IBufferSerializable):
    workers: Dict[int, WorkerData]

    def serialize(self, stream: BufferedIOBase):
        stream.write(struct.pack('>Q', len(self.workers)))
        for task in self.workers.values():
            task.serialize(stream)

    @classmethod
    def deserialize(cls, stream: BufferedIOBase) -> "WorkerBatchData":
        tasks_count, = struct.unpack('>Q', stream.read(8))
        workers = {}
        for i in range(tasks_count):
            task_data = WorkerData.deserialize(stream)
            workers[task_data.id] = task_data
        return WorkerBatchData(workers)


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
    workers: Optional[WorkerBatchData]
    task_groups: Optional[Dict[str, TaskGroupData]]

    def serialize(self, stream):
        buffer = BytesIO()

        buffer.write(struct.pack('>IQ', self.event_type.value,  self.db_uid))
        for data in (self.graph_data, self.tasks, self.workers):
            buffer.write(struct.pack('>?', data is not None))
            if data is not None:
                data.serialize(buffer)
        for data_dict in (self.task_groups,):
            buffer.write(struct.pack('>?', data_dict is not None))
            if data_dict is not None:
                buffer.write(struct.pack('>Q', len(data_dict)))
                for data in data_dict.values():
                    data.serialize(buffer)

        lzdata = lz4.frame.compress(buffer.getbuffer())
        stream.write(struct.pack('>Q', len(lzdata)))
        stream.write(lzdata)

    @classmethod
    def deserialize(cls, stream: BufferedIOBase) -> "UiData":
        buffer = BytesIO(lz4.frame.decompress(stream.read(struct.unpack('>Q', stream.read(8))[0])))

        event_type_value, db_uid = struct.unpack('>IQ', buffer.read(12))
        datas = []
        for data_type in (NodeGraphStructureData, TaskBatchData, WorkerBatchData):
            if struct.unpack('>?', buffer.read(1))[0]:
                datas.append(data_type.deserialize(buffer))
            else:
                datas.append(None)

        if struct.unpack('>?', buffer.read(1))[0]:
            data_count, = struct.unpack('>Q', buffer.read(8))
            data_dict = {}
            for _ in range(data_count):
                data = TaskGroupData.deserialize(buffer)
                data_dict[data.name] = data
            datas.append(data_dict)
        else:
            datas.append(None)
        assert len(datas), 4
        return UiData(UIDataType(event_type_value), db_uid, datas[0], datas[1], datas[2], datas[3])

    async def serialize_to_streamwriter(self, stream: asyncio.StreamWriter):
        await asyncio.get_event_loop().run_in_executor(None, self.serialize, stream)

    def __repr__(self):
        return f'{self.graph_data} :::: {self.tasks}'  # TODO: this is not a good representation at all
