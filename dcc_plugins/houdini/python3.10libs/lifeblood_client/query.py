import socket
import struct
import pickle
import json
import time
from .nethelpers import recv_exactly, send_string, recv_string
from .environment_resolver import EnvironmentResolverArguments

try:
    from typing import Optional, Tuple, List
except ImportError:  # py2 does not have typing module, but adequete IDE will still pick it up
    pass


class scheduler_client:
    def __init__(self, ip, port):
        self.__addr = (ip, port)
        self.__socket = None  # type: Optional[socket.socket]

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        if self.__socket is not None:
            return

        addr, sport = self.__addr
        port = int(sport)
        self.__socket = socket.create_connection((addr, port), timeout=30)
        self.__socket.sendall(b'\0\0\0\0')

    def close(self):
        if self.__socket is None:
            return
        self.__socket.close()

    def find_nodes_by_name(self, node_name):  # type: (str) -> Tuple[int, ...]
        send_string(self.__socket, 'nodenametoid')

        send_string(self.__socket, node_name)
        num = struct.unpack('>Q', recv_exactly(self.__socket, 8))[0]
        ids = struct.unpack('>' + 'Q'*num, recv_exactly(self.__socket, 8*num))
        return ids

    def find_tasks_by_name(self, task_name):  # type: (str) -> Tuple[int, ...]
        send_string(self.__socket, 'tasknametoid')

        send_string(self.__socket, task_name)
        num = struct.unpack('>Q', recv_exactly(self.__socket, 8))[0]
        ids = struct.unpack('>' + 'Q'*num, recv_exactly(self.__socket, 8*num))
        return ids

    def get_task_attributes(self, task_id):
        attribs, _ = self.get_task_attributes_and_env(task_id)
        return attribs

    def get_task_environment_resolver_arguments(self, task_id):
        attribs, _ = self.get_task_attributes_and_env(task_id)
        return attribs

    def get_task_attributes_and_env(self, task_id):
        send_string(self.__socket, 'gettaskattribs')

        self.__socket.sendall(struct.pack('>Q', task_id))
        data_len = struct.unpack('>Q', recv_exactly(self.__socket, 8))[0]
        data_attribs = recv_exactly(self.__socket, data_len)
        data_len = struct.unpack('>Q', recv_exactly(self.__socket, 8))[0]
        data_env = recv_exactly(self.__socket, data_len)

        attribs = pickle.loads(data_attribs)
        env = EnvironmentResolverArguments.deserialize(data_env)

        return attribs, env

    def get_task_fields(self, task_id):
        send_string(self.__socket, 'gettaskstate')

        self.__socket.sendall(struct.pack('>Q', task_id))
        data_len = struct.unpack('>Q', recv_exactly(self.__socket, 8))[0]
        data = recv_exactly(self.__socket, data_len)

        return json.loads(data)


class Task:
    class TaskState:  # note, this is NOT a ENUM, to keep py2 compatibility without extra deps
        WAITING = 0  # arrived at node, does not know what to do
        GENERATING = 1  # node is generating work load
        READY = 2  # ready to be scheduled
        INVOKING = 11  # task is being switched to IN_PROGRESS
        IN_PROGRESS = 3  # is being worked on by a worker
        POST_WAITING = 4  # task is waiting to be post processed by node
        POST_GENERATING = 5  # task is being post processed by node
        DONE = 6  # done, needs further processing
        ERROR = 7  # some internal error, not allowing to process task. NOT INVOCATION ERROR
        SPAWNED = 8  # spawned tasks are just passed down from node's "spawned" output
        DEAD = 9  # task will not be processed any more
        SPLITTED = 10  # task has been splitted, and will remain idle until splits are gathered

    def __init__(self, server_address, task_id):  # type: (Tuple[str, int], int) -> None
        self.__id = task_id
        self.__name = ''
        self.__parent_id = -1
        self.__children_count = -1
        self.__active_children_count = -1
        self.__split_level = -1
        self.__dead = False
        self.__paused = False
        self.__state = Task.TaskState.ERROR

        self.__fields_update_timestamp = 0
        self.__attribs_update_timestamp = 0

        self.__attribs = {}
        self.__env_resolver_args = None

        self.__cache_validity_timeout = 2
        self.__addr = server_address

    def _ensure_fields_fetched(self):
        if time.time() - self.__fields_update_timestamp < self.__cache_validity_timeout:
            return
        with scheduler_client(*self.__addr) as client:
            data_dict = client.get_task_fields(self.__id)
        self.__id = data_dict['id']
        self.__name = data_dict['name']
        self.__parent_id = data_dict['parent_id']
        self.__children_count = data_dict['children_count']
        self.__active_children_count = data_dict['active_children_count']
        self.__split_level = data_dict['split_level']
        self.__dead = bool(data_dict['dead'])
        self.__paused = bool(data_dict['paused'])
        self.__state = data_dict['state']
        self.__fields_update_timestamp = time.time()

    def _ensure_attributes_fetched(self):
        if time.time() - self.__attribs_update_timestamp < self.__cache_validity_timeout:
            return
        with scheduler_client(*self.__addr) as client:
            self.__attribs, self.__env_resolver_args = client.get_task_attributes_and_env(self.__id)
        self.__attribs_update_timestamp = time.time()

    @property
    def id(self):
        self._ensure_fields_fetched()
        return self.__id

    @property
    def name(self):
        self._ensure_fields_fetched()
        return self.__name

    @property
    def parent_id(self):
        self._ensure_fields_fetched()
        return self.__parent_id

    @property
    def children_count(self):
        self._ensure_fields_fetched()
        return self.__children_count

    @property
    def active_children_count(self):
        self._ensure_fields_fetched()
        return self.__active_children_count

    @property
    def split_level(self):
        self._ensure_fields_fetched()
        return self.__split_level

    @property
    def dead(self):
        self._ensure_fields_fetched()
        return self.__dead

    @property
    def paused(self):
        self._ensure_fields_fetched()
        return self.__paused

    @property
    def state(self):
        self._ensure_fields_fetched()
        return self.__state

    def get_attributes(self):
        self._ensure_attributes_fetched()
        return self.__attribs

    def get_attribute_names(self):
        self._ensure_attributes_fetched()
        return tuple(self.__attribs.keys())

    def get_attribute_value(self, attrib_name):
        self._ensure_attributes_fetched()
        return self.__attribs[attrib_name]

    def get_environment_resolver_arguments(self):
        self._ensure_attributes_fetched()
        return self.__env_resolver_args


def get_task_by_id(scheduler_address, task_id):  # type: (Tuple[str, int], int) -> Task
    return Task(scheduler_address, task_id)


def get_tasks_by_name(scheduler_address, task_name):  # type: (Tuple[str, int], str) -> List[Task]
    with scheduler_client(*scheduler_address) as client:
        return [Task(scheduler_address, x) for x in client.find_tasks_by_name(task_name)]
