# TODO: this should be deployed automatically to this particular houdini folders
#  and to other places form from some central location AUTOMATICALLY

"""
this module is supposed to be accessible for tasks ran by worker.
this module should be kept minimal, with only standard modules and maximum compatibility
"""
from __future__ import print_function, absolute_import
import pickle
import socket
import struct
from .nethelpers import recv_string, recv_exactly, send_string


class TaskSpawn(object):
    """
    this class is a pickle compatible shrunk copy of taskflow.taskspawn.TaskSpawn
    keep it up-to-date
    and keep it 2-3 compatible!!
    """
    def __init__(self, name, source_invocation_id, **attribs):
        self.__name = name
        self.__attributes = attribs
        self.__forced_node_task_id_pair = None
        self.__from_invocation_id = source_invocation_id
        self.__output = 'spawned'
        self._create_as_spawned = True

    def create_as_spawned(self):
        return self._create_as_spawned

    def force_set_node_task_id(self, node_id, task_id):
        self.__forced_node_task_id_pair = (node_id, task_id)

    def forced_node_task_id(self):
        return self.__forced_node_task_id_pair

    def source_invocation_id(self):
        return self.__from_invocation_id

    def set_node_output_name(self, new_name):
        self.__output = new_name

    def node_output_name(self):
        return self.__output

    def name(self):
        return self.__name

    def set_name(self, name):
        self.__name = name

    def set_attribute(self, attr_name, attr_value):
        self.__attributes[attr_name] = attr_value

    def remove_attribute(self, attr_name):
        del self.__attributes[attr_name]

    def attribute_value(self, attr_name):
        return self.__attributes.get(attr_name, None)

    def _attributes(self):
        return self.__attributes

    def serialize(self):
        return pickle.dumps(self)


class NewTask(TaskSpawn):
    def __init__(self, name, node_id, **attribs):
        super(NewTask, self).__init__(name, None, **attribs)
        self.set_node_output_name('main')
        self.force_set_node_task_id(node_id, None)
        self._create_as_spawned = False

    def submit(self):
        scheduler_address = ('127.0.0.1', 7979)  # for test
        addr, sport = scheduler_address
        port = int(sport)
        sock = socket.create_connection((addr, port), timeout=30)
        sock.sendall(b'\0\0\0\0')
        data = self.serialize()

        sock.sendall(b'spawn\n')
        sock.sendall(struct.pack('>Q', len(data)))
        sock.sendall(data)
        res = struct.unpack('>I', sock.recv(4))[0]
        if res != 0:
            raise RuntimeError('scheduler failed to create task')


def create_task(name, node_id_or_name, **attributes):
    if isinstance(node_id_or_name, str):
        scheduler_address = ('127.0.0.1', 7979)  # for test
        addr, sport = scheduler_address
        port = int(sport)
        sock = socket.create_connection((addr, port), timeout=30)

        sock.sendall(b'\0\0\0\0')
        sock.sendall(b'nodenametoid\n')
        send_string(sock, node_id_or_name)
        print('foo')
        numids = struct.unpack('>Q', recv_exactly(sock, 8))[0]
        print(numids)
        if numids == 0:
            raise RuntimeError('taskflow graph does not have node named %s' % node_id_or_name)
        nodeids = struct.unpack('>' + 'Q'*numids, recv_exactly(sock, 8*numids))
        print(nodeids)
        node_id = nodeids[0]
    else:
        assert isinstance(node_id_or_name, int)
        node_id = node_id_or_name

    task = NewTask(name, node_id, **attributes)
    return task
