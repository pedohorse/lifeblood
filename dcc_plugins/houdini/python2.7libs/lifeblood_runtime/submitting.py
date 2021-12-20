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
    this class is a pickle compatible shrunk copy of lifeblood.taskspawn.TaskSpawn
    keep it up-to-date
    and keep it 2-3 compatible!!
    """
    def __init__(self, name, source_invocation_id, env_args, task_attributes):
        self.__name = name
        self.__attributes = dict(task_attributes or {})
        self.__env_args = env_args
        self.__forced_node_task_id_pair = None
        self.__from_invocation_id = source_invocation_id
        self.__output = 'spawned'
        self._create_as_spawned = True
        self.__extra_groups = []

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

    def add_extra_group_name(self, group_name):
        self.__extra_groups.append(group_name)

    def extra_group_names(self):
        return self.__extra_groups

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

    def environment_arguments(self):
        return self.__env_args

    def set_environment_resolver(self, resolver_name, resolver_arguments):
        self.__env_args = EnvironmentResolverArguments(resolver_name, resolver_arguments)

    def serialize(self):
        return pickle.dumps(self)


class EnvironmentResolverArguments:
    """
    this is a copy of envirionment_resolver.EnvironmentResolverArguments class purely for pickling
    """
    def __init__(self, resolver_name=None, arguments=None):
        if arguments is None:
            arguments = {}
        if resolver_name is None and len(arguments) > 0:
            raise ValueError('if name is None - no arguments are allowed')
        self.__resolver_name = resolver_name
        self.__args = arguments


class NewTask(TaskSpawn):
    def __init__(self, name, node_id, scheduler_addr, env_args, task_attributes):
        super(NewTask, self).__init__(name, None, env_args, task_attributes)
        self.__scheduler_addr = scheduler_addr
        self.set_node_output_name('main')
        self.force_set_node_task_id(node_id, None)
        self._create_as_spawned = False

    def submit(self):
        addr, sport = self.__scheduler_addr
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


def create_task(name, node_id_or_name, scheduler_addr, **attributes):
    if isinstance(node_id_or_name, str):
        addr, sport = scheduler_addr
        port = int(sport)
        sock = socket.create_connection((addr, port), timeout=30)

        sock.sendall(b'\0\0\0\0')
        sock.sendall(b'nodenametoid\n')
        send_string(sock, node_id_or_name)
        print('foo')
        numids = struct.unpack('>Q', recv_exactly(sock, 8))[0]
        print(numids)
        if numids == 0:
            raise RuntimeError('lifeblood graph does not have node named %s' % node_id_or_name)
        nodeids = struct.unpack('>' + 'Q'*numids, recv_exactly(sock, 8*numids))
        print(nodeids)
        node_id = nodeids[0]
    else:
        assert isinstance(node_id_or_name, int)
        node_id = node_id_or_name

    task = NewTask(name, node_id, scheduler_addr, None, task_attributes=attributes)
    return task