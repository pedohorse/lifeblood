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
from getpass import getuser
from .nethelpers import recv_string, recv_exactly, send_string
from .query import scheduler_client, Task


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
        self.__default_priority = None

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

    def default_priority(self):
        """
        This priority will be used only in case this task requires a default group creation
        If this task has nonempty list of groups to be assigned to - this default priority is

        :return: default priority
        """
        return self.__default_priority

    def add_extra_group_name(self, group_name):
        self.__extra_groups.append(group_name)

    def extra_group_names(self):
        return self.__extra_groups

    def set_default_priority(self, priority):
        """
        This priority will be used only in case this task requires a default group creation
        If this task has nonempty list of groups to be assigned to - this default priority is
        """
        self.__default_priority = priority

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
    def __init__(self, name, node_id, scheduler_addr, env_args, task_attributes, priority=50.0):
        super(NewTask, self).__init__(name, None, env_args, task_attributes)
        self.__scheduler_addr = scheduler_addr
        self.set_node_output_name('main')
        self.force_set_node_task_id(node_id, None)
        self._create_as_spawned = False
        self.set_default_priority(priority)

    def submit(self):
        addr, sport = self.__scheduler_addr
        port = int(sport)
        sock = socket.create_connection((addr, port), timeout=30)
        sock.sendall(b'\0\0\0\0')
        data = self.serialize()

        send_string(sock, 'spawn')
        sock.sendall(struct.pack('>Q', len(data)))
        sock.sendall(data)
        status, is_not_null, task_id = struct.unpack('>I?Q', sock.recv(13))
        if status != 0:
            raise RuntimeError('scheduler failed to create task')
        assert is_not_null
        return Task(self.__scheduler_addr, task_id)


def create_task(name, node_id_or_name,
                scheduler_addr,
                attributes=None,
                env_resolver_name='StandardEnvironmentResolver', env_arguments=None,
                priority=50.0):

    if attributes is None:
        attributes = {}
    if env_arguments is None:
        env_arguments = {'user': getuser()}
    env_res = EnvironmentResolverArguments(env_resolver_name, env_arguments)
    if isinstance(node_id_or_name, str):
        addr, sport = scheduler_addr
        port = int(sport)
        with scheduler_client(addr, port) as client:
            nodeids = client.find_nodes_by_name(node_id_or_name)

        if len(nodeids) == 0:
            raise RuntimeError('lifeblood graph does not have node named %s' % node_id_or_name)
        node_id = nodeids[0]
    else:
        assert isinstance(node_id_or_name, int)
        node_id = node_id_or_name

    task = NewTask(name, node_id, scheduler_addr, env_args=env_res, task_attributes=attributes, priority=priority)
    return task
