"""
this module is supposed to be accessible for tasks ran by worker.
this module should be kept minimal, with only standard modules and maximum compatibility
"""
from __future__ import print_function
import os
import errno
import pickle
import threading
import socket
import struct
try:
    from typing import Optional
except ImportError:
    pass


class TaskSpawn:
    """
    this class is a pickle compatible shrunk copy of lifeblood.taskspawn.TaskSpawn
    keep it up-to-date
    and keep it 2-3 compatible!!
    """
    def __init__(self, name, source_invocation_id, env_args=None, task_attributes=None):
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

    def default_priority(self):  # type: () -> Optional[float]
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

    def set_default_priority(self, priority):  # type: (float) -> None
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

    def serialize(self):
        return pickle.dumps(self)


_threads_to_wait = []
_threads_to_wait_lock = threading.Lock()


def create_task(name, attributes, blocking=False):
    invocation_id = int(os.environ['LIFEBLOOD_RUNTIME_IID'])
    spawn = TaskSpawn(name, invocation_id, task_attributes=attributes)

    def _send():
        addrport = os.environ['LIFEBLOOD_RUNTIME_SCHEDULER_ADDR']
        addr, sport = addrport.rsplit(':', 1)
        port = int(sport)
        sock = socket.create_connection((addr, port), timeout=30)
        data = spawn.serialize()
        sock.sendall(b'\0\0\0\0')
        sock.sendall(b'spawn\n')
        sock.sendall(struct.pack('>Q', len(data)))
        sock.sendall(data)
        res = sock.recv(4)  # 4 should be small enough to ensure receiving in one call
        # ignore result?

        if not blocking:
            _clear_me_from_threads_to_wait()

    if blocking:
        _send()
    else:
        thread = threading.Thread(target=_send)
        _threads_to_wait.append(thread)
        thread.start()  # and not care


def set_attributes(attribs, blocking=False):  # type: (dict, bool) -> None
    def _send():
        addrport = os.environ['LIFEBLOOD_RUNTIME_SCHEDULER_ADDR']
        addr, sport = addrport.rsplit(':', 1)
        port = int(sport)
        sock = socket.create_connection((addr, port), timeout=30)
        sock.sendall(b'\0\0\0\0')
        sock.sendall(b'tupdateattribs\n')
        updata = pickle.dumps(attribs)
        sock.sendall(struct.pack('>QQQ', task_id, len(updata), 0))
        sock.sendall(updata)
        sock.recv(1)  # recv confirmation

        if not blocking:
            _clear_me_from_threads_to_wait()

    task_id = int(os.environ['LIFEBLOOD_RUNTIME_TID'])

    if blocking:
        _send()
    else:
        thread = threading.Thread(target=_send)
        with _threads_to_wait_lock:
            _threads_to_wait.append(thread)
        thread.start()  # and not care


def _clear_me_from_threads_to_wait():
    with _threads_to_wait_lock:
        me = threading.current_thread()
        try:
            _threads_to_wait.remove(me)
        except ValueError:  # not in the list
            pass


def wait_for_all_async_operations():
    """
    The python program will not end until all non-daemon threads are done.
    So skipping explicit wait for most of the operations is fine

    But sometimes you might need to wait for all current async operations to finish
    and for that you can use this function

    :return:
    """
    global _threads_to_wait
    with _threads_to_wait_lock:
        for thread in _threads_to_wait:
            thread.join()
        _threads_to_wait.clear()


def get_host_ip():
    """
    returns the ip address of the host running the task.
    if host has multiple interfaces - the one that can connect to the scheduler will be chosen

    :return:
    """
    addrport = os.environ['LIFEBLOOD_RUNTIME_SCHEDULER_ADDR']
    addr, sport = addrport.rsplit(':', 1)

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((addr, 1))
        myip = s.getsockname()[0]
    except Exception:
        myip = '127.0.0.1'
    finally:
        s.close()
    return myip


def get_free_tcp_port(ip, starting_at=20001):
    """
    returns the next openable port starting at starting_at on the given interface
    NOTE: that port is not reserved, so by the time you try to open it again after this function
    it may be already taken

    :param ip:
    :param starting_at:
    :return:
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    for starting_at in range(starting_at, starting_at + 2000):  # big barely sane number of tries, but not infinite
        try:
            s.bind((ip, starting_at))
            s.close()
            break
        except OSError as e:
            if e.errno == errno.EADDRINUSE:
                continue
            raise
    return starting_at
