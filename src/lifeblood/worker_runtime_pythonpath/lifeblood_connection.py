"""
this module is supposed to be accessible for tasks ran by worker.
this module should be kept minimal, with only standard modules and maximum compatibility
"""
from __future__ import print_function
import os
import time
import errno
import pickle
import json
import threading
import socket
import struct
import uuid
try:
    from typing import Optional, Tuple, Union
except ImportError:
    pass


class MessageSendError(RuntimeError):
    pass


class MessageReceiveTimeout(RuntimeError):
    pass


class InvocationMessageReceiverFuture(threading.Thread):
    def __init__(self, addressee, timeout=None):
        super().__init__()
        self.__addressee = addressee
        self.__timeout = timeout
        self.__result = None
        self.__exception = None

    def run(self) -> None:
        try:
            self.__result = _message_to_invocation_receive_blocking(self.__addressee, self.__timeout)
        except Exception as e:
            self.__exception = e
        _clear_me_from_threads_to_wait()

    def is_done(self):
        return not self.is_alive()

    def wait(self, timeout=None):  # type: (Optional[float]) -> Tuple[int, bytes]
        """
        wait for operation to finish and return result, or raise exception if error happened
        """
        self.join(timeout)
        if self.is_alive():  # timeout happened
            raise TimeoutError()
        if self.__exception:
            raise self.__exception
        return self.__result


def send_string(sock, text):  # type: (socket.socket, str) -> None
    bts = text.encode('UTF-8')
    sock.sendall(struct.pack('>Q', len(bts)))
    sock.sendall(bts)


def recv_string(sock):  # type: (socket.socket) -> str
    data_size, = struct.unpack('>Q', sock.recv(8))
    return sock.recv(data_size).decode('UTF-8')


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


def _connect_to_worker(timeout=None):
    addrport = os.environ['LIFEBLOOD_RUNTIME_SCHEDULER_ADDR']
    addr, sport = addrport.rsplit(':', 1)
    port = int(sport)
    if timeout is None:
        sock = socket.create_connection((addr, port))
    else:
        sock = socket.create_connection((addr, port), timeout=timeout)
    # negotiate protocol
    sock.sendall(struct.pack('>QII', 1, 1, 0))
    assert struct.unpack('>II', sock.recv(8)) == (1, 0), 'server does not support our protocol version'
    return sock


def create_task(name, attributes, env_arguments=None, blocking=False):
    """
    creates a new task with name and attributes.
    if env_attributes is None - environment is inherited fully from the parent,
      otherwise env_attributes completely override env from the parent task.

    if blocking is False - create_task creates a thread and exits immediately.
      this is the default as you would usually want to contact scheduler in parallel with actual work.
    """
    invocation_id = int(os.environ['LIFEBLOOD_RUNTIME_IID'])
    spawn = TaskSpawn(name, invocation_id, task_attributes=attributes, env_args=env_arguments)

    def _send():
        sock = _connect_to_worker(timeout=30)

        send_string(sock, 'spawn')

        data = spawn.serialize()
        sock.sendall(struct.pack('>Q', len(data)))
        sock.sendall(data)
        res = sock.recv(13)  # >I?Q  13 should be small enough to ensure receiving in one call
        good, is_not_none, task_id = struct.unpack('>I?Q', res)

        if not blocking:
            _clear_me_from_threads_to_wait()
        return task_id if is_not_none else -1

    if blocking:
        return _send()
    else:
        thread = threading.Thread(target=_send)
        _threads_to_wait.append(thread)
        thread.start()  # and not care


def set_attributes(attribs, blocking=False):  # type: (dict, bool) -> None
    def _send():
        sock = _connect_to_worker(timeout=30)

        send_string(sock, 'tupdateattribs')
        updata = json.dumps(attribs).encode('UTF-8')
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


def message_to_invocation_send(invocation_id, addressee, message, addressee_timeout=None):  # type: (int, str, bytes, Optional[float]) -> None
    """
    send a message to invocation_id, addressed to addressee.
    This is useful for designing workflows where:
     - child tasks are reporting processing results to running parent
     - child tasks are synchronizing through parent (useful for distributed sims)
     - other stuff

     NOTE: timeout=None means DEFAULT timeout, not no timeout. disabling timeout is not allowed
    """
    def _send():
        sock = _connect_to_worker()

        send_string(sock, 'sendinvmessage')

        sock.sendall(struct.pack('>QQQf', invocation_id, int(os.environ['LIFEBLOOD_RUNTIME_IID']), len(message), addressee_timeout))
        send_string(sock, addressee)
        sock.sendall(message)
        # now get reply
        reply = recv_string(sock)
        if reply != 'delivered':
            raise MessageSendError(reply)

    if addressee_timeout is None or addressee_timeout <= 0:
        addressee_timeout = 90
    _send()


def message_to_invocation_receive(addressee, timeout=None, blocking=True):  # type: (str, Optional[float], bool) -> Union[Tuple[int, bytes], InvocationMessageReceiverFuture]
    """
    await message from another invocation, addressed to addressee
    If graph is not properly designed so that another invocation may not send a message -
    this function will just block forever

    If blocking is False - it will return a Future-like object that caller can wait for.
    NOTE: this future object actually creates a thread, which may be undesireable
    """
    if blocking:
        return _message_to_invocation_receive_blocking(addressee, timeout)
    future = InvocationMessageReceiverFuture(addressee, timeout)
    future.start()
    with _threads_to_wait_lock:
        _threads_to_wait.append(future)
    return future


def _message_to_invocation_receive_blocking(addressee, timeout=None):  # type: (str, Optional[float]) -> Tuple[int, bytes]
    sock = _connect_to_worker()

    send_string(sock, 'recvinvmessage')

    send_string(sock, addressee)
    target_poll_time = 10.0
    min_poll_time = 0.1
    eps = 0.001
    poll_time = max(min_poll_time, (timeout+eps)/max(1.0, round((timeout+eps)/target_poll_time))) if timeout is not None else target_poll_time
    sock.sendall(struct.pack('>f', poll_time))
    ready = False
    waiting_elapsed = 0
    _last_timestamp = time.time()
    while not ready:
        _last_timestamp = time.time()
        ready, = struct.unpack('>?', sock.recv(1))

        waiting_elapsed += time.time() - _last_timestamp
        _last_timestamp = time.time()
        do_cancel = timeout is not None and waiting_elapsed >= timeout

        if not ready:
            sock.sendall(struct.pack('>?', do_cancel))  # False for no cancel, True for cancel
            if do_cancel:
                raise MessageReceiveTimeout(f'elapsed: {waiting_elapsed}s')
            continue
    source_iid, data_size = struct.unpack('>QQ', sock.recv(16))
    data = sock.recv(data_size)

    return source_iid, data


def _clear_me_from_threads_to_wait():
    with _threads_to_wait_lock:
        me = threading.current_thread()
        try:
            _threads_to_wait.remove(me)
        except ValueError:  # not in the list
            pass


def wait_for_currently_running_async_operations():
    """
    The python program will not end until all non-daemon threads are done.
    So skipping explicit wait for most of the operations is fine

    But sometimes you might need to wait for all current async operations to finish
    and for that you can use this function

    :return:
    """
    global _threads_to_wait
    with _threads_to_wait_lock:
        threads_list_snapshot = _threads_to_wait

    for thread in threads_list_snapshot:
        thread.join()

    with _threads_to_wait_lock:  # cleanup just in case
        for thread in threads_list_snapshot:
            if thread in _threads_to_wait:
                print('WARNING: uncleaned thread!')
                _threads_to_wait.remove(thread)


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


def generate_addressee():
    """
    generate random string, nothing more
    """
    return str(uuid.uuid4())


def get_my_invocation_id():
    """
    since this module is run in environment generated by worker,
    there are extra env variables available, including informatino about
    the task+invocation currently being worked on
    """
    return int(os.environ['LIFEBLOOD_RUNTIME_IID'])


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
