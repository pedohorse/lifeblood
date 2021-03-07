"""
this module is supposed to be accessible for tasks ran by worker.
this module shuld be kept minimal, with only standard modules and maximum compatibility
"""
from __future__ import print_function
import os
import pickle
import threading
import socket
import struct

from typing import Optional, Tuple


class TaskSpawn:
    """
    this class is a pickle compatible shrunk copy of taskflow.taskspawn.TaskSpawn
    keep it up-to-date
    """
    def __init__(self, name: str, source_invocation_id: Optional[int], **attribs):
        self.__name = name
        self.__attributes = attribs
        self.__forced_node_task_id_pair = None
        self.__from_invocation_id = source_invocation_id

    def force_set_node_task_id(self, node_id, task_id):
        self.__forced_node_task_id_pair = (node_id, task_id)

    def forced_node_task_id(self) -> Optional[Tuple[int, int]]:
        return self.__forced_node_task_id_pair

    def source_invocation_id(self):
        return self.__from_invocation_id

    def name(self) -> str:
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

    def serialize(self) -> bytes:
        return pickle.dumps(self)


def create_task(name, **attributes):
    invocation_id = int(os.environ['TASKFLOW_RUNTIME_IID'])
    spawn = TaskSpawn(name, invocation_id, **attributes)

    def _send():
        addrport = os.environ['TASKFLOW_RUNTIME_SCHEDULER_ADDR']
        addr, sport = addrport.rsplit(':', 1)
        port = int(sport)
        sock = socket.create_connection((addr, port), timeout=30)
        data = spawn.serialize()
        sock.sendall(b'spawn\n')
        sock.sendall(struct.pack('>I', len(data)))
        sock.sendall(data)
        res = sock.recv(4)  # 4 should be small enough to ensure receiving in one call
        # ignore result?

    thread = threading.Thread(target=_send)
    thread.start()  # and not care
