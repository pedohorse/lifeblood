import time
import json
from taskflow.basenode import BaseNode, AttributeType
from taskflow.invocationjob import InvocationJob
from taskflow.taskspawn import TaskSpawn
from taskflow.exceptions import NodeNotReadyToProcess

from threading import Lock

from typing import Dict, TypedDict, Set, List, Optional, Any


class SliceAwaiting(TypedDict):
    arrived: Set[int]
    awaiting: Set[int]


def create_node_object(name: str):
    return SliceWaiterNode()


def deserialize(data: bytes):
    obj = SliceWaiterNode()
    obj.__dict__.update(json.loads(data.decode('UTF-8')))
    return obj


class SliceWaiterNode(BaseNode):
    def __init__(self):
        self.__cache: Dict[int: SliceAwaiting] = {}
        self.__main_lock = Lock()

        self.__wait_for_all = True

    def process_task(self, task_dict) -> (InvocationJob, Optional[List[TaskSpawn]]): #TODO: not finished, attrib not taken into account, rethink return type
        split_id = task_dict['split_origin_task_id']
        if split_id is None:  # means no splits - just pass through
            return None, None
        with self.__main_lock:
            if split_id not in self.__cache:
                self.__cache[split_id] = {'arrived': set(), 'awaiting': set(range(task_dict['split_count']))}
            self.__cache[split_id]['arrived'].add(task_dict['split_id'])

        # we will not wait in loop or we risk deadlocking threadpool
        # check if everyone is ready
        with self.__main_lock:
            if self.__cache[split_id]['arrived'] == self.__cache[split_id]['awaiting']:
                return None, None
        raise NodeNotReadyToProcess()

    def postprocess_task(self, task_dict) -> (Dict[str, Any], Optional[List[TaskSpawn]]):
        return {}, None

    # attributes
    def attribs(self) -> Dict[str, AttributeType]:
        return {"wait for all": AttributeType.BOOL}

    def attrib_value(self, attrib_name):
        if attrib_name == 'wait for all':
            return self.__wait_for_all
        raise KeyError(attrib_name)

    def set_attrib_value(self, attrib_name, attrib_value):
        if attrib_name == 'wait for all':
            self.__wait_for_all = attrib_value
        raise KeyError(attrib_name)

    #serialization
    def serialize(self) -> bytes:
        attrs = self.__dict__
        del attrs['_SliceWaiterNode_main_lock']
        return json.dumps(attrs).encode('UTF-8')
