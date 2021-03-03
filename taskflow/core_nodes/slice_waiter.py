import time
import json
from taskflow.basenode import BaseNode
from taskflow.nodethings import ProcessingResult
from taskflow.taskspawn import TaskSpawn
from taskflow.exceptions import NodeNotReadyToProcess
from taskflow.enums import NodeParameterType
from taskflow.uidata import NodeUi

from threading import Lock

from typing import Dict, TypedDict, Set, List, Optional, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from taskflow.scheduler import Scheduler


class SliceAwaiting(TypedDict):
    arrived: Set[int]
    awaiting: Set[int]
    first_to_arrive: Optional[int]


def create_node_object(name: str, parent_scheduler):
    return SliceWaiterNode(name, parent_scheduler)


class SliceWaiterNode(BaseNode):
    def __init__(self, name: str, parent_scheduler: "Scheduler"):
        super(SliceWaiterNode, self).__init__(name, parent_scheduler)
        self.__cache: Dict[int: SliceAwaiting] = {}
        self.__main_lock = Lock()
        with self._parameters.initizlizing_interface_lock():
            self._parameters.add_parameter('wait for all', NodeParameterType.BOOL, True)

    def process_task(self, task_dict) -> ProcessingResult: #TODO: not finished, attrib not taken into account, rethink return type
        orig_id = task_dict['split_origin_task_id']
        split_id = task_dict['split_id']
        task_id = task_dict['id']
        if orig_id is None:  # means no splits - just pass through
            return ProcessingResult()
        with self.__main_lock:
            if split_id not in self.__cache:
                self.__cache[split_id] = {'arrived': set(),
                                          'awaiting': set(range(task_dict['split_count'])),
                                          'first_to_arrive': None}
            if self.__cache[split_id]['first_to_arrive'] is None and len(self.__cache[split_id]['arrived']) == 0:
                self.__cache[split_id]['first_to_arrive'] = task_id
            self.__cache[split_id]['arrived'].add(task_dict['split_element'])

        # we will not wait in loop or we risk deadlocking threadpool
        # check if everyone is ready
        if self._parameters.parameter_value('wait for all'):
            with self.__main_lock:
                if self.__cache[split_id]['arrived'] == self.__cache[split_id]['awaiting']:
                    res = ProcessingResult()
                    res.remove_split()
                    if orig_id != task_id:
                        res.kill_task()
                    return res
        else:
            with self.__main_lock:
                res = ProcessingResult()
                res.remove_split()
                if self.__cache[split_id]['first_to_arrive'] != task_id:
                    res.kill_task()
                return res

        raise NodeNotReadyToProcess()

    def postprocess_task(self, task_dict) -> ProcessingResult:
        return ProcessingResult()

    def __getstate__(self):
        d = super(SliceWaiterNode, self).__getstate__()
        assert '_SliceWaiterNode__main_lock' in d
        del d['_SliceWaiterNode__main_lock']
        return d
