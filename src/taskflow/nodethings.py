import json

from .invocationjob import InvocationJob
from .taskspawn import TaskSpawn

from typing import List, Dict, Any, Optional


class ProcessingResult:
    def __init__(self, job: InvocationJob = None, spawn: List[TaskSpawn] = None, node_output_name: str = 'main'):
        self.invocation_job: InvocationJob = job
        self.spawn_list: List[TaskSpawn] = spawn
        self.do_kill_task: bool = False
        self.attributes_to_set: Optional[Dict[str, Any]] = {}
        self.do_split_remove: bool = False
        self.output_name: str = node_output_name
        self._split_attribs = None

    def set_node_output_name(self, newname: str):
        self.output_name = newname

    def kill_task(self):
        self.do_kill_task = True

    def remove_split(self):
        self.do_split_remove = True

    def set_attribute(self, key: str, value):
        self.attributes_to_set[key] = value

    def remove_attribute(self, key: str):
        self.attributes_to_set[key] = None

    def add_spawned_task(self, spawn: TaskSpawn):
        if self.spawn_list is None:
            self.spawn_list = []
        self.spawn_list.append(spawn)

    def cancel_split_task(self):
        self._split_attribs = None

    def split_task(self, into: int):
        if into <= 1:
            raise ValueError('cannot split into less than or eq to 1 parts')

        self._split_attribs = [{} for _ in range(into)]

    def set_split_task_attrib(self, split: int, attr_name: str, attr_value):
        try:
            json.dumps(attr_value)
        except:
            raise ValueError('attribs must be json-serializable dict')
        self._split_attribs[split][attr_name] = attr_value

    def set_split_task_attribs(self, split: int, attribs: dict):
        try:
            assert isinstance(attribs, dict)
            json.dumps(attribs)
        except:
            raise ValueError('attribs must be json-serializable dict')
        self._split_attribs[split] = attribs

