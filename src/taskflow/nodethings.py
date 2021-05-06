from .invocationjob import InvocationJob
from .taskspawn import TaskSpawn

from typing import List, Dict, Any, Optional


class ProcessingResult:
    def __init__(self, job: InvocationJob = None, spawn: List[TaskSpawn] = None):
        self.invocation_job: InvocationJob = job
        self.spawn_list: List[TaskSpawn] = spawn
        self.do_kill_task: bool = False
        self.attributes_to_set: Optional[Dict[str, Any]] = {}
        self.do_split_remove: bool = False
        self.output_name: str = 'main'

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