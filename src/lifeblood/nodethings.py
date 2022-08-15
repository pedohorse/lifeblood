import json

from .invocationjob import InvocationJob
from .taskspawn import TaskSpawn
from .environment_resolver import EnvironmentResolverArguments

from typing import List, Dict, Any, Optional


class ProcessingError(RuntimeError):
    pass


class ProcessingResult:
    def __init__(self, job: Optional[InvocationJob] = None, spawn: List[TaskSpawn] = None, node_output_name: Optional[str] = None):
        self.invocation_job: Optional[InvocationJob] = job
        self.spawn_list: List[TaskSpawn] = spawn
        self.do_kill_task: bool = False
        self.attributes_to_set: Optional[Dict[str, Any]] = {}
        self.do_split_remove: bool = False
        self.split_attributes_to_set: Optional[Dict[str, Any]] = {}
        self.output_name: str = node_output_name
        self._split_attribs = None
        self._environment_resolver_arguments: Optional[EnvironmentResolverArguments] = None

    def set_node_output_name(self, newname: str):
        self.output_name = newname

    def kill_task(self):
        self.do_kill_task = True

    def remove_split(self, attributes_to_set=None):
        """
        seals this one split
        :param attributes_to_set:
        :return:
        """
        self.do_split_remove = True
        if attributes_to_set is not None:
            self.split_attributes_to_set.update(attributes_to_set)

    def set_attribute(self, key: str, value):
        self.attributes_to_set[key] = value

    def remove_attribute(self, key: str):
        self.attributes_to_set[key] = None

    def set_environment_resolver_arguments(self, args: EnvironmentResolverArguments):
        self._environment_resolver_arguments = args

    def add_spawned_task(self, spawn: TaskSpawn):
        if self.spawn_list is None:
            self.spawn_list = []
        self.spawn_list.append(spawn)

    def cancel_split_task(self):
        self._split_attribs = None

    def split_task(self, into: int):
        if into < 1:
            raise ValueError('cannot split into less than to 1 parts')

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

