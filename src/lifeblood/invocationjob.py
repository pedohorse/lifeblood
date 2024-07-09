import os
import re
from copy import copy, deepcopy
import json
import asyncio
import pickle
from types import MappingProxyType
from .enums import WorkerType
from dataclasses import dataclass, field

from typing import Optional, Iterable, Union, Dict, List, Set, Tuple, TYPE_CHECKING
if TYPE_CHECKING:
    from .environment_resolver import EnvironmentResolverArguments


Number = Union[int, float]


class InvocationNotFinished(RuntimeError):
    pass


class BadProgressRegexp(RuntimeError):
    pass


class Environment(dict):
    def __init__(self, *args, **kwargs):
        super(Environment, self).__init__(*args, **kwargs)
        self.__expandre = re.compile(r'\$(?:(\w+)|{(\w+)})')
        self.__extra_expand_dict = {}

    def set_extra_expand_dict(self, extra: dict):
        self.__extra_expand_dict = extra

    def expand(self, value: str) -> str:
        def _onmatch(match):
            key = match.group(1) or match.group(2)
            return self.get(key, self.__extra_expand_dict.get(key, None))
        return self.__expandre.sub(_onmatch, value)

    def __setitem__(self, key: str, value):
        if not isinstance(value, str):
            value = str(value)
        super(Environment, self).__setitem__(key, self.expand(value))

    def prepend(self, key: str, value):
        """
        treat key as path list and prepend to the list
        """
        if key not in self:
            self[key] = value
            return
        if not isinstance(value, str):
            value = str(value)
        self[key] = os.pathsep.join((self.expand(value), self[key]))

    def append(self, key: str, value):
        """
        treat key as path list and append to the list
        """
        if key not in self:
            self[key] = value
            return
        if not isinstance(value, str):
            value = str(value)
        self[key] = os.pathsep.join((self[key], self.expand(value)))


class InvocationEnvironment:
    def __init__(self, *args, **kwargs):
        super(InvocationEnvironment, self).__init__(*args, **kwargs)
        self.__action_queue = []

    def set_variable(self, key: str, value):
        if not isinstance(value, str):
            value = str(value)
        self.__action_queue.append(('__setitem__', key, value))

    def resolve(self, base_env: Optional[Environment] = None, additional_environment_to_expand_with: Optional[Environment] = None) -> Environment:
        """
        resolves action queue and produces final environment
        """
        if base_env is not None:
            env = copy(base_env)
        else:
            env = Environment()
        if additional_environment_to_expand_with is not None:
            env.set_extra_expand_dict(additional_environment_to_expand_with)
        for method, *args in self.__action_queue:
            getattr(env, method)(*args)
        return env

    def _enqueue_kv_method(self, method: str, key: str, value):
        if not isinstance(value, str):
            value = str(value)
        self.__action_queue.append((method, key, value))

    def prepend(self, key: str, value):
        self._enqueue_kv_method('prepend', key, value)

    def append(self, key: str, value):
        self._enqueue_kv_method('append', key, value)


@dataclass
class ResourceRequirement:
    min: Number = 0
    pref: Number = 0


class ResourceRequirements:
    """
    requirements for only resources
    """
    def __init__(self, data: Optional[Dict[str, ResourceRequirement]] = None):
        if data:
            self.__res: Dict[str, ResourceRequirement] = data
        else:
            self.__res: Dict[str, ResourceRequirement] = {}

    def get(self, name: str, default_val=None):
        return self.__res.get(name, default_val)

    def __getitem__(self, name: str) -> ResourceRequirement:
        """
        get requirements for resource "name"
        """
        return self.__res[name]

    def __setitem__(self, name: str, value: ResourceRequirement):
        self.__res[name] = value

    def __contains__(self, name):
        return name in self.__res

    def items(self):
        return self.__res.items()


@dataclass
class Requirements:
    resources: ResourceRequirements = field(default_factory=ResourceRequirements)
    # devices:   TO ADD LATER

    def serialize_to_string(self):
        """
        compact string representation
        """
        return json.dumps({
            'r': {name: {'m': val.min, 'p': val.pref} for name, val in self.resources.items()}
        })

    @classmethod
    def deserialize_from_string(cls, text: str) -> "Requirements":
        """
        reverse from serializa_to_stirng
        """
        data = json.loads(text)
        return Requirements(
            resources=ResourceRequirements({
                name: ResourceRequirement(val['m'], val['p']) for name, val in data['r'].items()
            })
        )


class InvocationRequirements:
    """
    requirements a worker has to match in order to be able to pick this task

    logic is that workers must fit into minimums, but up to pref (preferred) amount of resources will be actually taken
    for example, you might want minimum of 1 CPU core, but would prefer to use 4
    then a 1 core machine may pick up one task, but 16 core machine will pick just 4 tasks
    """
    def __init__(self, *,
                 groups: Optional[Iterable[str]] = None,
                 worker_type: WorkerType = WorkerType.STANDARD,
                 **resources):
        """
        each arg in **resources must start with "min_<name>" or "pref_<name>",
        where <name> is the name of the resource
        """
        self.__groups = set(groups) if groups is not None else set()
        self.__worker_type = worker_type

        self.__res_req: Requirements = Requirements()  # Dict[str, ResourceRequirement] = {}
        for arg_name, arg_val in resources.items():
            if '_' not in arg_name:
                raise RuntimeError(f'provided resource {arg_name} must start with either "min_", or "pref_"')
            bound, res = arg_name.split(arg_name, 1)

            if res not in self.__res_req.resources:
                self.__res_req.resources[res] = ResourceRequirement()
            if bound == 'min':
                self.__res_req.resources[res].min = arg_val
            elif bound == 'pref':
                self.__res_req.resources[res].pref = arg_val
            else:
                raise RuntimeError(f'provided resource {arg_name} must start with either "min_", or "pref_"')

    # querries

    def groups(self) -> Set[str]:
        return set(self.__groups)

    def worker_type(self) -> WorkerType:
        return self.__worker_type

    def min_resource(self, resource_name: str) -> Union[float, int]:
        if resource_name not in self.__res_req.resources:
            return 0
        return self.__res_req.resources[resource_name].min

    def preferred_resource(self, resource_name: str) -> Union[float, int]:
        if resource_name not in self.__res_req.resources:
            return 0
        return self.__res_req.resources[resource_name].pref

    # setters

    def set_groups(self, groups):
        self.__groups = groups

    def add_groups(self, groups):
        self.__groups.update(set(x for x in groups if x != ''))

    def add_group(self, group: str):
        self.__groups.add(group)

    def set_min_resource(self, resource_name: str, value: Union[float, int]):
        if resource_name not in self.__res_req.resources:
            self.__res_req.resources[resource_name] = ResourceRequirement()
        self.__res_req.resources[resource_name].min = value

    def set_preferred_resource(self, resource_name: str, value: Union[float, int]):
        if resource_name not in self.__res_req.resources:
            self.__res_req.resources[resource_name] = ResourceRequirement()
        self.__res_req.resources[resource_name].pref = value

    def set_worker_type(self, worker_type: WorkerType):
        self.__worker_type = worker_type

    def final_where_clause(self):
        conds = [f'("worker_type" = {self.__worker_type.value})']
        for res_name, res_req in self.__res_req.resources.items():
            conds.append(f'("{res_name}" >= {res_req.min - 1e-8 if isinstance(res_req.min, float) else res_req.min})')  # to ensure sql compare will work
        if len(self.__groups) > 0:
            esc = '\\'

            def _morph(s: str):
                return re.sub(r'(?<!\\)\?', '_', re.sub(r'(?<!\\)\*', '%', s.replace('%', r'\%').replace('_', r'\_')))
            conds.append(f'''(EXISTS (SELECT * FROM worker_groups wg WHERE wg."worker_hwid" == workers."hwid" AND ( {" OR ".join(f"""wg."group" LIKE '{_morph(x)}' ESCAPE '{esc}'""" for x in self.__groups)} )))''')
        return ' AND '.join(conds)

    def pack_selection_info(self) -> str:
        """
        provides a string with enough info to select worker and update worker resource
        this is to be stored in SQLite db
        """
        return ':::'.join((self.final_where_clause(), self.__res_req.serialize_to_string()))

    @classmethod
    def unpack_selection_info(cls, packed_string: str) -> Tuple[str, Requirements]:
        """
        returns sql clause  and  requirements
        reverse of what pack_selection_info() does
        """
        splitpos = packed_string.rfind(':::')
        if splitpos < 0:
            raise ValueError(f'malformed argument string: {repr(packed_string)}')
        requirements_clause = Requirements.deserialize_from_string(packed_string[splitpos + 3:])
        requirements_clause_sql = packed_string[:splitpos]
        return requirements_clause_sql, requirements_clause

class InvocationJob:
    """
    serializable data about launching something
    """
    def __init__(self, args: List[str], *, env: Optional[InvocationEnvironment] = None, invocation_id=None,
                 requirements: Optional[InvocationRequirements] = None,
                 # environment_wrapper_arguments: Optional[EnvironmentResolverArguments] = None,
                 good_exitcodes: Optional[Iterable[int]] = None,
                 retry_exitcodes: Optional[Iterable[int]] = None):
        """

        :param args: list of args passed to exec
        :param env: extra special environment variables to be set. in most cases you don't need that
        :param invocation_id: invocation ID in scheduler's DB this invocation is connected to
        :param requirements: requirements for the worker to satisfy in order to be able to pick up this invocation job
        # :param environment_wrapper_arguments: environment wrapper and arguments to pass to it. if None - worker's configured default wrapper will be invoked
        :param good_exitcodes: set of process exit codes to consider as success. default would be just 0
        :param retry_exitcodes: set of process exit codes to consider as retry is needed. for ex in case of sidefx products exit code 3 means there was a license error
                                which can occur due to delay in license return or network problems or bad license management.
                                in that case you might just want to restart the invocation job automatically. there are no codes in this set by default.
        """
        self.__args = [str(arg) for arg in args]
        self.__env = env or InvocationEnvironment()
        self.__invocation_id = invocation_id
        self.__task_id = None

        self.__out_progress_regex = re.compile(rb'ALF_PROGRESS\s+(\d+)%')
        self.__err_progress_regex = None

        self.__requirements = requirements or InvocationRequirements()
        self.__priority = 0.0
        self.__envres_args = None  # environment_wrapper_arguments

        self.__exitcode = None
        self.__running_time = None
        self.__good_exitcodes = set(good_exitcodes or [0])
        self.__retry_exitcodes = set(retry_exitcodes or [])

        self.__attrs = {}
        self.__extra_files: Dict[str, Union[str, bytes]] = {}

    def requirements(self) -> InvocationRequirements:
        return self.__requirements

    def set_requirements(self, requirements: InvocationRequirements):
        self.__requirements = requirements

    def priority(self) -> float:
        return self.__priority

    def set_priority(self, priority: float):
        self.__priority = priority

    def environment_resolver_arguments(self):  # type: () -> Optional[EnvironmentResolverArguments]
        return self.__envres_args

    def set_stdout_progress_regex(self, regex: Optional[bytes]):
        """
        regex logic is the following:
            - if it has one group - that is treated as progress percentage
            - if it has 2 or more unnamed groups - those are treated as <group1> out of <group2> progress
              so output = g1/g2*100 %
            - if there are named groups named 'current' and 'total' - those groups are treated the same way
              as above: = current/total*100 %
        """
        if regex is None:
            self.__out_progress_regex = None
            return
        self.__out_progress_regex = re.compile(regex)

    def set_stderr_progress_regex(self, regex: Optional[bytes]):
        """
        regex logic is the following:
            - if it has one group - that is treated as progress percentage
            - if it has 2 or more unnamed groups - those are treated as <group1> out of <group2> progress
              so output = g1/g2*100 %
            - if there are named groups named 'current' and 'total' - those groups are treated the same way
              as above: = current/total*100 %
        """
        if regex is None:
            self.__err_progress_regex = None
            return
        self.__err_progress_regex = re.compile(regex)

    def set_extra_file(self, file_path: str, file_data: Union[str, bytes]):
        """
        extra files to transfer with this task
        these files can be referenced in args list in qt resources format:
        like:
        :/path/to/file.smth
        if such file does not exist - argument will be left as is

        WARNING:  slashes / are used to split dirs, OS/FS INDEPENDENT
        WARNING:  other than that it is YOUR responsibility to ensure file names do not contain illegal characters

        :param file_path: file path, like file.smth or path/to/file.smth NOTE: NO leading slashes
        :param file_data: data. str data will be saved as utf-8 text, binary data will be saved as is
        """
        self.__extra_files[file_path] = file_data

    def match_stdout_progress(self, line: bytes) -> Optional[float]:
        if self.__out_progress_regex is None:
            return None
        try:
            return self._match_progress(line, self.__out_progress_regex)
        except BadProgressRegexp:
            self.__out_progress_regex = None
            return None

    def match_stderr_progress(self, line: bytes) -> Optional[float]:
        if self.__err_progress_regex is None:
            return None
        try:
            return self._match_progress(line, self.__err_progress_regex)
        except BadProgressRegexp:
            self.__err_progress_regex = None
            return None

    @classmethod
    def _match_progress(cls, line: bytes, regex: re.Pattern) -> Optional[float]:
        """
        should be given a bytes line as produced by running task output
        will return progress percentage (so 0-100) float, if output line matches progress regex
        otherwise returns None

        regex logic is the following:
            - if it has one group - that is treated as progress percentage
            - if it has 2 or more unnamed groups - those are treated as <group1> out of <group2> progress
              so output = g1/g2*100 %
            - if there are named groups named 'current' and 'total' - those groups are treated the same way
              as above: = current/total*100 %
        """
        match = regex.match(line)
        if match is None:
            return
        if len(match.groups()) == 0:
            raise BadProgressRegexp()
        # several cases possible
        if len(match.groups()) == 1:  # just single group match - treated as progress percentage
            return float(match.group(1))
        named_groups = match.groupdict()
        if len(named_groups) == 0:  # no named groups - first 2 groups are treated as <g1> out of <g2>
            return float(match.group(1)) / float(match.group(2)) * 100.0
        if 'current' in named_groups and 'total' in named_groups:
            return float(match.group('current')) / float(match.group('total')) * 100.0

    def attributes(self):
        return MappingProxyType(self.__attrs)

    def extra_files(self):
        return MappingProxyType(self.__extra_files)

    def args(self) -> List[str]:
        return self.__args

    def env(self):
        return self.__env

    def invocation_id(self) -> int:
        return self.__invocation_id

    def task_id(self) -> int:
        return self.__task_id

    def finish(self, exitcode: Optional[int], running_time: float):
        self.__exitcode = exitcode
        self.__running_time = running_time

    def is_finished(self):
        return self.__exitcode is not None

    def exit_code(self):
        return self.__exitcode

    def running_time(self) -> Optional[float]:
        """
        return time of the invocation run time,
        or None if invocation was not finished
        """
        return self.__running_time

    def finished_with_error(self):
        if self.__exitcode is None:
            raise InvocationNotFinished()
        return self.__exitcode not in self.__good_exitcodes

    def finished_needs_retry(self):
        if self.__exitcode is None:
            raise InvocationNotFinished()
        return self.__exitcode in self.__retry_exitcodes

    async def serialize_async(self) -> bytes:
        return await asyncio.get_event_loop().run_in_executor(None, pickle.dumps, self)

    #
    # methods for scheduler
    def _set_invocation_id(self, invocation_id):
        self.__invocation_id = invocation_id

    def _set_task_id(self, task_id):
        self.__task_id = task_id

    def _set_task_attributes(self, attr_dict):
        self.__attrs = deepcopy(attr_dict)

    def _set_envresolver_arguments(self, args: "EnvironmentResolverArguments"):
        self.__envres_args = args

    def __repr__(self):
        # TODO: be careful with displaying process env until we design a way to deal with secrets
        return f'<InvocationJob: {self.__invocation_id}, {repr(self.__args)}>'

    @classmethod
    def deserialize(cls, data: bytes) -> "InvocationJob":
        return pickle.loads(data)

    @classmethod
    async def deserialize_async(cls, data: bytes) -> "InvocationJob":
        return await asyncio.get_event_loop().run_in_executor(None, cls.deserialize, data)
