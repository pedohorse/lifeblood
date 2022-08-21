import os
import re
from copy import copy, deepcopy
import asyncio
import pickle
from types import MappingProxyType
from .enums import WorkerType
from .net_classes import WorkerResources

from typing import Optional, Iterable, Union, Dict, List, TYPE_CHECKING
if TYPE_CHECKING:
    from .environment_resolver import EnvironmentResolverArguments


class InvocationNotFinished(RuntimeError):
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

    # def __getattr__(self, item):
    #     return lambda k, v: self._enqueue_kv_method(item, k, v)

    # these 2 guys are explicitly added only for IDE popup hints
    def prepend(self, key: str, value):
        self._enqueue_kv_method('prepend', key, value)

    def append(self, key: str, value):
        self._enqueue_kv_method('append', key, value)


class InvocationRequirements:
    """
    requirements a worker has to match in order to be able to pick this task

    logic is that workers must fit into minimums, but up to pref (preferred) amount of resources will be actually taken
    for example, you might want minimum of 1 CPU core, but would prefer to use 4
    then a 1 core machine may pick up one task, but 16 core machine will pick just 4 tasks
    """
    def __init__(self, *, min_cpu_count: Optional[int] = None, min_memory_bytes: Optional[int] = None, groups: Optional[Iterable[str]] = None, worker_type: WorkerType = WorkerType.STANDARD):
        self.__groups = set(groups) if groups is not None else set()
        self.__worker_type = worker_type
        self.__min_cpu_count = min_cpu_count or 0
        self.__min_memory_bytes = min_memory_bytes or 0
        self.__min_gpu_count = 0
        self.__min_gpu_memory_bytes = 0

        self.__pref_cpu_count = None
        self.__pref_memory_bytes = None
        self.__pref_gpu_count = None
        self.__pref_gpu_memory_bytes = None

    def groups(self):
        return tuple(self.__groups)

    def set_groups(self, groups):
        self.__groups = groups

    def add_groups(self, groups):
        self.__groups.update(set(x for x in groups if x != ''))

    def add_group(self, group: str):
        self.__groups.add(group)

    def set_min_cpu_count(self, min_cpu_count):
        self.__min_cpu_count = min_cpu_count

    def set_min_memory_bytes(self, min_memory_bytes):
        self.__min_memory_bytes = min_memory_bytes

    def set_preferred_cpu_count(self, pref_cpu_count):
        self.__pref_cpu_count = pref_cpu_count

    def set_preferred_memory_bytes(self, pref_memory_bytes):
        self.__pref_memory_bytes = pref_memory_bytes

    def set_worker_type(self, worker_type: WorkerType):
        self.__worker_type = worker_type

    def final_where_clause(self):
        conds = [f'("worker_type" = {self.__worker_type.value})']
        if self.__min_cpu_count > 0:
            conds.append(f'("cpu_count" >= {self.__min_cpu_count - 1e-8 if isinstance(self.__min_cpu_count, float) else self.__min_cpu_count})')   # to ensure sql compare will work
        if self.__min_memory_bytes > 0:
            conds.append(f'("cpu_mem" >= {self.__min_memory_bytes})')
        if len(self.__groups) > 0:
            esc = '\\'

            def _morph(s: str):
                return re.sub(r'(?<!\\)\?', '_', re.sub(r'(?<!\\)\*', '%', s.replace('%', r'\%').replace('_', r'\_')))
            conds.append(f'''(EXISTS (SELECT * FROM worker_groups wg WHERE wg."worker_hwid" == workers."hwid" AND ( {" OR ".join(f"""wg."group" LIKE '{_morph(x)}' ESCAPE '{esc}'""" for x in self.__groups)} )))''')
        return ' AND '.join(conds)

    def to_dict(self, resources_only: bool = False) -> dict:
        ret = {'cpu_count': self.__min_cpu_count,
               'cpu_mem': self.__min_memory_bytes,
               'gpu_count': self.__min_gpu_count,
               'gpu_mem': self.__min_gpu_memory_bytes}
        if self.__pref_cpu_count is not None:
            ret['pref_cpu_count'] = self.__pref_cpu_count
        if self.__pref_memory_bytes is not None:
            ret['pref_cpu_mem'] = self.__pref_memory_bytes
        if self.__pref_gpu_count is not None:
            ret['pref_gpu_count'] = self.__pref_gpu_count
        if self.__pref_gpu_memory_bytes is not None:
            ret['pref_gpu_mem'] = self.__pref_gpu_memory_bytes

        if resources_only:
            return ret

        ret['groups'] = self.groups()
        return ret

    def to_min_worker_resources(self) -> WorkerResources:
        res = WorkerResources()
        res.cpu_count = self.__min_cpu_count
        res.cpu_mem = self.__min_memory_bytes
        res.gpu_count = self.__min_gpu_count
        res.gpu_mem = self.__min_gpu_memory_bytes
        return res

    def __gt__(self, other):
        if not isinstance(other, WorkerResources):
            raise NotImplementedError()
        return self.__min_cpu_count > other.cpu_count and \
               self.__min_memory_bytes > other.cpu_mem and \
               self.__min_gpu_count > other.gpu_count and \
               self.__min_gpu_memory_bytes > other.gpu_mem

    def __eq__(self, other):
        if not isinstance(other, WorkerResources):
            raise NotImplementedError()
        return self.__min_cpu_count == other.cpu_count and \
               self.__min_memory_bytes == other.cpu_mem and \
               self.__min_gpu_count == other.gpu_count and \
               self.__min_gpu_memory_bytes == other.gpu_mem


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
        # TODO: add here also all kind of resource requirements information
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

    def set_stdout_progress_regex(self, regex: Optional[str]):
        if regex is None:
            self.__out_progress_regex = None
            return
        self.__out_progress_regex = re.compile(regex)

    def set_stderr_progress_regex(self, regex: Optional[str]):
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
            return
        match = self.__out_progress_regex.match(line)
        if match is None:
            return
        if len(match.groups()) == 0:
            self.__out_progress_regex = None
            return
        return float(match.group(1))

    def match_stderr_progress(self, line: bytes) -> Optional[float]:
        if self.__err_progress_regex is None:
            return
        match = self.__err_progress_regex.match(line)
        if match is None:
            return
        if len(match.groups()) == 0:
            self.__err_progress_regex = None
            return
        return float(match.group(1))

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

    def finish(self, exitcode: int, running_time: float):
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
        return f'InvocationJob: {self.__invocation_id}, {repr(self.__args)} {repr(self.__env)}'

    @classmethod
    def deserialize(cls, data: bytes) -> "InvocationJob":
        return pickle.loads(data)

    @classmethod
    async def deserialize_async(cls, data: bytes) -> "InvocationJob":
        return await asyncio.get_event_loop().run_in_executor(None, cls.deserialize, data)
