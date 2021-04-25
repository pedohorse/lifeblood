import os
import re
from copy import copy
import asyncio
import pickle

from typing import Optional, Iterable


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
        treat key as path list and prepemd to the list
        """
        if key not in self:
            self[key] = value
            return
        if not isinstance(value, str):
            value = str(value)
        self[key] = os.pathsep.join((self[key], self.expand(value)))

    def append(self, key: str, value):
        """
        treat key as path list and appemd to the list
        """
        if key not in self:
            self[key] = value
            return
        if not isinstance(value, str):
            value = str(value)
        self[key] = os.pathsep.join((self.expand(value), self[key]))


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


class InvocationJob:
    """
    serializable data about launching something
    """
    def __init__(self, args: list, env: Optional[InvocationEnvironment] = None, invocation_id=None,
                 good_exitcodes: Optional[Iterable[int]] = None,
                 retry_exitcodes: Optional[Iterable[int]] = None):
        self.__args = args
        self.__env = env or InvocationEnvironment()
        self.__invocation_id = invocation_id
        # TODO: add here also all kind of resource requirements information
        self.__out_progress_regex = re.compile(rb'ALF_PROGRESS\s+(\d+)%')
        self.__err_progress_regex = None

        self.__exitcode = None
        self.__good_exitcodes = set(good_exitcodes or [0])
        self.__retry_exitcodes = set(retry_exitcodes or [])

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

    def args(self):
        return self.__args

    def env(self):
        return self.__env

    def invocation_id(self):
        return self.__invocation_id

    def finish(self, exitcode: int):
        self.__exitcode = exitcode

    def is_finished(self):
        return self.__exitcode is not None

    def exit_code(self):
        return self.__exitcode

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

    def set_invocation_id(self, invocation_id):
        self.__invocation_id = invocation_id

    def __repr__(self):
        return 'InvocationJob: %d, %s %s' % (self.__invocation_id, repr(self.__args), repr(self.__env))

    @classmethod
    def deserialize(cls, data: bytes) -> "InvocationJob":
        return pickle.loads(data)
