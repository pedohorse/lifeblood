"""
environment wrapper is an object that produces runtime environment for an invocation
taking into account invocation's requirements and worker's machine specifics

For example you might want to at least pass the version of software you want to use together with
your invocation job

Or a more complicated environment wrapper would take in a whole set of required packages from invocation job
and produce an environment fitting those requirements

As I see the it, a freelancer or a studio would implement one specific to them environment wrapper
for all workers, not several different wrappers
"""
import asyncio
import os
import json
from types import MappingProxyType
from . import invocationjob

from typing import Dict, Mapping


_wrappers: Dict[str, "BaseEnvironmentWrapper"] = {}  # this should be loaded from plugins


def _populate_wrappers():
    for k, v in dict(globals()).items():
        if type(v) != type or v.__module__ != __name__:
            continue
        _wrappers[k] = v


def get_wrapper(name: str) -> "BaseEnvironmentWrapper":
    return _wrappers[name]


class EnvironmentWrapperArguments:
    """
    this class objects specity requirements a task/invocation have for int's worker environment wrapper.
    """
    def __init__(self, wrapper_name=None, **arguments):
        """

        :param wrapper_name: if None - treat as no arguments at all
        :param arguments:
        """
        if wrapper_name is None and len(arguments) > 0:
            raise ValueError('if name is None - no arguments are allowed')
        self.__wrapper_name = wrapper_name
        self.__args = arguments

    def name(self):
        return self.__wrapper_name

    def arguiments(self):
        return MappingProxyType(self.__args)

    def get_wrapper(self):
        return get_wrapper(self.__wrapper_name)

    def get_environment(self) -> "invocationjob.Environment":
        return get_wrapper(self.name()).get_environment(self.arguiments())

    def serialize(self) -> bytes:
        return json.dumps(self.__dict__).encode('utf-8')

    async def serialize_async(self):
        return await asyncio.get_running_loop().run_in_executor(None, self.serialize)

    @classmethod
    def deserialize(cls, data: bytes):
        wrp = EnvironmentWrapperArguments(None)
        wrp.__dict__.update(json.loads(data.decode('utf-8')))
        return wrp

    @classmethod
    async def deserialize_async(cls, data: bytes):
        return await asyncio.get_running_loop().run_in_executor(None, cls.deserialize, data)


class BaseEnvironmentWrapper:
    def get_environment(self, arguments: Mapping) -> "invocationjob.Environment":
        """
        this is the main reason for environment wrapper's existance.
        give it your specific arguments

        :param additional_env:
        :param arguments:
        :return:
        """
        raise NotImplementedError()


class TrivialEnvironmentWrapper(BaseEnvironmentWrapper):
    """
    trivial environment wrapper does nothing
    """
    def get_environment(self, arguments: dict) -> "invocationjob.Environment":
        env = invocationjob.Environment(os.environ)
        return env


class StandardEnvironmentWrapper(BaseEnvironmentWrapper):
    pass


_populate_wrappers()
