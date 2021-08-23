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
import os
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
    def __init__(self, wrapper_name, **arguments):
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


_populate_wrappers()
