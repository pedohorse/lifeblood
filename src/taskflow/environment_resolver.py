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
from semantic_version import Version, SimpleSpec
from types import MappingProxyType
from . import invocationjob
from .config import get_config

from typing import Dict, Mapping


_resolvers: Dict[str, "BaseEnvironmentResolver"] = {}  # this should be loaded from plugins


def _populate_resolvers():
    for k, v in dict(globals()).items():
        if type(v) != type or v.__module__ != __name__:
            continue
        _resolvers[k] = v


def get_resolver(name: str) -> "BaseEnvironmentResolver":
    return _resolvers[name]


class ResolutionImpossibleError(RuntimeError):
    pass


class EnvironmentResolverArguments:
    """
    this class objects specity requirements a task/invocation have for int's worker environment wrapper.
    """
    def __init__(self, resolver_name=None, **arguments):
        """

        :param resolver_name: if None - treat as no arguments at all
        :param arguments:
        """
        if resolver_name is None and len(arguments) > 0:
            raise ValueError('if name is None - no arguments are allowed')
        self.__resolver_name = resolver_name
        self.__args = arguments

    def name(self):
        return self.__resolver_name

    def arguiments(self):
        return MappingProxyType(self.__args)

    def get_resolver(self):
        return get_resolver(self.__resolver_name)

    def get_environment(self) -> "invocationjob.Environment":
        return get_resolver(self.name()).get_environment(self.arguiments())

    def serialize(self) -> bytes:
        return json.dumps(self.__dict__).encode('utf-8')

    async def serialize_async(self):
        return await asyncio.get_running_loop().run_in_executor(None, self.serialize)

    @classmethod
    def deserialize(cls, data: bytes):
        wrp = EnvironmentResolverArguments(None)
        wrp.__dict__.update(json.loads(data.decode('utf-8')))
        return wrp

    @classmethod
    async def deserialize_async(cls, data: bytes):
        return await asyncio.get_running_loop().run_in_executor(None, cls.deserialize, data)


class BaseEnvironmentResolver:
    def get_environment(self, arguments: Mapping) -> "invocationjob.Environment":
        """
        this is the main reason for environment wrapper's existance.
        give it your specific arguments

        :param additional_env:
        :param arguments:
        :return:
        """
        raise NotImplementedError()


class TrivialEnvironmentResolver(BaseEnvironmentResolver):
    """
    trivial environment wrapper does nothing
    """
    def get_environment(self, arguments: dict) -> "invocationjob.Environment":
        env = invocationjob.Environment(os.environ)
        return env


class StandardEnvironmentResolver(BaseEnvironmentResolver):
    """
    will initialize environment based on requested software versions and it's own config
    will raise ResolutionImpossibleError if he doesn't know how to resolve given configuration

    example configuration:
    [packages.houdini."18.5.666"]
    env.PATH.prepend=[
        "/path/to/hfs/bin",
        "/some/other/path/dunno"
    ]
    env.PATH.append=[
        "/whatever/you/want/to/append"
    ]
    env.PYTHONPATH.prepend="/dunno/smth"
    """
    def get_environment(self, arguments: Mapping) -> "invocationjob.Environment":
        """

        :param arguments: are expected to be in format of package_name: version_specification
                          like houdini
        :return:
        """
        config = get_config('standard_environment_resolver')
        packages = config.get_option_noasync('packages')
        if packages is None:
            raise ResolutionImpossibleError('no packages are configured')

        available_software = {k: {Version(v): rest for v, rest in packages[k].items()} for k in packages.keys()}

        resolved_versions = {}
        for package, spec_str in arguments.items():
            if package not in available_software:
                raise ResolutionImpossibleError(f'no configurations for package {package} found')
            resolved_versions[package] = SimpleSpec(spec_str).select(available_software[package].keys())
            if resolved_versions[package] is None:
                raise ResolutionImpossibleError(f'could not satisfy version requirements {spec_str} for package {package}')

        env = invocationjob.Environment(os.environ)
        for package, version in sorted(resolved_versions.items(), key=lambda x: available_software[x[0]][x[1]].get('priority', 50)):
            actions = available_software[package][version]
            for env_name, env_action in actions.get('env', {}).items():
                if not isinstance(env_action, Mapping):
                    env_action = {'set': env_action}
                if 'prepend' in env_action:
                    value = env_action['prepend']
                    if isinstance(value, str):
                        value = [value]
                    for part in reversed(value):
                        env.prepend(env_name, part)
                if 'append' in env_action:
                    value = env_action['append']
                    if isinstance(value, str):
                        value = [value]
                    for part in value:
                        env.append(env_name, part)
                if 'set' in env_action:
                    value = env_action['set']
                    if isinstance(value, list):
                        value = os.pathsep.join(value)
                    env[env_name] = value
        return env


_populate_resolvers()
