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
import inspect
from semantic_version import Version, SimpleSpec
from types import MappingProxyType
from . import invocationjob, paths, logging
from .config import get_config
from .toml_coders import TomlFlatConfigEncoder

from typing import Dict, Mapping, Optional, Type


_resolvers: Dict[str, Type["BaseEnvironmentResolver"]] = {}  # this should be loaded from plugins


def _populate_resolvers():
    for k, v in dict(globals()).items():
        if not inspect.isclass(v) \
                or not issubclass(v, BaseEnvironmentResolver) \
                or v == BaseEnvironmentResolver \
                or v.__module__ != __name__:
            continue
        _resolvers[k] = v
    logging.get_logger('environment_resolver_registry').info('resolvers found:\n' + '\n'.join(f'\t{k}' for k in _resolvers))


def get_resolver(name: str) -> "BaseEnvironmentResolver":
    return _resolvers[name]()


class ResolutionImpossibleError(RuntimeError):
    pass


class EnvironmentResolverArguments:
    """
    this class objects specity requirements a task/invocation have for int's worker environment wrapper.
    """
    def __init__(self, resolver_name=None, arguments: Optional[Mapping] = None):
        """

        :param resolver_name: if None - treat as no arguments at all
        :param arguments:
        """
        if arguments is None:
            arguments = {}
        if resolver_name is None and len(arguments) > 0:
            raise ValueError('if name is None - no arguments are allowed')
        self.__resolver_name = resolver_name
        self.__args = arguments

    def name(self):
        return self.__resolver_name

    def set_name(self, name: str):
        self.__resolver_name = name

    def arguments(self):
        return MappingProxyType(self.__args)

    def add_argument(self, name: str, value):
        self.__args[name] = value

    def remove_argument(self, name: str):
        del self.__args[name]

    def get_resolver(self):
        return get_resolver(self.__resolver_name)

    def get_environment(self) -> "invocationjob.Environment":
        return get_resolver(self.name()).get_environment(self.arguments())

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

    def __init__(self):
        # attempt to locate config and autocreate if not found
        config = get_config('standard_environment_resolver')
        if len(config.loaded_files()) == 0:
            config.set_toml_encoder_generator(TomlFlatConfigEncoder)
            logger = logging.get_logger('environment resolver')
            logger.info('standard environment resolver is used, but no configuration found. auto generating configuration...')
            packages = self.autodetect_software()
            for pkgname, v in packages.items():
                for verstr in v.keys():
                    logger.info(f'found {pkgname} : {verstr}')
            config.set_option_noasync('packages', packages)
            logger.info(f'autogenerated config saved to {config.writeable_file()}')

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
            if not package.startswith('package.'):  # all package reqs start with package.
                continue
            package = package[len('package.'):]
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
        if 'user' in arguments:
            for uservar in ('USER', 'LOGNAME', 'USERNAME'):
                env[uservar] = arguments['user']
        return env

    @classmethod
    def autodetect_software(cls, base_path: Optional[str] = None) -> dict:
        import pathlib
        import sys
        import re
        """
        scans common install locations, tries to detect some software
        (currently only works on houdini...)

        :return:
        """

        base = pathlib.Path(base_path) if base_path is not None else None

        if sys.platform.startswith('linux'):
            base = base or pathlib.Path(r'/opt')
            hfs_prefix = pathlib.Path('')
        elif sys.platform.startswith('win'):
            base = base or pathlib.Path(r'C:\Program Files\Side Effects Software')
            hfs_prefix = pathlib.Path('')
        elif sys.platform.startswith('darwin'):
            base = base or pathlib.Path(r'/Applications/Houdini')
            hfs_prefix = pathlib.Path('Frameworks/Houdini.framework/Versions/Current/Resources')
        else:
            raise RuntimeError(f'unknown platform {sys.platform}')

        packages = {}

        houre = re.compile(r'^(?:[Hh]oudini|hfs)\s*(\d+\.\d+\.\d+)(?:\.py(\d+))?$')
        pyre = re.compile(r'^python(\d+)\.(\d+).*$')
        if base.exists():
            for houdir in base.iterdir():
                if not houdir.exists() or not houdir.is_dir():
                    continue
                match = houre.match(str(houdir.name))
                if not match:
                    continue
                hpy = match.group(2)
                if hpy is None:  # so we don't see explicit python version
                    for file in (houdir/hfs_prefix/'python'/'bin').iterdir():
                        pymatch = pyre.match(str(file.name))
                        if not pymatch:
                            continue
                        hpy = pymatch.group(1)
                        break
                if f'houdini.py{hpy}' not in packages:
                    packages[f'houdini.py{hpy}'] = {}
                packages[f'houdini.py{hpy}'][match.group(1)] = {
                    'label': f'SideFX Houdini, with python version {hpy}',
                    'env': {
                        'PATH': {
                            'prepend': str(houdir/hfs_prefix/'bin')
                        }
                    }
                }
        return packages


_populate_resolvers()


def main(args):
    """
    autodetect things for standard environment resolver
    """
    import argparse
    parser = argparse.ArgumentParser('environment_resolver', description='generates standard environment resolver config\n'
                                                                         'currently only Houdini is supported')
    subparser = parser.add_subparsers(title='command', required=True, dest='command')

    cmdparser = subparser.add_parser('generate', description='generate configuration file in user dir')
    cmdparser.add_argument('--basepath', '-p', help='optional comma (,) separated base path list to search software in')
    cmdparser.add_argument('--output', '-o', help='save config to this file instead of default config location')

    schparser = subparser.add_parser('scan', description='scan for software but do NOT generate config files')
    schparser.add_argument('--basepath', '-p', help='optional comma (,) separated base path list to search software in')

    opts = parser.parse_args(args)

    packages = StandardEnvironmentResolver.autodetect_software()
    if opts.basepath:
        for basepath in opts.basepath.split(','):
            packages.update(StandardEnvironmentResolver.autodetect_software(basepath))

    if opts.command == 'generate':
        config = get_config('standard_environment_resolver')
        if opts.output:
            config.override_config_save_location(opts.output)
        config.set_toml_encoder_generator(TomlFlatConfigEncoder)
        logger = logging.get_logger('environment resolver')
        logger.info('standard environment resolver is used, but no configuration found. auto generating configuration...')
        for pkgname, v in packages.items():
            for verstr in v.keys():
                logger.info(f'found {pkgname} : {verstr}')
        config.set_option_noasync('packages', packages)
        logger.info(f'autogenerated config saved to {config.writeable_file()}')
    elif opts.command == 'scan':
        for pkgname, stuff in packages.items():
            print(f'{pkgname}:')
            for ver, meta in stuff.items():
                print(f'\t{ver}' + (f' ({meta.get("label", "")})' if 'label' in meta else ''))
                print('\n'.join(f'\t\t{k}: {v}' for k,v in meta.items() if k != 'label'))


if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv[1:]) or 0)
