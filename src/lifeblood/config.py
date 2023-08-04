import os
from pathlib import Path
import copy
import toml
import asyncio
import tempfile
from threading import Lock
from . import paths
from .logging import get_logger

from typing import Any, List, Tuple, Union, Callable, Set


_conf_cache = {}
_glock = Lock()


def get_config(subname: str) -> "Config":
    global _glock, _conf_cache
    with _glock:
        if subname not in _conf_cache:
            _conf_cache[subname] = Config(subname)
        return _conf_cache[subname]


def set_config_overrides(subname: str, overrides=None):
    """
    convenient method to set config's overrides without actually creating config object explicitly

    :param subname:
    :param overrides:
    :return:
    """
    global _glock, _conf_cache
    with _glock:
        if subname not in _conf_cache:
            _conf_cache[subname] = Config(subname, overrides)
        else:
            _conf_cache[subname].set_overrides(overrides)


def create_default_user_config_file(subname: str, default_config: Union[str, dict], force: bool = False, toml_encoder=None):
    """
    create user configuration file
    useful for initialization, but can be forced to overwrite

    :param subname:
    :param default_config:
    :param force: if true - user config will be overriden even if it exists
    :param toml_encoder: config backend is currently TOML, and i already kinda regret it... so you can pass special encoders here
    :return:
    """
    user_conf_path = paths.config_path('config.toml', subname)
    if os.path.exists(user_conf_path) and not force:
        return
    os.makedirs(os.path.dirname(user_conf_path), exist_ok=True)
    with open(user_conf_path, 'w') as f:
        if isinstance(default_config, str):
            f.write(default_config)
        else:
            toml.dump(default_config, f, encoder=toml_encoder)


def get_local_scratch_path():
    return os.path.join(tempfile.gettempdir(), 'lifeblood', 'shared')


class Config:
    __logger = get_logger('config')

    class OverrideNotFound(RuntimeError):
        pass

    def __init__(self, subname: str, base_name: str = 'config', overrides=None):
        config_path = paths.config_path(f'{base_name}.toml', subname)
        configd_path = paths.config_path(f'{base_name}.d', subname)
        self.__writable_config_path = config_path
        self.__conf_lock = Lock()
        self.__write_file_lock = Lock()

        self.__sources: List["Path"] = []
        self.__broken_sources: Set["Path"] = set()

        self.__config_paths_to_check = [config_path]
        if configd_path.exists() and configd_path.is_dir():
            self.__config_paths_to_check.extend(configd_path.iterdir())

        self.__stuff = {}

        self.__encoder_generator = None
        self.__overrides = {}
        self.set_overrides(overrides)

        self.reload(keep_overrides=True)

    @classmethod
    def __update_dicts(cls, main: dict, secondary: dict):
        for key, value in secondary.items():
            if isinstance(value, dict) and isinstance(main.get(key), dict):
                cls.__update_dicts(main[key], value)
                continue
            main[key] = value

    def reload(self, keep_overrides=True) -> None:
        self.__stuff = {}
        if not keep_overrides:
            self.__overrides = {}

        paths_to_check = self.__config_paths_to_check
        # append writable path only for the reload time
        # we don't want to mix paths added to the list from legal sources and this user overridable one
        if self.__writable_config_path not in paths_to_check:
            paths_to_check.append(self.__writable_config_path)

        for config_path in paths_to_check:
            if not config_path.exists():
                continue
            try:
                with config_path.open('r') as f:
                    self.__update_dicts(self.__stuff, toml.load(f))
            except Exception as e:
                self.__logger.error(f'failed to load config file {config_path}, skipping')
                self.__broken_sources.add(config_path)
            else:
                self.__sources.append(config_path)

    def writeable_file(self) -> "Path":
        """
        Get the path to the file chis config changes will be saved into.
        The file might not yet exist

        :return:
        """
        return self.__writable_config_path

    def override_config_save_location(self, path: Union[Path, str]):
        """
        change default config write location

        :return:
        """
        self.__writable_config_path = Path(path)

    def loaded_files(self) -> Tuple["Path"]:
        """
        list files from which this config was sourced

        :return: tuple of file paths
        """
        return tuple(self.__sources)

    def broken_files(self) -> Set["Path"]:
        """
        returns set of paths that look like configs, but had errors when parsing
        """
        return set(self.__broken_sources)

    def set_overrides(self, overrides: dict) -> None:
        """
        sets overrides to a prepared dictionary of items.
        :param overrides:
        """
        if overrides is not None:
            self.__overrides = copy.deepcopy(overrides)
        else:
            self.__overrides = {}

    def set_override(self, option_name: str, val: Any) -> None:
        """
        set one item override
        :param option_name: option path, like foo.bar.cat.dog
        :param val: any serializable value
        :return:
        """
        names = option_name.split('.')
        clevel = self.__overrides
        for name in names[:-1]:
            if name not in clevel:
                clevel[name] = {}
            clevel = clevel[name]
        clevel[names[-1]] = val

    async def get_option(self, option_name: str, default_val: Any = None) -> Any:
        return await asyncio.get_event_loop().run_in_executor(None, self.get_option_noasync, option_name, default_val)

    def _get_option_in_overrides(self, option_name: str):
        names = option_name.split('.')
        clevel = self.__overrides
        for name in names:
            if name not in clevel:
                raise Config.OverrideNotFound()
            clevel = clevel[name]
        return clevel

    def has_option_noasync(self, option_name):
        class _SomethingStrangeL:
            pass
        return self.get_option_noasync(option_name, default_val=_SomethingStrangeL) is not _SomethingStrangeL

    @staticmethod
    def _split_config_names(option_name: str) -> Tuple[str, ...]:
        names = []
        in_quotes = False
        mark = 0
        word_parts = []
        for i, l in enumerate(option_name):
            if l == '.' and not in_quotes:
                word_parts.append(option_name[mark:i])
                names.append(''.join(word_parts))
                if names[-1] == '':
                    raise ValueError(f'"{option_name}" is not a valid option_name')
                word_parts.clear()
                mark = i + 1
            elif l == '"':
                word_parts.append(option_name[mark:i])
                mark = i + 1
                in_quotes = not in_quotes
        if in_quotes:
            raise ValueError(f'"{option_name}" is not a valid option_name')
        word_parts.append(option_name[mark:])
        names.append(''.join(word_parts))
        if names[-1] == '':
            raise ValueError(f'"{option_name}" is not a valid option_name')
        return tuple(names)


    def get_option_noasync(self, option_name: str, default_val: Any = None) -> Any:
        try:
            return self._get_option_in_overrides(option_name)
        except Config.OverrideNotFound:
            pass
        with self.__conf_lock:  # to prevent config corruption when running in parallel in executor
            names = self._split_config_names(option_name)
            clevel = self.__stuff
            for name in names:
                if name not in clevel:
                    return default_val
                clevel = clevel[name]

            return clevel  # TODO: return IMMUTABLE shit! this can be a list or a dict, and it can be nested too!

    def _set_option_noasync_nolock(self, option_name: str, value) -> None:
        names = self._split_config_names(option_name)
        clevel = self.__stuff
        for name in names:
            last = name == names[-1]
            if name not in clevel and not last:
                clevel[name] = {}
            if last:
                clevel[name] = value
                break
            clevel = clevel[name]
        self.write_config_noasync()

    def set_option_noasync(self, option_name: str, value: Any) -> None:
        with self.__conf_lock:
            self._set_option_noasync_nolock(option_name, value)

    async def set_option(self, option_name: str, value: Any) -> None:
        return await asyncio.get_event_loop().run_in_executor(None, self.set_option_noasync, option_name, value)

    def set_toml_encoder_generator(self, generator: Callable):
        self.__encoder_generator = generator

    def write_config_noasync(self):
        with self.__write_file_lock:
            if not os.path.exists(self.__writable_config_path):
                os.makedirs(os.path.dirname(self.__writable_config_path), exist_ok=True)
            with open(self.__writable_config_path, 'w') as f:
                toml.dump(self.__stuff, f, encoder=self.__encoder_generator() if self.__encoder_generator is not None else None)

    async def write_config(self):
        return await asyncio.get_event_loop().run_in_executor(None, self.write_config_noasync)
