import os
from pathlib import Path
import copy
import toml
import asyncio
import tempfile
from threading import Lock
from . import paths
from .logging import get_logger

from typing import Any, List, Tuple, Union, Callable


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

        self.__stuff = {}
        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    self.__stuff = toml.load(f)
            except Exception as e:
                self.__logger.error(f'failed to load primary config file {config_path}, skipping')
            else:
                self.__sources.append(config_path)

        if configd_path.exists() and configd_path.is_dir():
            for subconfig_path in configd_path.iterdir():
                try:
                    with subconfig_path.open('r') as f:
                        self.__update_dicts(self.__stuff, toml.load(f))
                except Exception as e:
                    self.__logger.error(f'failed to load config file {subconfig_path}, skipping')
                else:
                    self.__sources.append(subconfig_path)

        self.__encoder_generator = None
        self.__overrides = {}
        self.set_overrides(overrides)

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

        if self.__writable_config_path not in self.__sources and self.__writable_config_path.exists():
            self.__sources.append(self.__writable_config_path)

        for source in self.__sources:
            with open(source, 'r') as f:
                self.__update_dicts(self.__stuff, toml.load(f))

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

    def get_option_noasync(self, option_name: str, default_val: Any = None) -> Any:
        try:
            return self._get_option_in_overrides(option_name)
        except Config.OverrideNotFound:
            pass
        with self.__conf_lock:  # to prevent config corruption when running in parallel in executor
            names = option_name.split('.')
            clevel = self.__stuff
            for name in names:
                if name not in clevel:
                    return default_val
                clevel = clevel[name]

            return clevel

    def _set_option_noasync_nolock(self, option_name: str, value) -> None:
        names = option_name.split('.')
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
