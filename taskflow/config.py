import os
import toml
import asyncio
from threading import Lock
from . import paths

from typing import Any


class Config:
    def __init__(self, subname: str, kwargs=None):
        config_path = paths.config_path('config.toml', subname)
        self.__config_path = config_path
        self.__conf_lock = Lock()
        self.__write_file_lock = Lock()

        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                self.__stuff = toml.load(f)
        else:
            self.__stuff = {}

        if kwargs is not None:
            self.__overrides = kwargs
        else:
            self.__overrides = {}

    async def get_option(self, option_name: str, default_val: Any = None) -> Any:
        return await asyncio.get_event_loop().run_in_executor(None, self.get_option_noasync, option_name, default_val)

    def get_option_noasync(self, option_name: str, default_val: Any = None) -> Any:
        with self.__conf_lock:  # to prevent config corruption when running in parallel in executor
            names = option_name.split('.')
            clevel = self.__stuff
            for name in names:
                if name not in clevel:
                    self._set_option_noasync_nolock(option_name, default_val)
                    assert name in clevel
                clevel = clevel[name]

            return clevel

    def _set_option_noasync_nolock(self, option_name: str, value) -> None:
        names = option_name.split('.')
        clevel = self.__stuff
        for name in names:
            last = name == names[-1]
            if name not in clevel:
                if not last:
                    clevel[name] = {}
                else:
                    clevel[name] = value
        self.write_config()

    def set_option_noasync(self, option_name: str, value: Any) -> None:
        with self.__conf_lock:
            self._set_option_noasync_nolock(option_name, value)

    async def set_option(self, option_name: str, value: Any) -> None:
        return await asyncio.get_event_loop().run_in_executor(None, self.set_option_noasync, option_name, value)

    def write_config(self):
        with self.__write_file_lock:
            with open(self.__config_path, 'w') as f:
                toml.dump(f, self.__stuff)

    async def write_config_async(self):
        return await asyncio.get_event_loop().run_in_executor(None, self.write_config)
