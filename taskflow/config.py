import os
import toml
from . import paths


class Config:
    def __init__(self, subname: str, kwargs=None):
        config_path = paths.config_path('config.toml', subname)
        if not os.path.exists(config_path):
            self.write_default_config(subname, config_path)

        with open(config_path, 'r') as f:
            self.__stuff = toml.load(f)

        if kwargs is not None:
            self.__stuff.update(kwargs)

    def get_option(self, option_name: str, default_val=None):
        return self.__stuff.get(option_name, default_val)

    def has_option(self, option_name: str):
        return option_name in self.__stuff

    def write_default_config(self, subname, path):
        raise NotImplementedError()
