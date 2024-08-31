import os
import sys
from pathlib import Path
from typing import Optional

org = 'xxx'
basename = 'lifeblood'

config_env_var_name = 'LIFEBLOOD_CONFIG_LOCATION'
log_env_var_name = 'LIFEBLOOD_LOG_LOCATION'


def config_path(config_name: str, subname: Optional[str] = None) -> Path:
    return config_unexpanded_path(config_name, subname).expanduser()


def config_unexpanded_path(config_name: str, subname: Optional[str] = None) -> Path:
    """
    returns path to the config_name provided

    :param config_name: name of the file or dir of the config
    :param subname: optional name of the subconfig, OR absolute path to the config
    """
    if subname is None:
        subname = 'common'
    elif os.path.isabs(subname):
        # if subname is abs path - we treat it as base path
        return Path(subname) / config_name

    if config_env_var_name in os.environ:
        return Path(os.environ[config_env_var_name])/subname/config_name
    base = Path.home()
    if '.' in subname:
        subname = Path(*subname.split('.'))
    if sys.platform.startswith('linux'):
        return base/basename/subname/config_name
    if sys.platform.startswith('win'):
        return base/basename/subname/config_name
    elif sys.platform.startswith('darwin'):
        return base/'Library'/'Preferences'/basename/subname/config_name
    else:
        raise NotImplementedError(f'da heck is {sys.platform} anyway??')


def log_path(log_name: Optional[str], subname: Optional[str] = None, ensure_path_exists=True) -> Optional[Path]:
    path = log_unexpanded_path(log_name, subname).expanduser()
    if not ensure_path_exists:
        return path
    if not path.exists():
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch(exist_ok=True)
        except Exception:
            return None
    if not os.access(path, os.W_OK):
        return None
    return path


def log_unexpanded_path(log_name: Optional[str], subname: Optional[str] = None) -> Path:
    if log_env_var_name in os.environ:
        log_base_path = Path(os.environ[log_env_var_name])
    else:
        log_base_path = config_unexpanded_path('', 'logs')
    if subname:
        log_base_path /= subname
    if log_name:
        log_base_path /= log_name
    return log_base_path


def default_main_database_location() -> Path:
    return config_unexpanded_path('main.db', 'scheduler')
