import os
import sys
from pathlib import Path
from typing import Optional

org = 'xxx'
basename = 'lifeblood'

env_var_name = 'LIFEBLOOD_CONFIG_LOCATION'


def config_path(config_name: str, subname: Optional[str] = None) -> Path:
    return config_unexpanded_path(config_name, subname).expanduser()


def config_unexpanded_path(config_name: str, subname: Optional[str] = None) -> Path:
    if env_var_name in os.environ:
        return Path(os.environ[env_var_name])/subname/config_name
    base = Path('~')
    if subname is None:
        subname = 'common'
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
    # if sys.platform.startswith('linux'):
    #     return base/'.local'/'share'/org/subname/config_name
    # if sys.platform.startswith('win'):
    #     return base/'AppData'/'Roaming'/org/subname/config_name
    # elif sys.platform.startswith('darwin'):
    #     return base/'Library'/'Application Support'/org/subname/config_name


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
    if sys.platform.startswith('linux'):
        log_path = Path(os.environ.get("XDG_DATA_HOME", "~/.local/share"))
    elif sys.platform.startswith("win"):
        log_path = Path(os.environ["LOCALAPPDATA"])
    elif sys.platform.startswith("darwin"):
        log_path = Path("~/Library/Application Support")
    else:
        raise NotImplementedError(f'da heck is {sys.platform} anyway??')
    log_path /= basename
    if subname:
        log_path /= subname
    if log_name:
        log_path /= log_name
    return log_path


def default_main_database_location() -> Path:
    return config_unexpanded_path('main.db', 'scheduler')
