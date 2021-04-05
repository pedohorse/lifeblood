import sys
from pathlib import Path
from typing import Optional

org = 'xxx'


def config_path(config_name: str, subname: Optional[str] = None) -> Path:
    base = Path.home()
    if subname is None:
        subname = 'common'
    if sys.platform.startswith('linux'):
        return base/'.local'/'share'/org/subname/config_name
    if sys.platform.startswith('win'):
        return base/'AppData'/'Roaming'/org/subname/config_name
    elif sys.platform.startswith('darwin'):
        return base/'Library'/'Application Support'/org/subname/config_name


def default_main_database_location() -> Path:
    return config_path('main.db', 'scheduler')
