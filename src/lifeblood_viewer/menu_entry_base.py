from dataclasses import dataclass

from typing import Callable, Tuple, Union


@dataclass
class MainMenuLocation:
    location: Tuple[str, ...]
    label: Union[str, Callable[[], str]]


@dataclass
class MainMenuEntry(MainMenuLocation):
    action: Callable
