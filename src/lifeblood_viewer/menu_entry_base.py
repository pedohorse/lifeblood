from dataclasses import dataclass

from typing import Callable, Tuple


@dataclass
class MainMenuLocation:
    location: Tuple[str, ...]
    label: str


@dataclass
class MainMenuEntry(MainMenuLocation):
    action: Callable
