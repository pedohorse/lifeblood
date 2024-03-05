import time

from typing import Any, Dict, Iterable


class ExpiringSet:
    def __init__(self):
        # TODO: this can be sped up by keeping a sorted list of expirations
        self.__values: Dict[Any, float] = {}

    def add(self, item: Any, lifetime: float):
        self.__values[item] = time.monotonic() + lifetime

    def prune(self):
        now = time.monotonic()
        for key, val in list(self.__values.items()):
            if val < now:
                self.__values.pop(key)

    def __contains__(self, item) -> bool:
        return self.contains(item)

    def contains(self, item, *, prune=True) -> bool:
        if prune:
            self.prune()
        return item in self.__values

    def remove(self, item):
        if item in self.__values:
            self.__values.pop(item)

    def items(self) -> Iterable[Any]:
        return self.__values.keys()

