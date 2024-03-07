import time

from typing import Any, Dict, Iterable, Optional


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

    def __len__(self) -> int:
        return self.size()

    def size(self, *, prune=True) -> int:
        if prune:
            self.prune()
        return len(self.__values)

    def contains(self, item, *, prune=True) -> bool:
        if prune:
            self.prune()
        return item in self.__values

    def remove(self, item):
        if item in self.__values:
            self.__values.pop(item)

    def items(self) -> Iterable[Any]:
        return self.__values.keys()


class ExpiringValuesSetMap:
    def __init__(self):
        self.__values: Dict[int, ExpiringSet] = {}

    def __len__(self) -> int:
        return self.size(prune=True)

    def size(self, *, prune=True) -> int:
        if prune:
            self.__prune_all()
        return len(self.__values)

    def prune(self, key: Optional[int] = None):
        if key is None:
            self.__prune_all()
        else:
            self.__prune_single(key)

    def __prune_single(self, key: int):
        if key not in self.__values:
            return
        self.__values[key].prune()
        if self.__values[key].size(prune=False) == 0:
            self.__values.pop(key)

    def __prune_all(self):
        to_remove = []
        for key, val in self.__values.items():
            val.prune()
            if val.size(prune=False) == 0:
                to_remove.append(key)
        for key in to_remove:
            self.__values.pop(key)

    def add_expiring_value(self, key: int, what_to_ban, ban_time: float):
        if key not in self.__values:
            self.__values[key] = ExpiringSet()
        self.__values[key].add(what_to_ban, ban_time)

    def get_values(self, key: int, *, prune=True) -> Iterable[Any]:
        if prune:
            self.prune(key)
        if key not in self.__values:
            return ()
        return self.__values[key].items()
