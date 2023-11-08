from typing import Any, Callable, Optional


class BetterOrderedDict:
    def __init__(self):
        self.__dict = {}
        self.__key_order = []

    def __getitem__(self, item):
        return self.__dict[item]

    def __setitem__(self, key, value):
        if key not in self.__dict:
            self.__key_order.append(key)
        self.__dict[key] = value

    def __contains__(self, item):
        return item in self.__dict

    def __len__(self):
        return len(self.__key_order)

    def setdefault(self, key, default):
        if key not in self.__dict:
            self.__dict[key] = default
            self.__key_order.append(key)
        return self.__dict[key]

    def get(self, key, default):
        return self.__dict.get(key, default)

    def items(self):
        for key in self.__key_order:
            yield key, self.__dict[key]

    def insert_at(self, key, value, i):
        if key in self.__dict:
            raise KeyError(key)
        self.__dict[key] = value
        self.__key_order.insert(i, key)

    def index(self, key, *, key_func: Optional[Callable[[Any], Any]]):
        if key_func is None:
            return self.__key_order.index(key)
        for i, k in enumerate(self.__key_order):
            if key_func(k) == key:
                return i
        raise ValueError(key)

    def insert_or_reorder_after_key(self, key, value, after_key):
        need_remove = key in self.__dict
        if after_key not in self.__dict:
            raise KeyError(after_key)

        self.__dict[key] = value
        if need_remove:
            self.__key_order.remove(key)
        i = self.__key_order.index(after_key)
        self.__key_order.insert(i + 1, key)
