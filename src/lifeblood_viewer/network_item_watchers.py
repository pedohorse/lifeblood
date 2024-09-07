from .network_item import NetworkItem

from typing import FrozenSet, Set


class NetworkItemWatcher:
    def item_was_updated(self, item: NetworkItem):
        pass


class WatchableNetworkItem:
    def __init__(self):
        super().__init__()
        self.__task_watchers: Set["NetworkItemWatcher"] = set()

    def item_watchers(self) -> FrozenSet["NetworkItemWatcher"]:
        return frozenset(self.__task_watchers)

    def add_item_watcher(self, watcher: "NetworkItemWatcher"):
        """
        watcher observes the task, therefore all task's metadata must be updated,
        watchers need to be properly notified on update too.
        """
        self.__task_watchers.add(watcher)

    def remove_item_watcher(self, watcher: "NetworkItemWatcher"):
        self.__task_watchers.remove(watcher)

    def has_item_watcher(self, watcher: "NetworkItemWatcher") -> bool:
        return watcher in self.__task_watchers


class WatchableNetworkItemProxy(WatchableNetworkItem, NetworkItemWatcher):
    """
    is NOT being watched itself, but propagates the watch to it's own watchable items
    """
    def item_was_updated(self, item: NetworkItem):
        for watcher in self.item_watchers():
            watcher.item_was_updated(item)
