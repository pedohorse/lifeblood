from typing import Iterable


class NodeTypeMetadata:
    def __init__(self, type_name: str, label: str, tags: Iterable[str]):
        self.type_name = type_name
        self.label = label
        self.tags = set(tags)
