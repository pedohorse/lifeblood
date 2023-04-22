from typing import Optional, Set

class TypeMetadata:
    @property
    def label(self) -> Optional[str]:
        raise NotImplementedError()

    @property
    def tags(self) -> Set[str]:
        raise NotImplementedError()