from typing import Tuple, Iterable


def split_address(address: str) -> Tuple[Tuple[str, int], ...]:  # TODO: move this to special module
    parts = address.strip().split('|')

    def _split_part(part: str) -> Tuple[str, int]:
        addr, port = part.split(':', 1)
        return addr, int(port)

    return tuple(tuple(_split_part(part)) for part in parts)


def join_address(address: Iterable[Tuple[str, int]]) -> str:
    return '|'.join(':'.join(str(x) for x in part) for part in address)

