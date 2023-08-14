import re
from typing import Tuple, Iterable, Union


class AddressChain(str):
    _validity_re = re.compile(r'^[^|]+(?:\|[^|]+)*$|^$')  # empty string is a valid invalid address

    def __new__(cls, source_str):
        if not cls._validity_re.match(source_str):
            raise ValueError(f'"{source_str}" is not a valid {cls.__name__}')
        return str.__new__(cls, source_str)

    def split_address(self) -> Tuple["DirectAddress", ...]:  # TODO: move this to special module
        parts = self.strip().split('|')

        return tuple(DirectAddress(part) for part in parts)

    @classmethod
    def join_address(cls, address: Iterable["AddressChain"]) -> "AddressChain":
        # return '|'.join(':'.join(str(x) for x in part) for part in address)
        return cls('|'.join(address))

    @classmethod
    def from_host_port(cls, host: str, port: int):
        return cls(f'{host}:{port}')


class DirectAddress(AddressChain):
    _validity_re = re.compile(r'^[^|]+$|^$')  # empty string is a valid invalid address


AddressAny = Union[DirectAddress, AddressChain]


