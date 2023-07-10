from typing import Tuple, Iterable, Union


class AddressChain(str):
    def split_address(self) -> Tuple["DirectAddress", ...]:  # TODO: move this to special module
        parts = self.strip().split('|')

        return tuple(DirectAddress(part) for part in parts)

    @classmethod
    def join_address(cls, address: Iterable["DirectAddress"]) -> "AddressChain":
        # return '|'.join(':'.join(str(x) for x in part) for part in address)
        return cls('|'.join(address))


class DirectAddress(AddressChain):
    pass


AddressAny = Union[DirectAddress, AddressChain]


