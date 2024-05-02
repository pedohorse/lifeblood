from .address import DirectAddress, AddressChain
from .exceptions import MessageTransferError
from typing import Iterable, Optional


class RoutingImpossible(MessageTransferError):
    def __init__(self, sources: Iterable[DirectAddress], destination: AddressChain, *, wrapped_exception: Optional[Exception] = None):
        self.sources = list(sources)
        self.destination = destination
        super().__init__(f'failed to find suitable address to reach {self.destination} from {self.sources}', wrapped_exception=wrapped_exception)


class AddressRouter:
    def select_source_for(self, possible_sources: Iterable[DirectAddress], destination: AddressChain) -> DirectAddress:
        raise NotImplementedError()
