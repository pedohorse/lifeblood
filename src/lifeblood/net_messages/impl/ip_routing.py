import socket
from ..address import DirectAddress, AddressChain
from ..address_routing import AddressRouter, RoutingImpossible

from typing import Iterable


class IPRouter(AddressRouter):
    def select_source_for(self, possible_sources: Iterable[DirectAddress], destination: AddressChain) -> DirectAddress:
        """
        gets interface ipv4 address to reach given address
        """
        # we expect address to be ip:port
        destination0 = destination.split_address()[0]
        if ':' in destination0:
            dest_ip, _ = destination0.split(':', 1)
        else:
            dest_ip = str(destination0)

        # thank you https://stackoverflow.com/questions/166506/finding-local-ip-addresses-using-pythons-stdlib
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # doesn't even have to be reachable
            s.connect((dest_ip, 1))
            myip = s.getsockname()[0]
        except Exception as e:
            raise RoutingImpossible(possible_sources, destination, wrapped_exception=e)
        finally:
            s.close()

        candidates = [
            x
            for x in possible_sources
            if myip == (x.split(':', 1)[0] if ':' in x else x)
        ]
        if len(candidates) == 0:
            raise RoutingImpossible(possible_sources, destination)
        # there may be several candidates, and we may add some more logic to pick one from them in future
        return candidates[0]
