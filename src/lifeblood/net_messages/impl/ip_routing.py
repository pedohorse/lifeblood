import socket
from ..address import DirectAddress, AddressChain
from ..address_routing import AddressRouter, RoutingImpossible

from typing import Iterable


class IPRouter(AddressRouter):
    def __init__(self, use_caching: bool = True):
        # WE ASSUME ROUTING TO BE STATIC
        # So if a case where interfaces/routing may change dynamically come up -
        # then we can think about them, not now
        if use_caching:
            self.__routing_cache = {}
        else:
            self.__routing_cache = None

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

        do_caching = self.__routing_cache is not None
        cache_key = None
        if do_caching:
            possible_sources = tuple(sorted(possible_sources))
            # cache key takes all input arguments into account
            # NOTE: we don't take destination port into account
            cache_key = (possible_sources, dest_ip)
        elif not isinstance(possible_sources, tuple):
            possible_sources = tuple(possible_sources)

        if not do_caching or cache_key not in self.__routing_cache:
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

            if do_caching:
                assert cache_key is not None
                self.__routing_cache[cache_key] = candidates[0]
            else:
                return candidates[0]

        assert do_caching and cache_key is not None
        return self.__routing_cache[cache_key]
