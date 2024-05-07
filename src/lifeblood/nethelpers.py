import socket
import psutil
import asyncio
import time

from .logging import get_logger

from typing import Any, AnyStr, List, Optional


class BaseFeeder:
    def __init__(self, writer: asyncio.StreamWriter):
        self._writer = writer
        self._entered = False

    async def __aenter__(self):
        if self._entered:
            raise RuntimeError('nested withs are not supported')
        self._entered = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._writer.drain()

    def feed(self, line: AnyStr):
        raise NotImplementedError()


class BaseDrainer:
    def __init__(self, reader: asyncio.StreamReader):  # TODO: add timeouts
        self._reader = reader
        self._iterating = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    async def __aiter__(self):
        if self._iterating:
            raise RuntimeError('nested iterations are not supported!')
        self._iterating = True
        return self

    async def __anext__(self):
        raise NotImplementedError()


class LineFeeder(BaseFeeder):
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._writer.write(b'\00')
        await self._writer.drain()

    def feed(self, line: AnyStr):
        if not self._entered:
            raise RuntimeError('not inside with block!')
        if isinstance(line, str):
            line = line.encode('UTF-8')
        if line[-1] != b'\n':
            line += b'\n'
        self._writer.write(line)


class LineDrainer(BaseDrainer):
    async def __anext__(self):
        go = await self._reader.readexactly(1)
        if go == b'\x00':
            raise StopIteration
        return await self._reader.readline()


def get_default_addr():
    # thank you https://stackoverflow.com/questions/166506/finding-local-ip-addresses-using-pythons-stdlib
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('224.224.224.224', 1))
        myip = s.getsockname()[0]
    except Exception:
        myip = get_localhost()
    finally:
        s.close()
    return myip


def get_localhost():
    return '127.0.0.1'


def get_hostname() -> str:
    return socket.gethostname()


def get_default_broadcast_addr() -> str:
    return get_broadcast_addr_for(addr=get_default_addr(), try_fallbacks=True)


def get_broadcast_addr_for(addr: str, *, try_fallbacks=False) -> Optional[str]:
    net_addrs = psutil.net_if_addrs()
    potential_mask = None
    for iface, ifdatalist in net_addrs.items():
        for ifdata in ifdatalist:
            if ifdata.family != socket.AF_INET:
                continue
            if ifdata.address == addr:
                if ifdata.broadcast is not None:
                    return ifdata.broadcast
                potential_mask = ifdata.netmask
    # ok, no proper broadcast - we can still try inverted mask
    if not try_fallbacks:
        return None
    if potential_mask:
        return '.'.join(str(x) for x in (~int(x) & 255 | int(y) for x, y in zip(potential_mask.split('.'), addr.split('.'))))
    # if all fails
    get_logger('NETWORK').warning('could not detect a proper broadcast address, trying general 255.255.255.255')
    return '<broadcast>'


def all_interfaces(active_only: bool = True) -> List[str]:
    """
    by convention, localhost is the first in returned list
    """
    addrs = []
    net_addrs = psutil.net_if_addrs()
    net_stats = psutil.net_if_stats()
    loopback_address = None  # store it separately to insert later into the list
    for iface, ifdatalist in net_addrs.items():
        if iface not in net_stats:  # probably impossible, just for sanity
            continue
        stats = net_stats[iface]
        if active_only and not stats.isup:  # skip disabled interfaces
            continue
        for ifdata in ifdatalist:
            if ifdata.family != socket.AF_INET:
                continue
            # special case for loopback that we add in later
            if 'loopback' in stats.flags.split(','):
                if loopback_address is None or loopback_address != '127.0.0.1':  # if SEVERAL loopbacks found - we prefer 127.0.0.1 if that exists
                    loopback_address = ifdata.address
                    continue  # do NOT add it to addrs list yet
            addrs.append(ifdata.address)
    # now insert found loopback in the front
    if loopback_address is not None:
        addrs.insert(0, loopback_address)
    return addrs


def get_addr_to(ip):
    if ':' in ip:
        ip = ip.split(':', 1)[0]
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((ip, 1))
        myip = s.getsockname()[0]
    except Exception:
        myip = '127.0.0.1'
    finally:
        s.close()
    return myip


def recv_exactly(sock: socket.socket, numbytes) -> bytes:
    patches = []
    got_bytes = 0
    while got_bytes != numbytes:
        patches.append(sock.recv(numbytes - got_bytes))
        got_bytes += len(patches[-1])
        if len(patches[-1]) == 0:
            raise ConnectionResetError()

    if len(patches) == 1:
        return patches[0]
    elif len(patches) == 0:
        return b''
    return b''.join(patches)


def address_to_ip_port(addr_str: str) -> (str, int):
    if addr_str.count(':') != 1:
        raise ValueError('bad address format')
    addr, sport = addr_str.split(':')
    return addr, int(sport)


class TimeCachedData:
    class CacheInvalid(Exception):
        pass

    def __init__(self, data: Any, valid_period=1, auto_recache_function=None):
        self.__creation_time = time.time()
        self.__valid_period = valid_period
        self.__expiration_time = self.__creation_time + valid_period
        self.__data = data
        self.__recacher = auto_recache_function

    def is_cache_valid(self):
        """
        note that checking is_cache_valid just before getting cached_data does NOT guarantee a no throw

        :return:
        """
        return time.time() < self.__creation_time

    def cached_data(self):
        if not self.is_cache_valid():
            if self.__recacher is None:
                raise TimeCachedData.CacheInvalid()
            else:
                self.__data = self.__recacher()
                self.__creation_time = time.time()
                self.__expiration_time = self.__creation_time + self.__valid_period
        return self.__data

