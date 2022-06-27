import asyncio
import socket
from string import ascii_letters
import random
import struct

from . import logging
from .nethelpers import get_localhost
from .defaults import broadcast_port as default_broadcast_port

from . import os_based_cheats

from typing import Tuple, Union, Optional, Callable, Coroutine, Any

Address = Tuple[str, int]

# base taken from https://gist.github.com/yluthu/4f785d4546057b49b56c

_magic = b'=\xe2\x88\x88\xe2\xad\x95\xe2\x88\x8b='


async def create_broadcaster(identifier: str, information, *, broad_port: int = default_broadcast_port(), ip='0.0.0.0', broadcasts_count: Optional[int] = None, broadcast_interval: int = 10):
    loop = asyncio.get_event_loop()
    return await loop.create_datagram_endpoint(
        lambda: BroadcastProtocol(identifier, information, (ip, broad_port), broadcasts_count, broadcast_interval, loop=loop),
        family=socket.AF_INET, reuse_port=True, allow_broadcast=True)


async def await_broadcast(identifier: str, broad_port: int = default_broadcast_port(), listen_address: str = '0.0.0.0') -> str:
    loop = asyncio.get_event_loop()
    protocol: BroadcastReceiveProtocol
    _, protocol = await loop.create_datagram_endpoint(
        lambda: BroadcastReceiveProtocol(identifier, loop=loop),
        local_addr=(listen_address, broad_port), family=socket.AF_INET, reuse_port=True, allow_broadcast=True)
    message = await protocol.till_done()

    # rebroadcast to localhost
    _, protocol = await create_broadcaster(identifier, message, broad_port=broad_port, ip=get_localhost(), broadcasts_count=1)  # TODO: bet with my future self - i will regret hardcoding ipv4 localhost like that
    await protocol.till_done()
    return message


class BroadcastListener:  # TODO: This was never tested
    def __init__(self, identifier: str, callback: Callable[[str], Coroutine], broad_port: int = default_broadcast_port(), listen_address: str = '0.0.0.0'):
        self.__identifier = identifier
        self.__callback = callback
        self.__addr = (listen_address, broad_port)
        self.__proto: Optional[BroadcastReceiveProtocol] = None
        self.__trans = None

    async def start(self):
        loop = asyncio.get_event_loop()
        self.__trans, self.__proto = await loop.create_datagram_endpoint(
            lambda: BroadcastReceiveProtocol(self.__identifier, loop=loop, single_shot=False, callback=self.__callback),
            local_addr=self.__addr, family=socket.AF_INET, reuse_port=True, allow_broadcast=True)

    def stop(self):
        self.__proto.close()

    async def wait_till_stopped(self):
        await self.__proto.till_done()


class BroadcastProtocol(asyncio.DatagramProtocol):
    def __init__(self, identifier: str, information: str, target: Address, count=None, broadcast_interval=10, *, loop: asyncio.AbstractEventLoop = None):
        self.__logger = logging.get_logger('broadcast')
        self.__target = target
        self.__loop = asyncio.get_event_loop() if loop is None else loop
        self.__transport = None
        self.__identifier = identifier
        self.__interval = broadcast_interval
        self.__broadcast_count = count
        self.__information = information
        self.__closed_future = self.__loop.create_future()
        self.__broadcast_task = None
        # TODO: raise on dgram size too big

    def connection_made(self, transport: asyncio.transports.DatagramTransport):
        self.__logger.info(f'started broadcasting')
        self.__transport = transport
        sock = transport.get_extra_info("socket")  # type: socket.socket
        #sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        #sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.__broadcast_task = self.__loop.create_task(self.broadcast())

    def datagram_received(self, data: Union[bytes, str], addr: Address):
        self.__logger.debug(f'data received: {len(data)}, {data[:64]}, {addr}')

    async def broadcast(self):
        self.__logger.info(f'starting broadcast for {self.__identifier} on {self.__target}')
        while True:
            self.__logger.debug(f'sending broadcast for {self.__identifier} on {self.__target}')
            bitent = self.__identifier.encode('UTF-8')
            binfo = self.__information.encode('UTF-8')
            data: bytes = b''.join((_magic, struct.pack('>I', len(bitent)), struct.pack('>I', len(binfo)), bitent, binfo))
            self.__transport.sendto(data, self.__target)
            if self.__broadcast_count is not None:
                self.__broadcast_count -= 1
                if self.__broadcast_count <= 0:
                    break
            await asyncio.sleep(self.__interval)
        self.__transport.close()
        self.__closed_future.set_result(True)

    async def till_done(self):
        return await self.__closed_future


class BroadcastReceiveProtocol(asyncio.DatagramProtocol):
    def __init__(self, identifier: str, loop: asyncio.AbstractEventLoop = None, single_shot: bool = True, callback: Optional[Callable[[str], Any]] = None):
        self.__logger = logging.get_logger('broadcast_catcher')
        self.__loop = asyncio.get_event_loop() if loop is None else loop
        self.__single_shot = single_shot
        self.__callback = callback
        self.__transport = None
        self.__identifier = identifier
        self.__last_message = None
        self.__closed_future = self.__loop.create_future()

    def connection_made(self, transport: asyncio.transports.DatagramTransport):
        self.__transport = transport
        sock = transport.get_extra_info("socket")  # type: socket.socket
        self.__logger.info(f'started listening on {sock.getsockname()}')
        #sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        #sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 255)
        #sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    def datagram_received(self, data: Union[bytes, str], addr: Address):
        self.__logger.debug(f'data received: {len(data)}, {data[:64]}, {addr}')
        if not data.startswith(_magic):
            return
        data = data[len(_magic):]
        identlen, bodylen = struct.unpack('>II', data[:8])
        identifier = data[8:8+identlen].decode('UTF-8')
        message = data[8+identlen:8+identlen+bodylen].decode('UTF-8')
        self.__logger.info(f'broadcast received from {identifier} at {addr}')
        self.__logger.debug(f'received: {identifier}, {message}')
        self.__last_message = message
        if self.__callback:
            self.__callback(message)
        if self.__single_shot:
            self.close()

    def close(self):
        self.__transport.close()
        self.__closed_future.set_result(self.__last_message)

    async def till_done(self):
        return await self.__closed_future


if __name__ == '__main__':

    async def message_receiver():
        result = await await_broadcast('teststuff')
        print('got result', result)

    asyncio.get_event_loop().create_task(create_broadcaster('teststuff', f'{get_localhost()}:6669'))
    asyncio.get_event_loop().create_task(message_receiver())
    asyncio.get_event_loop().run_forever()
