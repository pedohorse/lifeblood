import asyncio
import socket
from string import ascii_letters
import random
import struct

from . import logging

from typing import Tuple, Union

Address = Tuple[str, int]

# base taken from https://gist.github.com/yluthu/4f785d4546057b49b56c

_magic = b'=\xe2\x88\x88\xe2\xad\x95\xe2\x88\x8b='


async def create_broadcaster(identifier, information, broad_port=9000, ip='0.0.0.0', broadcasts_count=None):
    loop = asyncio.get_event_loop()
    return await loop.create_datagram_endpoint(
        lambda: BroadcastProtocol(identifier, information, (ip, broad_port), broadcasts_count, loop=loop),
        family=socket.AF_INET, reuse_port=True, allow_broadcast=True)


async def await_broadcast(identifier, broad_port=9000):
    loop = asyncio.get_event_loop()
    protocol: BroadcastReceiveProtocol
    _, protocol = await loop.create_datagram_endpoint(
        lambda: BroadcastReceiveProtocol(identifier, loop=loop),
        local_addr=('0.0.0.0', broad_port), family=socket.AF_INET, reuse_port=True, allow_broadcast=True)
    message = await protocol.till_done()

    # rebroadcast to localhost
    _, protocol = await create_broadcaster(identifier, message, broad_port, ip='127.0.0.1', broadcasts_count=1)  # TODO: bet with my future self - i will regret hardcoding ipv4 localhost like that
    await protocol.till_done()
    return message


class BroadcastProtocol(asyncio.DatagramProtocol):
    def __init__(self, identifier: str, information: str, target: Address, count=None, *, loop: asyncio.AbstractEventLoop = None):
        self.__logger = logging.get_logger('broadcast')
        self.__target = target
        self.__loop = asyncio.get_event_loop() if loop is None else loop
        self.__transport = None
        self.__identifier = identifier
        self.__interval = 10
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
        while True:
            self.__logger.info(f'sending broadcast for {self.__identifier} on {self.__target}')
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
    def __init__(self, identifier: str, loop: asyncio.AbstractEventLoop = None):
        self.__logger = logging.get_logger('broadcast_catcher')
        self.__loop = asyncio.get_event_loop() if loop is None else loop
        self.__transport = None
        self.__identifier = identifier
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
        self.__transport.close()
        self.__closed_future.set_result(message)

    async def till_done(self):
        return await self.__closed_future


if __name__ == '__main__':

    async def message_receiver():
        result = await await_broadcast('teststuff')
        print('got result', result)

    asyncio.get_event_loop().create_task(create_broadcaster('teststuff', '127.0.0.1:6669'))
    asyncio.get_event_loop().create_task(message_receiver())
    asyncio.get_event_loop().run_forever()
