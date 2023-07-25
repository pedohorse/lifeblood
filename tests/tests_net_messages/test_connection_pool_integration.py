import asyncio

import uuid
import socket
import struct
from unittest import IsolatedAsyncioTestCase
from lifeblood.nethelpers import get_localhost
from lifeblood.logging import set_default_loglevel
set_default_loglevel('DEBUG')  # TODO: this must be set by env variable
from lifeblood.net_messages.tcp_impl.message_protocol import MessageProtocol, IProtocolInstanceCounter
from lifeblood.net_messages.tcp_impl.tcp_message_stream_factory import TcpMessageStreamPooledFactory, _initialize_connection
from lifeblood.net_messages.address import DirectAddress


class TestProtocolInstanceCounter(IProtocolInstanceCounter):
    def _protocol_inc_count(self, instance):
        pass

    def _protocol_dec_count(self, instance):
        pass

    def _allowed_new_instances(self):
        return True


class MessageProtocolTest(MessageProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_writer = None
        self.last_reader = None

    async def connection_callback(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.last_writer = writer
        self.last_reader = reader
        return await super().connection_callback(reader, writer)


class FakeStreamReader:  # duplicate from test_connection_pool
    def __init__(self, addr):
        self.addr = addr
        self.__closed = False

    async def readexactly(self, size):
        await asyncio.sleep(0)
        if size <= 0:
            return b''
        if size == 1:  # assume it's ack
            return b'\1'
        else:
            return b'\0'*size

    def imitate_closed_by_peer(self):
        self.__closed = True

    def at_eof(self):
        return self.__closed


class FakeStreamWriter:  # duplicate from test_connection_pool
    def __init__(self, addr):
        self.addr = addr
        self.closed = False
        self.close_waited = False

    def write(self, data):
        return

    async def drain(self):
        await asyncio.sleep(0)

    def close(self):
        self.closed = True

    def is_closing(self):
        return self.closed

    async def wait_closed(self):
        if not self.closed:
            raise RuntimeError('waiting before closing!')
        await asyncio.sleep(0.05)
        self.close_waited = True


def _fake_conn_opener_factory(storage: list, real_func=None):  # duplicate from test_connection_pool
    async def _fake_conn_opener(dest, src):
        if real_func is None:
            stuff = FakeStreamReader((dest, src)), FakeStreamWriter((dest, src))
        else:
            stuff = await real_func(dest, src)
        await asyncio.sleep(0)
        storage.append(stuff)
        return stuff
    return _fake_conn_opener


class TestConnectionPoolIntegration(IsolatedAsyncioTestCase):
    async def test_pooled_connection_closed_from_another_side_real(self):
        # sock = None
        counter = TestProtocolInstanceCounter()
        srv = None
        prt = None

        async def _noop(message):
            return True

        async def _aconnsink():
            nonlocal srv
            nonlocal prt

            def _create_protocol():
                nonlocal prt
                prt = MessageProtocolTest(counter, (get_localhost(), 29360), _noop)
                return prt

            srv = await asyncio.get_event_loop().create_server(_create_protocol,
                                                               get_localhost(), 29361)

        # listen_task = asyncio.get_event_loop().run_in_executor(None, _connsink)
        listen_task = asyncio.create_task(_aconnsink())
        while srv is None:
            await asyncio.sleep(0.1)

        try:
            stream_stash = []
            timeout = 2
            pooled_factory = TcpMessageStreamPooledFactory(timeout, _fake_conn_opener_factory(stream_stash, _initialize_connection), timeout=5)

            addr = DirectAddress(f'{get_localhost()}:29361'), DirectAddress(f'{get_localhost()}:29360')

            print('attempting connection')
            for _ in range(3):
                stream0 = await pooled_factory.open_sending_stream(*addr)
                stream0.close()
                await stream0.wait_closed()

            self.assertEqual(1, len(stream_stash))

            # now imitate connection closed from the other side
            print('closing server writer to imitate nice connection lost')
            prt.last_writer.close()
            await prt.last_writer.wait_closed()

            for _ in range(3):
                stream0 = await pooled_factory.open_sending_stream(*addr)
                await stream0.send_data_message(b'foo', addr[0], session=uuid.uuid4())
                stream0.close()
                await stream0.wait_closed()

            self.assertEqual(2, len(stream_stash))

            # now imitate connection closed more aggressively from the other side
            print('shutting down server socket to imitate not as nice connection lost')
            sock = prt.last_writer.get_extra_info('socket')
            sock.shutdown(socket.SHUT_RDWR)
            # expect exception being printed from the protocol

            for _ in range(3):
                stream0 = await pooled_factory.open_sending_stream(*addr)
                await stream0.send_data_message(b'bar', addr[0], session=uuid.uuid4())
                stream0.close()
                await stream0.wait_closed()

            self.assertEqual(3, len(stream_stash))
        finally:
            print('closing all')
            if not listen_task.done():
                listen_task.cancel()
            srv.close()
            pooled_factory.close_pool()
            print('waiting closed')
            await pooled_factory.wait_pool_closed()
            print('pool closed')
            await srv.wait_closed()
            print('srv closed')

