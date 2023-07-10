import asyncio
from unittest import IsolatedAsyncioTestCase

from lifeblood.net_messages.tcp_impl.tcp_message_stream_factory import TcpMessageStreamPooledFactory
from lifeblood.net_messages.address import DirectAddress


class FakeStreamReader:
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


class FakeStreamWriter:
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


def _fake_conn_opener_factory(storage: list, real_func=None):
    async def _fake_conn_opener(dest, src):
        if real_func is None:
            stuff = FakeStreamReader((dest, src)), FakeStreamWriter((dest, src))
        else:
            stuff = await real_func(dest, src)
        await asyncio.sleep(0)
        storage.append(stuff)
        return stuff
    return _fake_conn_opener


class TestConnectionPool(IsolatedAsyncioTestCase):
    async def test_pool(self):
        stream_stash = []
        timeout = 2
        pooled_factory = TcpMessageStreamPooledFactory(timeout, _fake_conn_opener_factory(stream_stash))

        addr0 = DirectAddress('123.234:345'), DirectAddress('0.0:0')
        addr1 = DirectAddress('234.345:456'), DirectAddress('1.1:1')
        addr2 = DirectAddress('345.456:567'), DirectAddress('0.0:0')
        for i, addr in enumerate((addr0, addr1, addr2)):
            for _ in range(5):
                stream0 = await pooled_factory.open_sending_stream(*addr)
                await asyncio.sleep(0.077)
                stream0.close()
                await stream0.wait_closed()
                await asyncio.sleep(0.1)
            self.assertEqual(1+i, len(stream_stash))

        # now close all
        pooled_factory.close_pool()
        await pooled_factory.wait_pool_closed()
        self.assertEqual(3, len(stream_stash))
        for rstream, wstream in stream_stash:
            self.assertTrue(wstream.closed)
            self.assertTrue(wstream.close_waited)

    async def test_pool_prune(self):
        stream_stash = []
        timeout = 1
        pooled_factory = TcpMessageStreamPooledFactory(timeout, _fake_conn_opener_factory(stream_stash))

        addr0 = DirectAddress('123.234:345'), DirectAddress('0.0:0')
        addr1 = DirectAddress('234.345:456'), DirectAddress('1.1:1')
        addr2 = DirectAddress('345.456:567'), DirectAddress('0.0:0')
        for i, addr in enumerate((addr0, addr1, addr2)):
            for _ in range(5):
                stream0 = await pooled_factory.open_sending_stream(*addr)
                await asyncio.sleep(0.0077)
                stream0.close()
                await stream0.wait_closed()
                await asyncio.sleep(0.01)
            self.assertEqual(1+i, len(stream_stash))

        # now prune, but nothing should be pruned cuz of timeouts
        await pooled_factory.prune()
        for rstream, wstream in stream_stash:
            self.assertFalse(wstream.closed)
            self.assertFalse(wstream.close_waited)

        # now wait and prune pooled connections
        await asyncio.sleep(1.1)
        await pooled_factory.prune()
        for rstream, wstream in stream_stash:
            self.assertTrue(wstream.closed)
            self.assertTrue(wstream.close_waited)

        # now again connect to all addresses
        for i, addr in enumerate((addr0, addr1, addr2)):
            for _ in range(5):
                stream0 = await pooled_factory.open_sending_stream(*addr)
                await asyncio.sleep(0.077)
                stream0.close()
                await stream0.wait_closed()
                await asyncio.sleep(0.1)
            self.assertEqual(4+i, len(stream_stash))

        # now close all
        pooled_factory.close_pool()
        await pooled_factory.wait_pool_closed()
        self.assertEqual(6, len(stream_stash))
        for rstream, wstream in stream_stash:
            self.assertTrue(wstream.closed)
            self.assertTrue(wstream.close_waited)

    async def test_pool_close_before_streams(self):
        stream_stash = []
        timeout = 2
        pooled_factory = TcpMessageStreamPooledFactory(timeout, _fake_conn_opener_factory(stream_stash))

        addr0 = DirectAddress('123.234:345'), DirectAddress('0.0:0')
        addr1 = DirectAddress('234.345:456'), DirectAddress('1.1:1')
        addr2 = DirectAddress('345.456:567'), DirectAddress('0.0:0')
        stream0 = await pooled_factory.open_sending_stream(*addr0)
        stream1 = await pooled_factory.open_sending_stream(*addr1)
        stream2 = await pooled_factory.open_sending_stream(*addr2)
        self.assertEqual(3, len(stream_stash))
        await asyncio.sleep(0.0789)

        # we close one of the streams in expected way
        stream1.close()
        await stream1.wait_closed()

        # nothing should be closed just yet
        for rstream, wstream in stream_stash:
            self.assertFalse(wstream.closed)
            self.assertFalse(wstream.close_waited)

        # now we close pool
        pooled_factory.close_pool()
        wait_task = asyncio.create_task(pooled_factory.wait_pool_closed())
        await asyncio.sleep(0.5)
        self.assertFalse(wait_task.done())  # not done cus streams r opened

        # stream1 might be closed (we did not wait - cannot be sure), but streams 0 and 2 will not for sure
        for rstream, wstream in (stream_stash[0], stream_stash[2]):
            self.assertFalse(wstream.closed)
            self.assertFalse(wstream.close_waited)

        # now closing wrappers should cause stream closure
        stream0.close()
        await stream0.wait_closed()
        stream2.close()
        await stream2.wait_closed()
        self.assertEqual(3, len(stream_stash))

        for rstream, wstream in stream_stash:
            self.assertTrue(wstream.closed)
            self.assertTrue(wstream.close_waited)

        await wait_task

    async def test_pooled_connection_closed_from_another_side(self):
        stream_stash = []
        timeout = 2
        pooled_factory = TcpMessageStreamPooledFactory(timeout, _fake_conn_opener_factory(stream_stash))

        addr0 = DirectAddress('123.234:345'), DirectAddress('0.0:0')
        addr1 = DirectAddress('234.345:456'), DirectAddress('1.1:1')
        addr2 = DirectAddress('345.456:567'), DirectAddress('0.0:0')
        for i, addr in enumerate((addr0, addr1, addr2)):
            stream0 = await pooled_factory.open_sending_stream(*addr)
            await asyncio.sleep(0.0077)
            stream0.close()
            await stream0.wait_closed()
            await asyncio.sleep(0.01)
            self.assertEqual(1 + i, len(stream_stash))

        # now connection got closed
        for rstream, wstream in stream_stash:
            rstream.imitate_closed_by_peer()

        for i, addr in enumerate((addr0, addr1, addr2)):
            stream0 = await pooled_factory.open_sending_stream(*addr)
            await asyncio.sleep(0.0077)
            stream0.close()
            await stream0.wait_closed()
            await asyncio.sleep(0.01)
            self.assertEqual(4 + i, len(stream_stash))
