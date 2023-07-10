import asyncio
from unittest import IsolatedAsyncioTestCase
from lifeblood.logging import get_logger, set_default_loglevel
from lifeblood.nethelpers import get_localhost
from lifeblood.net_messages.address import AddressChain, DirectAddress
from lifeblood.net_messages.messages import Message
from lifeblood.net_messages.client import MessageClient
from lifeblood.net_messages.exceptions import MessageSendingError

from lifeblood.net_messages.tcp_impl.tcp_message_processor import MessageProcessor, MessageProxyProcessor

from typing import Callable, List, Type, Awaitable

set_default_loglevel('DEBUG')
logger = get_logger('message_test')


class TestReceiver(MessageProcessor):
    def __init__(self, listening_host: str, listening_port: int, *, backlog=None):
        super().__init__((listening_host, listening_port), backlog=backlog)
        self.messages_received: List[Message] = []

    async def process_message(self, message: Message, client: MessageClient):
        self._logger.debug(f'got message {message}')
        self.messages_received.append(message)


class DummyReceiver(TestReceiver):
    pass


class DummyReceiverWithReply(TestReceiver):
    def __init__(self, listening_host: str, listening_port: int, *, backlog=None):
        super().__init__(listening_host=listening_host, listening_port=listening_port, backlog=backlog)

    async def process_message(self, message: Message, client: MessageClient):
        try:
            await super().process_message(message, client)
            data = message.message_body()
            self._logger.debug(f'p({message.message_session()}): got message {message}, {bytes(data)}')
            await client.send_message(bytes(reversed(data)))
            self._logger.debug(f'p({message.message_session()}): reply sent')
            msg2 = await client.receive_message()
            self._logger.debug(f'p({message.message_session()}): got message {msg2}, {bytes(data)}')
            await client.send_message(b'!!!' + bytes(reversed(msg2.message_body())))
            self._logger.debug(f'p({message.message_session()}): reply sent')
        except:
            self._logger.exception('whoops!')
            raise


class TestIntegration(IsolatedAsyncioTestCase):

    async def test_direct(self):
        async def _logic(proc1, proc2, proxies):
            with proc1.message_client(proc2.listening_address()) as client:  # type: MessageClient
                timeout = 2
                await client.send_message(b'foofoobarbar')
                while len(proc2.messages_received) == 0:
                    await asyncio.sleep(0.1)
                    timeout -= 0.1
                    if timeout <= 0:
                        raise RuntimeError(f'no message received! {len(proc2.messages_received)}')
                self.assertEqual(1, len(proc2.messages_received))
                self.assertEqual(b'foofoobarbar', proc2.messages_received[0].message_body())

        await self._direct_comm_helper(DummyReceiver, _logic)

    async def test_fail_stream(self):
        async def _logic(proc1: TestReceiver, proc2: TestReceiver, proxies: List[MessageProxyProcessor]):
            addr_to_nowhere = AddressChain.join_address(tuple(p.listening_address() for p in proxies) + (DirectAddress('127.11.22.33:6666'),))  # assume that address does not exist
            with proc1.message_client(addr_to_nowhere, send_retry_attempts=3) as client:
                good = False
                try:
                    await client.send_message(b'something')
                    print('sent')
                except MessageSendingError:
                    good = True
                except Exception as e:
                    logger.exception(e)
                    good = False
                self.assertTrue(good, 'exception was not raised')

        await self._direct_comm_helper(DummyReceiverWithReply, _logic, num_hops=2)
        print('2 hops fine')
        await asyncio.sleep(0.5)
        await self._direct_comm_helper(DummyReceiverWithReply, _logic, num_hops=1)
        print('1 hop fine')
        await asyncio.sleep(0.5)
        await self._direct_comm_helper(DummyReceiverWithReply, _logic, num_hops=0)
        print('0 hops fine')

    async def test_direct_reply(self):
        async def _logic(proc1: TestReceiver, proc2: TestReceiver, proxies):
            with proc1.message_client(proc2.listening_address()) as client:  # type: MessageClient
                timeout = 2
                logger.debug('c: sending message "foofoobarbar"')
                await client.send_message(b'foofoobarbar')
                logger.debug('c: waiting for reply')
                reply = await client.receive_message()
                logger.debug(f'c: reply received: {reply}, {bytes(reply.message_body())}')
                self.assertEqual(b'rabraboofoof', reply.message_body())
                logger.debug('c: sending message "poweroverwhelming"')
                await client.send_message(b'poweroverwhelming')
                logger.debug('c: waiting for reply2')
                reply = await client.receive_message()
                logger.debug(f'c: reply2 received: {reply}, {bytes(reply.message_body())}')
                self.assertEqual(b'!!!gnimlehwrevorewop', reply.message_body())

                while len(proc2.messages_received) == 0:
                    await asyncio.sleep(0.1)
                    timeout -= 0.1
                    if timeout <= 0:
                        raise RuntimeError(f'no message received! {len(proc2.messages_received)}')
                self.assertEqual(1, len(proc2.messages_received))
                self.assertEqual(b'foofoobarbar', proc2.messages_received[0].message_body())

        await self._direct_comm_helper(DummyReceiverWithReply, _logic)

    async def test_direct_parallel(self):
        async def _sub_logic(proc1: TestReceiver, proc2: TestReceiver):
            with proc1.message_client(proc2.listening_address()) as client:  # type: MessageClient
                timeout = 2
                logger.debug(f'c({client.session()}): sending message "foofoobarbar"')
                await client.send_message(b'foofoobarbar')
                logger.debug(f'c({client.session()}): waiting for reply')
                reply = await client.receive_message()
                logger.debug(f'c({client.session()}): reply received: {reply}, {bytes(reply.message_body())}')
                self.assertEqual(b'rabraboofoof', reply.message_body())
                logger.debug(f'c({client.session()}): sending message "poweroverwhelming"')
                await client.send_message(b'poweroverwhelming')
                logger.debug(f'c({client.session()}): waiting for reply2')
                reply = await client.receive_message()
                logger.debug(f'c({client.session()}): reply2 received: {reply}, {bytes(reply.message_body())}')
                self.assertEqual(b'!!!gnimlehwrevorewop', reply.message_body())

                while len(proc2.messages_received) == 0:
                    await asyncio.sleep(0.1)
                    timeout -= 0.1
                    if timeout <= 0:
                        raise RuntimeError(f'no message received! {len(proc2.messages_received)}')
                self.assertEqual(b'foofoobarbar', proc2.messages_received[0].message_body())

                all_tasks.remove(asyncio.current_task())
                logger.info(f'tasks left: {len(all_tasks)}')

        async def _logic(proc1: TestReceiver, proc2: TestReceiver, proxies):
            all_tasks.extend([asyncio.create_task(_sub_logic(proc1, proc2)) for _ in range(666)])

            done, pending = await asyncio.wait(
                all_tasks[:],
                return_when=asyncio.ALL_COMPLETED,
                timeout=30
            )
            logger.info('================== wait done! ==================')
            logger.info(f'error tasks: {len([task for task in done if task.exception() is not None])}')
            logger.info(f'pending tasks: {len(pending)}')
            if len(pending) > 0:
                logger.warning('trying to print stack of some pending tasks...')
                for task in list(pending)[:11]:
                    task.print_stack()

            self.assertEqual(0, len(pending))
            for task in done:
                self.assertTrue(task.done())
                self.assertIsNone(task.exception())

        all_tasks = []
        await self._direct_comm_helper(DummyReceiverWithReply, _logic)

    # proxy

    async def test_proxy(self):
        async def _logic(proc1: TestReceiver, proc2: TestReceiver, proxies: List[MessageProxyProcessor]):
            address = AddressChain.join_address([prox.listening_address() for prox in proxies] + [proc2.listening_address()])
            logger.info(f'sending message to address: "{address}"')
            with proc1.message_client(address) as client:  # type: MessageClient
                timeout = 2
                await client.send_message(b'foofoobarbar')
                while len(proc2.messages_received) == 0:
                    await asyncio.sleep(0.1)
                    timeout -= 0.1
                    if timeout <= 0:
                        raise RuntimeError(f'no message received! {len(proc2.messages_received)}')
                self.assertEqual(1, len(proc2.messages_received))
                for proxy in proxies:
                    self.assertEqual(1, proxy.forwarded_messages_count())
                self.assertEqual(b'foofoobarbar', proc2.messages_received[0].message_body())

        await self._direct_comm_helper(DummyReceiver, _logic, num_hops=4)

    async def test_proxy_reply(self):
        async def _logic(proc1: TestReceiver, proc2: TestReceiver, proxies):
            address = AddressChain.join_address([prox.listening_address() for prox in proxies] + [proc2.listening_address()])
            logger.info(f'sending message to address: "{address}"')

            with proc1.message_client(address) as client:  # type: MessageClient
                logger.debug('c: sending message "foofoobarbar"')
                await client.send_message(b'foofoobarbar')
                logger.debug('c: waiting for reply')
                reply = await client.receive_message()
                logger.debug(f'c: reply received: {reply}, {bytes(reply.message_body())}')
                self.assertEqual(b'rabraboofoof', reply.message_body())
                logger.debug('c: sending message "poweroverwhelming"')
                await client.send_message(b'poweroverwhelming')
                logger.debug('c: waiting for reply2')
                reply = await client.receive_message()
                logger.debug(f'c: reply2 received: {reply}, {bytes(reply.message_body())}')
                self.assertEqual(b'!!!gnimlehwrevorewop', reply.message_body())

                timeout = 2
                while len(proc2.messages_received) == 0:
                    await asyncio.sleep(0.1)
                    timeout -= 0.1
                    if timeout <= 0:
                        raise RuntimeError(f'no message received! {len(proc2.messages_received)}')

                timeout = 1
                while any(proxy.forwarded_messages_count() < 4 for proxy in proxies):
                    await asyncio.sleep(0.1)
                    timeout -= 0.1
                    if timeout <= 0:
                        break  # will raise assertion error after anyway
                self.assertEqual(1, len(proc2.messages_received))
                for proxy in proxies:
                    self.assertEqual(4, proxy.forwarded_messages_count())
                self.assertEqual(b'foofoobarbar', proc2.messages_received[0].message_body())

        await self._direct_comm_helper(DummyReceiverWithReply, _logic, num_hops=3)

    async def _direct_comm_helper(self, proc_class: Type[TestReceiver], logic: Callable[[TestReceiver, TestReceiver, List[MessageProxyProcessor]], Awaitable[None]], num_hops: int = 0):
        addr1 = (get_localhost(), 23456)
        addr2 = (get_localhost(), 23457)
        proc1 = proc_class(*addr1)
        proc2 = proc_class(*addr2)
        proxies = []
        for i in range(num_hops):
            proxy = MessageProxyProcessor((get_localhost(), 23458 + i))
            proxies.append(proxy)

        logger.info('starting up')
        proc1.start()
        proc2.start()
        for proxy in proxies:
            proxy.start()
        await asyncio.sleep(1)
        logger.info('assume all started')
        try:
            logger.info('running test logic')
            await logic(proc1, proc2, proxies)

        finally:
            logger.info('stopping')
            proc1.stop()
            proc2.stop()
            for proxy in proxies:
                proxy.stop()
            logger.info('wait till stopped')
            await proc1.wait_till_stops()
            await proc2.wait_till_stops()
            for proxy in proxies:
                await proxy.wait_till_stops()
            logger.info('all stopped')

