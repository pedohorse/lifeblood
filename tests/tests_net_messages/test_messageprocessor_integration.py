import asyncio
import random
from unittest import IsolatedAsyncioTestCase
from lifeblood.logging import get_logger, set_default_loglevel
from lifeblood.nethelpers import get_localhost
from lifeblood.net_messages.address import AddressChain, DirectAddress
from lifeblood.net_messages.messages import Message
from lifeblood.net_messages.client import MessageClient
from lifeblood.net_messages.exceptions import MessageSendingError, MessageTransferTimeoutError

from lifeblood.net_messages.tcp_impl.tcp_message_processor import TcpMessageProcessor, TcpMessageProxyProcessor

from typing import Callable, List, Type, Awaitable

set_default_loglevel('DEBUG')
logger = get_logger('message_test')


class TestReceiver(TcpMessageProcessor):
    def __init__(self, listening_host: str, listening_port: int, *, backlog=None, artificial_delay: float = 0, stream_timeout=None, default_client_retry_attempts=None):
        super().__init__((listening_host, listening_port), backlog=backlog, stream_timeout=stream_timeout, default_client_retry_attempts=default_client_retry_attempts)
        self.messages_received: List[Message] = []
        self.__artificial_delay: float = artificial_delay

    async def process_message(self, message: Message, client: MessageClient):
        self._logger.debug(f'got message {message}')
        self.messages_received.append(message)

    async def new_message_received(self, message: Message) -> bool:
        if self.__artificial_delay > 0:
            logger.warning(f'artificially waiting for {self.__artificial_delay}')
            await asyncio.sleep(self.__artificial_delay)
            logger.warning(f'artificial wait over')
        return await super().new_message_received(message)


class DummyReceiver(TestReceiver):
    pass


class DummyReceiverWithReply(TestReceiver):
    _counter = 0

    def __init__(self, listening_host: str, listening_port: int, *, backlog=None, artificial_delay: float = 0, stream_timeout=None):
        super().__init__(listening_host=listening_host,
                         listening_port=listening_port,
                         backlog=backlog,
                         artificial_delay=artificial_delay,
                         stream_timeout=stream_timeout,
                         default_client_retry_attempts=6)  # cuz on slow windows test ci worker there seem to be backlog error, so we have to keep trying for some time
        self.__my_id = self._counter
        DummyReceiverWithReply._counter += 1

    async def process_message(self, message: Message, client: MessageClient):
        try:
            await super().process_message(message, client)
            data = message.message_body()
            self._logger.debug(f'p({self.__my_id}:{message.message_session()}): got message {message}, {bytes(data)}')
            await client.send_message(bytes(reversed(data)))
            self._logger.debug(f'p({self.__my_id}:{message.message_session()}): reply sent')
            msg2 = await client.receive_message()
            self._logger.debug(f'p({self.__my_id}:{message.message_session()}): got message {msg2}, {bytes(data)}')
            await client.send_message(b'!!!' + bytes(reversed(msg2.message_body())))
            self._logger.debug(f'p({self.__my_id}:{message.message_session()}): reply sent')
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
        async def _logic(proc1: TestReceiver, proc2: TestReceiver, proxies: List[TcpMessageProxyProcessor]):
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

    async def test_timeout(self):
        async def _logic(proc1: TestReceiver, proc2: TestReceiver, proxies: List[TcpMessageProxyProcessor]):
            good = False
            with proc1.message_client(AddressChain.join_address([prox.listening_address() for prox in proxies] + [proc2.listening_address()]), send_retry_attempts=3) as client:  # type: MessageClient
                client.set_base_attempt_timeout(0.6)
                try:
                    await client.send_message(b'foofoobarbar')
                except MessageTransferTimeoutError:
                    good = True

            logger.info('--- sleeping waiting ---')
            await asyncio.sleep(3)

            self.assertTrue(good, "exception was not raised")

        logger.info('=== testing with 0 hops ===')
        await self._direct_comm_helper(lambda h, p: DummyReceiver(h, p, stream_timeout=1, artificial_delay=1.5), _logic, num_hops=0)
        logger.info('=== testing with 2 hops ===')
        await self._direct_comm_helper(lambda h, p: DummyReceiver(h, p, stream_timeout=1, artificial_delay=1.5), _logic, num_hops=2)

    async def test_timeout_with_lost_reply(self):
        async def _logic(proc1: TestReceiver, proc2: TestReceiver, proxies: List[TcpMessageProxyProcessor]):
            good = False
            with proc1.message_client(AddressChain.join_address([prox.listening_address() for prox in proxies] + [proc2.listening_address()]), send_retry_attempts=3) as client:  # type: MessageClient
                client.set_base_attempt_timeout(0.6)
                try:
                    await client.send_message(b'foofoobarbar')
                except MessageTransferTimeoutError:
                    good = True
                else:  # this should NEVER happen if test succeeds
                    reply = await client.receive_message()
                    await client.send_message(b'poweroverwhelming')
                    reply = await client.receive_message()

            logger.info('--- sleeping waiting ---')
            await asyncio.sleep(3)

            self.assertTrue(good, "exception was not raised")

        logger.info('=== testing with 0 hops ===')
        await self._direct_comm_helper(lambda h, p: DummyReceiverWithReply(h, p, stream_timeout=1, artificial_delay=1.5), _logic, num_hops=0)
        #logger.info('=== testing with 2 hops ===')
        #await self._direct_comm_helper(lambda h, p: DummyReceiverWithReply(h, p, stream_timeout=1, artificial_delay=1.5), _logic, num_hops=2)

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
                timeout=300
            )
            logger.info('================== wait done! ==================')
            errored = [task for task in done if task.exception() is not None]
            logger.info(f'error tasks: {len(errored)}')
            logger.info(f'pending tasks: {len(pending)}')
            if len(pending) > 0:
                logger.warning('trying to print stack of some pending tasks...')
                for task in list(pending)[:11]:
                    task.print_stack()
            if len(errored) > 0:
                logger.warning('trying to print some errors')
                for task in errored[:11]:
                    logger.error(task.exception())

            self.assertEqual(0, len(pending))
            for task in done:
                self.assertTrue(task.done())
                self.assertIsNone(task.exception())

        all_tasks = []
        await self._direct_comm_helper(DummyReceiverWithReply, _logic)

    # proxy

    async def test_proxy(self):
        async def _logic(proc1: TestReceiver, proc2: TestReceiver, proxies: List[TcpMessageProxyProcessor]):
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

    async def _direct_comm_helper(self,
                                  proc_class: Callable[[str, int], TestReceiver],
                                  logic: Callable[[TestReceiver, TestReceiver, List[TcpMessageProxyProcessor]], Awaitable[None]],
                                  num_hops: int = 0,
                                  proxy_timeout: float = 30):
        addr1 = (get_localhost(), 23456)
        addr2 = (get_localhost(), 23457)
        proc1 = proc_class(*addr1)
        proc2 = proc_class(*addr2)
        proxies = []
        for i in range(num_hops):
            proxy = TcpMessageProxyProcessor((get_localhost(), 23458 + i), stream_timeout=proxy_timeout)
            proxies.append(proxy)

        logger.info('starting up')
        await proc1.start()
        await proc2.start()
        for proxy in proxies:
            await proxy.start()
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

