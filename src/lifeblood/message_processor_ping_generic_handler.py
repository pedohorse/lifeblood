import asyncio
from datetime import datetime, timedelta
from contextlib import contextmanager
from .timestamp import global_timestamp_float, global_timestamp_datetime
from .net_messages.address import AddressChain
from .net_messages.messages import Message
from .net_messages.impl.tcp_simple_command_message_processor import TcpCommandMessageProcessor
from .net_messages.impl.message_haldlers import CommandMessageHandlerBase
from .net_messages.impl.clients import CommandJsonMessageClient
from .logging import get_logger
from .config import get_config

from typing import Dict, Optional, Tuple


class PingGenericHandler(CommandMessageHandlerBase):
    def __init__(self):
        super().__init__()
        self.__next_ping_promises: Dict[AddressChain, Tuple[datetime, asyncio.Task]] = {}
        self.__time_till_next_ping_allowed_delay = int(get_config('pinger').get_option_noasync('time_till_next_ping_allowed_delay', 15))  # seconds
        self._logger = get_logger('ping_handler')

    async def clear_internal_state(self):
        tasks_to_wait = []
        for address in list(self.__next_ping_promises.keys()):
            tasks_to_wait.append(self.__clear_waiting_address(address))

        for task, should_wait in tasks_to_wait:
            if not should_wait:
                continue
            try:
                await task
            except Exception:
                self._logger.exception('error during custom handle_expected_ping_not_coming')

    def command_mapping(self):
        return {
            'ping_generic': self._ping_handler
        }

    async def __timeout_waiter(self, timeout: float, address: AddressChain):
        await asyncio.sleep(timeout)
        if timeout_data := self.__next_ping_promises.pop(address, None):
            expiry_time, myself = timeout_data
            if myself != asyncio.current_task():
                self._logger.debug('dropped timeout task reached after sleep point, ignoring')
                return
            if global_timestamp_datetime() < expiry_time:
                # somehow we were triggered before expiration time
                # - this is not good, this should never happen,
                # in case of new ping new task would have been created, and this cancelled
                self._logger.error('next ping expectation timeout from %s happened before expiration time. internal error', (address,))
                return
        else:
            # this also should never happen, but if it does due to strangenesses of asyncio scheduling
            self._logger.error('next ping expectation timeout from %s happened before expiration time. internal error', (address,))
            return
        self._logger.warning('next ping promise failed from %s', (address,))
        await self.handle_expected_ping_not_coming(address)

    def __clear_waiting_address(self, address: AddressChain) -> Tuple[Optional[asyncio.Task], bool]:
        """
        returns task|None and if it needs to be awaited on
        This is done so that this func is not async and never touches event loop, so no other coros may happen during this,
        but we might want to get exceptions from finished tasks later
        """
        if (timeout_data := self.__next_ping_promises.pop(address, None)) is None:
            self._logger.debug('ping promise from %s is already cleared or was never set', address)
            return None, False

        self._logger.debug('clearing ping promise from %s', address)
        _, timeout_task = timeout_data
        if timeout_task.done():
            return timeout_task, True
        else:
            timeout_task.cancel()
            return timeout_task, False

    async def _ping_handler(self, args: dict, client: CommandJsonMessageClient, original_message: Message):
        data = args.get('data')
        if time_till_next_ping := args.get('max_seconds_till_next_ping'):
            if time_till_next_ping <= 0:
                self._logger.warning('got non-positive max_seconds_till_next_ping, ignoring')
            else:
                timeout = time_till_next_ping + self.__time_till_next_ping_allowed_delay
                timeout_task = asyncio.create_task(self.__timeout_waiter(timeout, original_message.message_source()))
                # first - cancel/clear any existing waiting tasks
                old_timeout_task, should_await_old_timeout_task = self.__clear_waiting_address(original_message.message_source())
                # then create a new task
                self.__next_ping_promises[original_message.message_source()] = (global_timestamp_datetime() + timedelta(seconds=timeout), timeout_task)
                self._logger.debug('setting ping promise from %s to %s sec', original_message.message_source(), timeout)
                if should_await_old_timeout_task:
                    # catch possible exceptions
                    try:
                        await old_timeout_task
                    except Exception:
                        self._logger.exception('error during custom handle_expected_ping_not_coming')
        else:
            # no max_seconds_till_next_ping or None means no future pings are promised from this address
            old_timeout_task, should_await_old_timeout_task = self.__clear_waiting_address(original_message.message_source())
            if should_await_old_timeout_task:
                # catch possible exceptions
                try:
                    await old_timeout_task
                except Exception:
                    self._logger.exception('error during custom handle_expected_ping_not_coming')

        reply: dict = {
            'timestamp': global_timestamp_float(),
        }
        if data is not None:
            reply['data'] = await self.produce_reply(data)

        await client.send_message_as_json(reply)

    async def produce_reply(self, data: dict) -> dict:
        """
        override this for custom logic for ping reply generation
        """
        return {}

    async def handle_expected_ping_not_coming(self, address: AddressChain):
        """
        If ping from given address declared max_seconds_till_next_ping, but that next ping never happened -
        this function fill be run
        """
        pass


class PingGenericClient:
    def __init__(self, client: CommandJsonMessageClient):
        super().__init__()
        self.__client = client

    @classmethod
    @contextmanager
    def get_worker_control_client(cls, worker_address: AddressChain, processor: TcpCommandMessageProcessor) -> "PingGenericClient":
        with processor.message_client(worker_address) as message_client:
            yield PingGenericClient(message_client)

    async def ping(self, data: Optional[dict] = None, *, max_seconds_till_next_ping: Optional[float] = None) -> dict:
        req = {
            'timestamp': global_timestamp_float(),
            'max_seconds_till_next_ping': max_seconds_till_next_ping,  # provide this to allow pingee to detect scheduler being down
        }
        if data is not None:
            req['data'] = data

        await self.__client.send_command(
            'ping_generic',
            req,
        )

        reply = await self.__client.receive_message()
        return await reply.message_body_as_json()
