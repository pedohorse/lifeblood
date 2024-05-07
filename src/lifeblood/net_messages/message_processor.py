import asyncio
import uuid

from .queue import MessageQueue
from .messages import Message
from .interfaces import MessageReceiverFactory, MessageStreamFactory
from .client import MessageClient, MessageClientFactory, RawMessageClientFactory
from .message_handler import MessageHandlerBase
from .logging import get_logger
from .address import AddressChain, DirectAddress
from .address_routing import AddressRouter
from .enums import MessageType
from .exceptions import StreamOpeningError
from ..component_base import ComponentBase

from typing import Iterable, List, Optional, Sequence, Tuple, Union


class ProcessedSessionsMap:
    def __init__(self):
        self.__map = {}
        self.__empty_event = asyncio.Event()
        self.__empty_event.set()

    def empty_event(self):
        return self.__empty_event

    def __getitem__(self, item):
        return self.__map[item]

    def __setitem__(self, key, value):
        self.__map[key] = value
        if self.__empty_event.is_set():
            self.__empty_event.clear()

    def pop(self, key):
        item = self.__map.pop(key)
        if len(self.__map) == 0:
            self.__empty_event.set()
        return item

    def __contains__(self, item):
        return item in self.__map


class MessageProcessorBase(ComponentBase):
    def __init__(self, listening_address_or_addresses: Union[DirectAddress, Iterable[DirectAddress]], *,
                 address_router: AddressRouter,
                 message_receiver_factory: MessageReceiverFactory,
                 message_stream_factory: MessageStreamFactory,
                 message_client_factory: MessageClientFactory = None,
                 default_client_retry_attempts: Optional[int] = None,
                 message_handlers: Sequence[MessageHandlerBase] = ()):
        super().__init__()
        self.__message_queue = MessageQueue()
        if isinstance(listening_address_or_addresses, DirectAddress):
            self.__addresses = (listening_address_or_addresses,)
        else:
            self.__addresses = tuple(listening_address_or_addresses)
        self.__address_router = address_router
        self.__sessions_being_processed = ProcessedSessionsMap()
        self.__processing_tasks = set()
        self.__forwarded_messages_count = 0
        self.__message_receiver_factory = message_receiver_factory
        self.__message_stream_factory = message_stream_factory
        self.__message_client_factory = message_client_factory or RawMessageClientFactory()
        self.__default_client_retry_attempts = 2 if default_client_retry_attempts is None else default_client_retry_attempts
        self.__handlers: List[MessageHandlerBase] = list(message_handlers)

        self._logger = get_logger(f'message_processor {type(self).__name__}')

    class _ClientContext:
        def __init__(self, host_address: AddressChain,
                     destination: AddressChain,
                     message_queue: MessageQueue,
                     sessions_being_processed: ProcessedSessionsMap,
                     message_stream_factory: MessageStreamFactory,
                     message_client_factory: MessageClientFactory,
                     force_session: Optional[uuid.UUID] = None,
                     send_retry_attempts: int = 2):
            self.__destination = destination
            self.__force_session = force_session
            self.__sessions_being_processed = sessions_being_processed
            self.__message_queue = message_queue
            self.__message_stream_factory = message_stream_factory
            self.__message_client_factory = message_client_factory
            self.__address = host_address
            self.__session = None
            self.__initialized = False
            self.__send_retry_attempts = send_retry_attempts

        def initialize(self) -> MessageClient:
            if self.__initialized:
                raise RuntimeError('already initialized')
            self.__initialized = True

            if self.__force_session is None:
                while (session := uuid.uuid4()) in self.__sessions_being_processed:
                    pass
            else:
                if self.__force_session in self.__sessions_being_processed:
                    raise ValueError(f'forced session cannot be already in processing! {self.__force_session}')
                session = self.__force_session

            self.__session = session

            client = self.__message_client_factory.create_message_client(
                self.__message_queue, session,
                source_address_chain=self.__address,
                destination_address_chain=self.__destination,
                message_stream_factory=self.__message_stream_factory,
                send_retry_attempts=self.__send_retry_attempts
            )
            self.__sessions_being_processed[session] = client
            return client

        def finalize(self):
            if not self.__initialized:
                raise RuntimeError('not yet initialized')
            self.__sessions_being_processed.pop(self.__session)

        def __enter__(self):
            return self.initialize()

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.finalize()

    def listening_address(self, for_this: AddressChain) -> DirectAddress:
        """
        message processor may listen to several nic addresses,
        this function will return address processor listens to that system can route to given for_this address
        """
        return self.__address_router.select_source_for(self.__addresses, for_this)

    def listening_addresses(self) -> Tuple[DirectAddress]:
        return self.__addresses

    def forwarded_messages_count(self):
        return self.__forwarded_messages_count

    def add_message_handler(self, handler: MessageHandlerBase):
        if handler not in self.__handlers:
            self.__handlers.append(handler)

    def message_client(self, destination: AddressChain, *, force_session: Optional[uuid.UUID] = None, send_retry_attempts: Optional[int] = None) -> _ClientContext:
        if send_retry_attempts is None:
            send_retry_attempts = self.__default_client_retry_attempts
        source_address = self.__address_router.select_source_for(self.__addresses, destination)
        return MessageProcessorBase._ClientContext(source_address,
                                                   destination,
                                                   self.__message_queue,
                                                   self.__sessions_being_processed,
                                                   self.__message_stream_factory,
                                                   self.__message_client_factory,
                                                   force_session,
                                                   send_retry_attempts=send_retry_attempts)

    def _main_task(self):
        return self.__serve()

    async def __serve(self):
        self._logger.info('starting serving messages')
        servers = []
        exception_to_reraise = None
        try:
            for address in self.__addresses:
                servers.append(await self.__message_receiver_factory.create_receiver(address, self.new_message_received))
        except Exception as e:
            # must stop the ones that were started
            self._logger.debug('some listeners failed to start, failing...')
            if len(servers) == 0:  # if we have not yet started anything
                raise
            self._logger.debug('some listeners were already started, so first stopping them, then failing...')
            exception_to_reraise = e
            self._stop_event.set()
        else:
            self._logger.debug('server started')
            self._main_task_is_ready_now()

        await self._stop_event.wait()

        self._logger.info('message server stopping...')
        self._logger.debug('waiting for all existing sessions to finish')
        await self.__sessions_being_processed.empty_event().wait()
        self._logger.debug('all sessions finished, stopping server')
        await self._pre_receiver_stop()
        for server in servers:
            server.stop()
        await self._post_receiver_stop()
        await asyncio.gather(*(
            server.wait_till_stopped()
            for server in servers
        ))
        await self._post_receiver_stop_waited()
        self._logger.info('message server stopped')
        if exception_to_reraise:
            self._logger.debug('re-raising exception that happened during listener creation')
            raise exception_to_reraise

    async def _pre_receiver_stop(self):
        return

    async def _post_receiver_stop(self):
        return

    async def _post_receiver_stop_waited(self):
        return

    async def should_process(self, orig_message: Message):
        """
        override this to decide on processing NEW messages
        """
        return not self._stop_event.is_set()

    async def new_message_received(self, message: Message) -> bool:
        r"""
        note about ordering: if session messages come from the same tcp connection - then same protocol instance
         is processing it, so only one message is processed here at a time
         However if messages of same session are coming from multiple tcp connections - there is already no way of
         telling what is the correct order, so avoid that.
         smth like:
         >A1 >A2 <B1 >A3 <B2 <B3 >A4
         \_____/ \_/ \_/ \_____/ \_/
         each group should have single tcp connection, otherwise no guarantee about ordering
        """
        destination = message.message_destination().split_address()
        assert isinstance(destination, tuple)
        if destination[0] not in self.__addresses:
            self._logger.error('received message not meant for me, dropping')
            return True

        si = 0
        for si, addr in enumerate(destination):
            if addr not in self.__addresses:
                break
        else:
            si += 1

        current_part = destination[:si]
        assert len(current_part) > 0  # destination check above catches this error, so assert must never fail
        next_part = destination[si:]
        if len(next_part) > 0:  # redirect it further
            return_input_address = self.__address_router.select_source_for(self.__addresses, next_part[0])
            if len(current_part) == 2 and current_part[0] != current_part[1] and current_part[1] == return_input_address \
                    or len(current_part) == 1 and current_part[0] == return_input_address:
                pass  # easier and more explicit to state cases of correctly normalized addresses than invert the expression above
            else:
                self._logger.debug('incoming redirected message address is not normalized, re-normalizing')
                current_part = current_part[:1]  # throw out any other our addresses, including dups
                if current_part[-1] != return_input_address:  # for now, we enforce addresses to be normalized
                    current_part = (*current_part, return_input_address)

            try:
                stream = await self.__message_stream_factory.open_sending_stream(next_part[0], return_input_address)
            except StreamOpeningError:
                raise
            except Exception as e:
                raise StreamOpeningError(wrapped_exception=e) from None

            try:
                message.set_message_destination(AddressChain.join_address(next_part))
                message.set_message_source(AddressChain.join_address((*reversed(current_part), *(message.message_source().split_address()))))
                await stream.send_raw_message(message)
                self.__forwarded_messages_count += 1
            finally:
                try:
                    stream.close()
                    await stream.wait_closed()
                except:
                    self._logger.exception('failed to close forwarding stream, suppressing')
            return True

        if len(current_part) > 1:
            self._logger.debug('incoming message address is not normalized, re-normalizing')
            message.set_message_destination(current_part[0])

        if message.message_type() == MessageType.SYSTEM_PING:
            return True

        session = message.message_session()
        if session in self.__sessions_being_processed:
            await self.__message_queue.put_message(message)
            return True

        # otherwise - noone is expecting message, so we process it
        # we rely here on that several messages of same session CANNOT be processed here at the same time
        #async with self.message_client(message.message_source(), force_session=session) as client:
        if not await self.should_process(message):
            return False
        context = self.message_client(message.message_source(), force_session=session)
        client = context.initialize()
        task = asyncio.create_task(self.__process_message_wrapper(message, client, context))
        self.__processing_tasks.add(task)
        return True

    def get_address_for(self, target_address: AddressChain):
        """
        select one of the addresses this processor is listening to
        that is able to directly connect to the first direct address in given target_address address chain
        """
        return self.__address_router.select_source_for(self.__addresses, target_address)

    async def __process_message_wrapper(self, message: Message, client: MessageClient, context: _ClientContext):
        try:
            for handler in self.__handlers:
                processed = await handler.process_message(message, client)
                if processed:
                    break
            else:
                await self.process_message(message, client)
        except Exception as e:
            self._logger.exception('processing exception happened')
        finally:
            self.__processing_tasks.remove(asyncio.current_task())
            context.finalize()

    async def process_message(self, message: Message, client: MessageClient):
        """
        This will be called only if no handler processed the message

        Override this with actual processing
        """
        self._logger.warning(f'no handlers found to process message "{message}", ignoring')
