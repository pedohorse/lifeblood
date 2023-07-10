import asyncio
from contextlib import asynccontextmanager
import uuid

from .queue import MessageQueue
from .messages import Message
from .message_protocol import MessageProtocol
from .client import MessageClient
from .logging import get_logger
from .connections import open_message_connection
from .address import split_address, join_address
from ..component_base import ComponentBase

from typing import Optional, Tuple


class MessageProcessorBase(ComponentBase):
    def __init__(self, listening_host: str, listening_port: int, *, backlog: Optional[int] = None):
        super().__init__()
        self.__message_queue = MessageQueue()
        self.__address = (listening_host, listening_port)
        self.__sessions_being_processed = {}
        self.__processing_tasks = set()
        self.__forwarded_messages_count = 0
        self.__socket_backlog = backlog or 4096
        self._logger = get_logger('message_processor')

    class _ClientContext:
        def __init__(self, host_address,
                     destination: str,
                     message_queue: MessageQueue,
                     sessions_being_processed: dict,
                     force_session: Optional[uuid.UUID] = None):
            self.__destination = destination
            self.__force_session = force_session
            self.__sessions_being_processed = sessions_being_processed
            self.__message_queue = message_queue
            self.__address = host_address
            self.__session = None
            self.__initialized = False

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

            client = MessageClient(self.__message_queue, session, source=self.__address, destination=self.__destination)
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

    def listening_address(self) -> Tuple[str, int]:
        return self.__address

    def listening_link(self) -> str:
        return join_address((self.__address,))

    def forwarded_messages_count(self):
        return self.__forwarded_messages_count

    def message_client(self, destination: str, force_session: Optional[uuid.UUID] = None) -> _ClientContext:
        return MessageProcessorBase._ClientContext(self.__address, destination, self.__message_queue, self.__sessions_being_processed, force_session)

    # @asynccontextmanager
    # async def message_client(self, destination: str, force_session: Optional[uuid.UUID] = None) -> AsyncIterator[MessageClient]:
    #     """
    #     use this line
    #     async with processor.message_client(to_smth) as clinet:
    #         await client.send_message(data)
    #         process_reply(await client.recieve_message())
    #
    #     """
    #     if force_session is None:
    #         while (session := uuid.uuid4()) in self.__sessions_being_processed:
    #             pass
    #     else:
    #         if force_session in self.__sessions_being_processed:
    #             raise ValueError(f'forced session cannot be already in processing! {force_session}')
    #         session = force_session
    #
    #     client = MessageClient(self.__message_queue, session, source=self.__address, destination=destination)
    #     self.__sessions_being_processed[session] = client
    #     try:
    #         yield client
    #     finally:
    #         self.__sessions_being_processed.pop(session)
    #

    def _main_task(self):
        return self.__serve()

    async def __serve(self):
        self._logger.info('start serving messages')
        server = await asyncio.get_event_loop().create_server(lambda: MessageProtocol(self.__address, self.new_message_received),
                                                              self.__address[0],
                                                              self.__address[1],
                                                              backlog=self.__socket_backlog)
        self._logger.debug('server started')
        while not self._stop_event.is_set():
            await asyncio.sleep(0.5)

        self._logger.info('message server stopping...')
        server.close()
        await server.wait_closed()
        self._logger.info('message server stopped')

    async def new_message_received(self, message: Message):
        """
        note about ordering: if session messages come from the same tcp connection - then same protocol instance
         is processing it, so only one message is processed here at a time
         However if messages of same session are coming from multiple tcp connections - there is already no way of
         telling what is the correct order, so avoid that.
         smth like:
         >A1 >A2 <B1 >A3 <B2 <B3 >A4
         \_____/ \_/ \_/ \_____/ \_/
         each group should have single tcp connection, otherwise no guarantee about ordering
        """
        destination = split_address(message.message_destination())
        if destination[0] != self.__address:
            self._logger.error('received message not meant for me, dropping')
            return

        if len(destination) > 1:  # redirect it further
            dcurrent, dnext = destination[0], destination[1:]
            stream = await open_message_connection(dnext, ('', -1))
            try:
                message.set_message_destination(join_address(dnext))
                message.set_message_source(join_address((dcurrent, *split_address(message.message_source()))))
                await stream.forward_message(message)
                self.__forwarded_messages_count += 1
            finally:
                try:
                    stream.close()
                    await stream.wait_closed()
                except:
                    self._logger.exception('failed to close forwarding stream, suppressing')
            return

        session = message.message_session()
        if session in self.__sessions_being_processed:
            await self.__message_queue.put_message(message)
            return

        # otherwise - noone is expecting message, so we process it
        # we rely here on that several messages of same session CANNOT be processed here at the same time
        #async with self.message_client(message.message_source(), force_session=session) as client:
        context = self.message_client(message.message_source(), force_session=session)
        client = context.initialize()
        task = asyncio.create_task(self.__process_message_wrapper(message, client, context))
        self.__processing_tasks.add(task)

    async def __process_message_wrapper(self, message: Message, client: MessageClient, context: _ClientContext):
        try:
            await self.process_message(message, client)
        except Exception as e:
            self._logger.exception('processing exception happened')
        finally:
            self.__processing_tasks.remove(asyncio.current_task())
            context.finalize()

    async def process_message(self, message: Message, client: MessageClient):
        """
        Override this with actual processing
        """
        raise NotImplementedError()  # actual processing here


class MessageProxyProcessor(MessageProcessorBase):
    async def process_message(self, message: Message, client: MessageClient):
        self._logger.warning('received a message addressed to me, though i\'m just a proxy. ignoring')
