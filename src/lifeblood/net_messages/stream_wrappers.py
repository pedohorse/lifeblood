import asyncio
import uuid
import struct
from io import BytesIO
from .messages import MessageInterface, Message
from .message_stream import MessageSendStreamBase, MessageReceiveStreamBase
from .enums import MessageType
from .address import AddressChain, DirectAddress
from .exceptions import MessageReceivingError, MessageSendingError, MessageTransferTimeoutError, NoMessageError
from .defaults import default_stream_timeout

from typing import Optional, Tuple, Union


def _write_header(stream: asyncio.StreamWriter, data_size: int, message_type: MessageType, source: AddressChain, destination: AddressChain, session: Optional[uuid.UUID]):
    src_data = source.encode('UTF-8')
    src_data_size = len(src_data)
    dest_data = destination.encode('UTF-8')
    dest_data_size = len(dest_data)
    has_session = session is not None
    stream.write(struct.pack('>QQQ?', data_size + 16 + src_data_size + dest_data_size + 1 + message_type.message_type_size() + (16 if has_session else 0),
                             src_data_size,
                             dest_data_size,
                             session is not None))
    stream.write(message_type.value)
    stream.write(src_data)
    stream.write(dest_data)
    if has_session:
        stream.write(session.bytes)


async def _read_header(stream: asyncio.StreamReader, timeout: float) -> Tuple[int, MessageType, AddressChain, AddressChain, Optional[uuid.UUID]]:
    return await asyncio.wait_for(__read_header(stream), timeout=timeout)


async def __read_header(stream: asyncio.StreamReader) -> Tuple[int, MessageType, AddressChain, AddressChain, Optional[uuid.UUID]]:
    """
    returns: (data_size, destination, session)
    """
    message_size, src_data_size, dest_data_size, has_session = struct.unpack('>QQQ?', await stream.readexactly(25))
    message_type = MessageType(await stream.readexactly(MessageType.message_type_size()))
    source = (await stream.readexactly(src_data_size)).decode('UTF-8')
    destination = (await stream.readexactly(dest_data_size)).decode('UTF-8')
    session = uuid.UUID(bytes=await stream.readexactly(16)) if has_session else None
    return message_size - 8-src_data_size - 8-dest_data_size - 1 - MessageType.message_type_size() - (16 if has_session else 0), \
           message_type, \
           AddressChain(source), \
           AddressChain(destination), \
           session


async def _from_stream_reader(stream: asyncio.StreamReader):
    data_size, message_type, source, destination, session = await _read_header(stream)
    data = await stream.readexactly(data_size)
    return Message(data, message_type, source, destination, session)


async def _serialize_to_stream_writer(message: Message, stream: asyncio.StreamWriter):
    _write_header(stream,
                  len(message.message_body()),
                  message.message_type(),
                  message.message_source(),
                  message.message_destination(),
                  message.message_session())
    stream.write(message.message_body())
    await stream.drain()


class WriterStreamRawMessageWrapper(MessageInterface):
    """
    wraps writes into "messages" that have predictable borders, that can be redirected easily
    """
    def __init__(self, writer: asyncio.StreamWriter, *, message_type: Optional[MessageType] = None, source: AddressChain, destination: AddressChain, session: Optional[uuid.UUID] = None):
        """
        Note: source and destination must be provided:
            while destination is basically where reader/writer are connected,
            source is where reply should come, so it's NOT current socket's address

        """
        self.__writer = writer
        self.__buffer: Optional[BytesIO] = None
        self.__size = 0
        self.__source: AddressChain = source
        self.__destination: AddressChain = destination
        self.__session = session
        self.__message_type: MessageType = message_type or MessageType.DEFAULT_MESSAGE
        self.__initialized = False

    def initialized(self):
        return self.__initialized

    async def __aenter__(self) -> "WriterStreamRawMessageWrapper":
        if self.__initialized:
            raise RuntimeError('message was already initialized')
        self.__initialized = True
        self.__buffer = BytesIO()
        self.__size = 0
        return self

    def write(self, data: bytes):
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        self.__size += len(data)
        self.__buffer.write(data)

    def write_string(self, s: str):
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        b = s.encode('UTF-8')
        self.write(struct.pack('>Q', len(b)))
        self.write(b)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        _write_header(self.__writer, self.__size, self.__message_type, self.__source, self.__destination, self.__session)
        self.__writer.write(self.__buffer.getbuffer())
        await self.__writer.drain()

    def message_size(self) -> int:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__size

    def message_body(self) -> Union[bytes, memoryview]:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__buffer.getbuffer()

    def message_destination(self) -> AddressChain:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__destination

    def message_source(self) -> AddressChain:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__source

    def message_session(self) -> Optional[uuid.UUID]:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__session

    def message_type(self) -> MessageType:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__message_type

    def to_message(self) -> Message:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return Message(self.message_body(), self.message_type(), self.message_source(), self.message_destination(), self.message_session())


class ReaderStreamRawMessageWrapper(MessageInterface):
    def __init__(self, reader: asyncio.StreamReader, session: Optional[uuid.UUID] = None, timeout: Optional[float] = None):
        self.__reader = reader
        self.__message_body_size: int = 0
        self.__already_read = 0
        self.__source: Optional[AddressChain] = None
        self.__destination: Optional[AddressChain] = None
        self.__message_type: Optional[MessageType] = None
        self.__initialized = False
        self.__buffer = BytesIO()
        self.__session = session
        self.__timeout = timeout

    def total_read_bytes(self) -> int:
        return self.__already_read

    def initialized(self):
        return self.__initialized

    def message_size(self):
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__message_body_size

    def message_body(self) -> Union[bytes, memoryview]:
        """
        message body so far. this ONLY guaranteed returns the whole message body after with block end
        """
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__buffer.getbuffer()

    def message_destination(self) -> AddressChain:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__destination

    def message_source(self) -> AddressChain:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__source

    def message_session(self) -> Optional[uuid.UUID]:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__session

    def message_type(self) -> MessageType:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return self.__message_type

    async def __aenter__(self) -> "ReaderStreamRawMessageWrapper":
        if self.__initialized:
            raise RuntimeError('message was already initialized')
        self.__initialized = True
        self.__message_body_size, self.__message_type, self.__source, self.__destination, session = await _read_header(self.__reader, timeout=self.__timeout)
        if self.__session is not None:  # if we expect specific session
            if session != self.__session:
                raise RuntimeError('received message has wrong session')
        else:
            self.__session = session
        self.__already_read = 0
        return self

    async def readexactly(self, size: int):
        if size < 0:
            return
        elif size == 0:
            return b''
        data = await asyncio.wait_for(self.__reader.readexactly(size), timeout=self.__timeout)
        self.__already_read += size
        self.__buffer.write(data)
        return data

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.readexactly(self.__message_body_size - self.__already_read)

    def to_message(self) -> Message:
        if not self.__initialized:
            raise RuntimeError('message was not initialized')
        return Message(self.message_body(), self.message_type(), self.message_source(), self.message_destination(), self.message_session())


class MessageSendStream(MessageSendStreamBase):
    def __init__(self,
                 reader_stream: asyncio.StreamReader,
                 writer_stream: asyncio.StreamWriter,
                 *,
                 reply_address: DirectAddress,
                 destination_address: DirectAddress,
                 stream_timeout: float = default_stream_timeout,
                 confirmation_timeout: float = default_stream_timeout):
        super().__init__(reply_address, destination_address, stream_timeout=stream_timeout, message_confirmation_timeout=confirmation_timeout)
        self.__reader = reader_stream
        self.__writer = writer_stream

    # def start_new_message(self, destination: AddressChain, session: uuid.UUID) -> WriterStreamRawMessageWrapper:
    #     return WriterStreamRawMessageWrapper(self.__writer, source=self.reply_address(), destination=destination, session=session)

    async def _send_message_implementation(self, message: Message, timeout: Optional[float]):
        """
        override this with actual message sending implementation
        """
        await _serialize_to_stream_writer(message, self.__writer)
        await self.__writer.drain()  # TODO: timeout not implemented

    async def _ack_message_implementation(self, message: Message, timeout: Optional[float]):
        try:
            if not struct.unpack('?', await asyncio.wait_for(self.__reader.readexactly(1),
                                                             timeout=timeout))[0]:
                raise MessageSendingError('message delivery failed', wrapped_exception=None)
        except asyncio.TimeoutError as e:
            raise MessageTransferTimeoutError(wrapped_exception=e) from None

    def close(self):
        if not self.__writer.is_closing():
            self.__writer.close()

    async def wait_closed(self):
        await self.__writer.wait_closed()


class MessageReceiveStream(MessageReceiveStreamBase):
    def __init__(self, reader_stream: asyncio.StreamReader, writer_stream: asyncio.StreamWriter, *, this_address: DirectAddress, other_end_address: DirectAddress, stream_timeout: float = default_stream_timeout):
        super().__init__(this_address, other_end_address, stream_timeout=stream_timeout)
        self.__reader = reader_stream
        self.__writer = writer_stream

    # def start_new_message(self, destination: AddressChain, session: uuid.UUID) -> WriterStreamRawMessageWrapper:
    #     return WriterStreamRawMessageWrapper(self.__writer, source=self.reply_address(), destination=destination, session=session)

    def __start_receiving_message(self):
        return ReaderStreamRawMessageWrapper(self.__reader, timeout=self._stream_timeout())

    async def _receive_message_implementation(self) -> Message:
        """
        override this with actual message receiving implementation
        """
        stream = None
        try:
            async with self.__start_receiving_message() as stream:
                pass  # will receive all
            return stream.to_message()
        except asyncio.TimeoutError as e:
            if stream is not None:
                if stream.total_read_bytes() == 0:
                    raise NoMessageError(wrapped_exception=e) from None
            raise MessageTransferTimeoutError(f'read {stream.total_read_bytes()}: {stream.message_body().decode("latin1")}', wrapped_exception=e) from None

    async def _acknowledge_message_implementation(self, status: bool):
        self.__writer.write(struct.pack('?', status))
        await self.__writer.drain()

    def close(self):
        self.__writer.close()

    async def wait_closed(self):
        await self.__writer.wait_closed()
