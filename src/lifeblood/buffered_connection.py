import io
import socket
import struct
from .exceptions import IncompleteReadError

from typing import Tuple


class BufferedConnection:
    def __init__(self, address, buffering=None, timeout=30):
        """

        :param address:
        :param buffering: None means default buffer size, 0 means unbuffered
        :return:
        """
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__sock.settimeout(timeout)
        self.__sock.connect(address)

        if buffering is None:
            buffering = -1
        if buffering < 0:
            buffering = io.DEFAULT_BUFFER_SIZE

        reader = self.__sock.makefile('rb', buffering)
        writer = self.__sock.makefile('wb', buffering)
        assert isinstance(reader, io.BufferedReader)
        assert isinstance(writer, io.BufferedWriter)
        self.__reader = BufferedReaderWrapper(reader, buffering)
        self.__writer = BufferedWriterWrapper(writer, buffering)

    def close(self):
        self.__writer.flush()
        self.__writer.close()
        self.__reader.close()
        self.__sock.close()

    @property
    def reader(self) -> "BufferedReaderWrapper":
        return self.__reader

    @property
    def writer(self) -> "BufferedWriterWrapper":
        return self.__writer

    def get_rw_pair(self) -> Tuple["BufferedReaderWrapper", "BufferedWriterWrapper"]:
        return self.__reader, self.__writer


class BufferedReader(io.BufferedReader):
    def readexactly(self, size) -> bytes:
        # a tiny optimization: first try to read with a single call, and then employ general tactics if that fails
        part = self.read(size)
        lpart = len(part)
        if lpart == 0:
            raise IncompleteReadError(f'wanted to read {size}, but got EOF')
        if lpart == size:
            return part

        # and now generic algorithm
        parts = [part]
        total_read = lpart
        while total_read < size:
            part = self.read(size)
            lpart = len(part)
            if lpart == 0:  # meaning EOF
                raise IncompleteReadError(f'wanted to read {size}, but got only {total_read} before EOF')
            total_read += lpart
            parts.append(part)
        return b''.join(parts)


class BufferedWriter(io.BufferedWriter):
    pass


class BufferedReaderWrapper(BufferedReader):
    def __init__(self, stuff_to_wrap: io.BufferedReader, buffering=None):
        if buffering is None:
            buffering = -1
        if buffering < 0:
            buffering = io.DEFAULT_BUFFER_SIZE
        super().__init__(stuff_to_wrap.raw, buffering)
        self.__wrapped = stuff_to_wrap

    def read_string(self):
        data_len, = struct.unpack('>Q', self.readexactly(8))
        if data_len == 0:
            return ''
        return self.readexactly(data_len).decode('UTF-8')

    def close(self) -> None:
        super().close()
        self.__wrapped.close()


class BufferedWriterWrapper(BufferedWriter):
    def __init__(self, stuff_to_wrap: io.BufferedWriter, buffering=None):
        super().__init__(stuff_to_wrap.raw, buffering)
        self.__wrapped = stuff_to_wrap

    def write_string(self, text: str):
        data = text.encode('UTF-8')
        self.write(struct.pack('>Q', len(data)))
        self.write(data)

    def close(self) -> None:
        super().close()
        self.__wrapped.close()
