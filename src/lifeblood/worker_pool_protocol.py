import asyncio
import struct
from .logging import get_logger
from .enums import WorkerState

from typing import TYPE_CHECKING, Optional
if TYPE_CHECKING:
    from .simple_worker_pool import WorkerPool


class WorkerPoolProtocol(asyncio.StreamReaderProtocol):
    def __init__(self, worker_pool: "WorkerPool", limit=2 ** 16, logger=None):
        self.__logger = logger or get_logger(self.__class__.__name__.lower())
        self.__timeout = 60
        self.__worker_pool = worker_pool
        self.__reader = asyncio.StreamReader(limit=limit)
        self.__saved_references = []
        super(WorkerPoolProtocol, self).__init__(self.__reader, self.connection_cb)

    async def connection_cb(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        # there is a bug in py <=3.8, callback task can be GCd
        # see https://bugs.python.org/issue46309
        # so we HAVE to save a reference to self somewhere
        self.__saved_references.append(asyncio.current_task())

        try:
            prot = await asyncio.wait_for(reader.readexactly(4), self.__timeout)
            if prot != b'\0\0\0\0':
                raise NotImplementedError()

            worker_id = struct.unpack('>Q', await asyncio.wait_for(reader.readexactly(8), self.__timeout))[0]

            while True:
                command = await asyncio.wait_for(reader.readline(), timeout=self.__timeout)  # type: bytes
                if command.endswith(b'\n'):
                    command = command[:-1]
                self.__logger.debug(f'got command {command}')
                if command == b'state_report':
                    state = WorkerState(struct.unpack('>Q', await reader.readexactly(8))[0])
                    await self.__worker_pool._worker_state_change(worker_id, state)
                    writer.write(b'\1')
                elif reader.at_eof():
                    self.__logger.debug('connection closed')
                    return
                else:
                    raise NotImplementedError(f'{command} command is not implemented')
                await writer.drain()

        except asyncio.exceptions.TimeoutError as e:
            self.__logger.error('connection timeout')
        except ConnectionResetError as e:
            self.__logger.exception('connection was reset. disconnected %s', e)
        except ConnectionError as e:
            self.__logger.exception('connection error. disconnected %s', e)
        except Exception as e:
            self.__logger.exception('unknown error. disconnected %s', e)
            raise
        finally:
            writer.close()
            await writer.wait_closed()
            # according to the note in the beginning of the function - now reference can be cleared
            self.__saved_references.remove(asyncio.current_task())

    # def connection_lost(self, exc):
    #     self.__
    #     super(WorkerPoolProtocol, self).connection_lost(exc)


class WorkerPoolClient:
    async def write_string(self, s: str):
        b = s.encode('UTF-8')
        self.__writer.write(struct.pack('>Q', len(b)))
        self.__writer.write(b)

    def __init__(self, ip: str, port: int, worker_id: int):
        self.__logger = get_logger('workerpoolclient')
        self.__conn_task = asyncio.create_task(asyncio.open_connection(ip, port))
        self.__worker_id = worker_id
        self.__reader: Optional[asyncio.StreamReader] = None
        self.__writer: Optional[asyncio.StreamWriter] = None

    async def __aenter__(self) -> "WorkerPoolClient":
        await self._ensure_conn_open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        self.__writer.close()
        await self.__writer.wait_closed()

    async def _ensure_conn_open(self):
        if self.__reader is not None:
            return
        self.__reader, self.__writer = await self.__conn_task
        self.__writer.write(b'\0\0\0\0')
        self.__writer.write(struct.pack('>Q', self.__worker_id))

    async def report_state(self, state: WorkerState):
        self.__writer.write(b'state_report\n')
        self.__writer.write(struct.pack('>Q', state.value))
        await self.__writer.drain()
        assert await self.__reader.readexactly(1) == b'\1'
