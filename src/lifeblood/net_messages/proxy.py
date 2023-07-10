import asyncio
from lifeblood import logging


class CommandProxy:
    def __init__(self, host: str, port: int):
        self.__sched_connection = asyncio.open_connection(host, port)
        self.__queue = asyncio.Queue

    def queue
        self.__sched_connection.


class CommandProxyProtocol(asyncio.StreamReaderProtocol):
    def __init__(self, proxy: "CommandProxy", limit=2**16):
        self.__logger = logging.get_logger('scheduler')
        self.__timeout = 60.0
        self.__reader = asyncio.StreamReader(limit=limit)
        self.__proxy = proxy
        self.__saved_references = []
        super(CommandProxyProtocol, self).__init__(self.__reader, self.connection_cb)

    async def connection_cb(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        # there is a bug in py <=3.8, callback task can be GCd
        # see https://bugs.python.org/issue46309
        # so we HAVE to save a reference to self somewhere
        self.__saved_references.append(asyncio.current_task())

        try:
            pass
        finally:
            writer.close()
            await writer.wait_closed()
            # according to the note in the beginning of the function - now reference can be cleared
            self.__saved_references.remove(asyncio.current_task())
