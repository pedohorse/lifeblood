import struct
import pickle
import asyncio


class SchedulerUiProtocol(asyncio.StreamReaderProtocol):
    def __init__(self, scheduler):
        self.__scheduler = scheduler
        self.__reader = asyncio.StreamReader()
        super(SchedulerUiProtocol, self).__init__(self.__reader, self.connection_cb)

    async def connection_cb(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        print('UI connected')
        try:
            proto = await reader.readexactly(4)
            if proto != b'\0\0\0\0':
                raise NotImplementedError(f'protocol version unsupported {proto}')

            while True:
                command: bytes = await reader.readline()
                if command.endswith(b'\n'):
                    command = command[:-1]
                print(f'got command {command}')
                if command == b'getfullstate':
                    uidata = await self.__scheduler.get_full_ui_state()
                    uidata_ser = await uidata.serialize()
                    writer.write(struct.pack('>I', len(uidata_ser)))
                    writer.write(uidata_ser)
                elif command in (b'getlogmeta', b'getlog', b'getalllog'):
                    if command == b'getlogmeta':
                        task_id = struct.unpack('>Q', await reader.readexactly(8))[0]
                        all_logs = await self.__scheduler.get_log_metadata(task_id)
                    elif command == b'getlog':
                        task_id, node_id, invocation_id = struct.unpack('>QQQ', await reader.readexactly(24))
                        all_logs = await self.__scheduler.get_logs(task_id, node_id, invocation_id)
                    elif command == b'getalllog':
                        # TODO: instead of getting all invocation logs first get invocation list
                        # TODO: then bring in logs only for required invocation
                        task_id, node_id = struct.unpack('>QQ', await reader.readexactly(16))
                        all_logs = await self.__scheduler.get_logs(task_id, node_id)
                    else:
                        raise RuntimeError('this error is impossible!')
                    data = await asyncio.get_event_loop().run_in_executor(None, pickle.dumps, all_logs)
                    writer.write(struct.pack('>I', len(data)))
                    writer.write(data)
                # if conn is closed - result will be b'', but in mostl likely totally impossible case it can be unfinished command.
                # so lets just catch all
                elif reader.at_eof():
                    print('UI disconnected')
                    return
                else:
                    raise NotImplementedError()

                await writer.drain()
        except ConnectionResetError as e:
            print('connection was reset. UI disconnected', e)
        except ConnectionError as e:
            print('connection error. UI disconnected', e)
        except Exception as e:
            print('unknown error. UI disconnected', e)
            raise
