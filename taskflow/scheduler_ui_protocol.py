import struct
import pickle
import asyncio
from .uidata import NodeUi
from .enums import NodeParameterType

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .basenode import BaseNode

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
                # get full nodegraph state. only brings in where is which item, no other details
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
                # brings in interface data for one particular node
                elif command == b'getnodeinterface':
                    node_id = struct.unpack('>Q', await reader.readexactly(8))[0]
                    nodeui: NodeUi = (await self.__scheduler.get_node_object_by_id(node_id)).get_ui()
                    data: bytes = await nodeui.serialize_async()
                    writer.write(struct.pack('>I', len(data)))
                    writer.write(data)
                elif command == b'gettaskattribs':
                    task_id = struct.unpack('>Q', await reader.readexactly(8))[0]
                    attribs = await self.__scheduler.get_task_attributes(task_id)
                    data: bytes = await asyncio.get_event_loop().run_in_executor(None, pickle.dumps, attribs)
                    writer.write(struct.pack('>I', len(data)))
                    writer.write(data)
                elif command == b'setnodeparam':
                    node_id, param_type, param_name_data_length = struct.unpack('>QII', await reader.readexactly(16))
                    param_name = (await reader.readexactly(param_name_data_length)).decode('UTF-8')
                    if param_type == NodeParameterType.FLOAT.value:
                        param_value = struct.unpack('>d', await reader.readexactly(8))[0]
                    elif param_type == NodeParameterType.INT.value:
                        param_value = struct.unpack('>q', await reader.readexactly(8))[0]
                    elif param_type == NodeParameterType.BOOL.value:
                        param_value = struct.unpack('>?', await reader.readexactly(1))[0]
                    elif param_type == NodeParameterType.STRING.value:
                        param_str_data_length = struct.unpack('>Q', await reader.readexactly(8))[0]
                        param_value = (await reader.readexactly(param_str_data_length)).decode('UTF-8')
                    else:
                        raise NotImplementedError()
                    node: BaseNode = await self.__scheduler.get_node_object_by_id(node_id)
                    node.set_param_value(param_name, param_value)
                elif command == b'changeconnection':
                    connection_id, change_out, change_in, new_id_out, new_id_in = struct.unpack('>Q??QQ', await reader.readexactly(26))
                    in_name, out_name = None, None
                    if change_out:
                        out_name_len = struct.unpack('>I', await reader.readexactly(4))[0]
                        out_name = (await reader.readexactly(out_name_len)).decode('UTF-8')
                    if change_in:
                        in_name_len = struct.unpack('>I', await reader.readexactly(4))[0]
                        in_name = (await reader.readexactly(out_name_len)).decode('UTF-8')
                    raise NotImplementedError()  # TODO: finish this
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
