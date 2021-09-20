import struct
import pickle
import asyncio
from . import logging
from .uidata import NodeUi
from .enums import NodeParameterType, TaskState, SpawnStatus
from . import pluginloader
from .net_classes import NodeTypeMetadata
from .taskspawn import NewTask

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .basenode import BaseNode
    from .scheduler import Scheduler


class SchedulerUiProtocol(asyncio.StreamReaderProtocol):
    def __init__(self, scheduler):
        self.__logger = logging.get_logger('scheduler.uiprotocol')
        self.__scheduler: "Scheduler" = scheduler
        self.__reader = asyncio.StreamReader()
        self.__timeout = 60.0
        super(SchedulerUiProtocol, self).__init__(self.__reader, self.connection_cb)

    async def connection_cb(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.__logger.debug('UI connected')

        async def read_string() -> str:
            strlen = struct.unpack('>Q', await reader.readexactly(8))[0]
            return (await reader.readexactly(strlen)).decode('UTF-8')

        async def write_string(s: str):
            b = s.encode('UTF-8')
            writer.write(struct.pack('>Q', len(b)))
            writer.write(b)

        try:
            proto = await asyncio.wait_for(reader.readexactly(4), timeout=self.__timeout)
            if proto != b'\0\0\0\0':
                raise NotImplementedError(f'protocol version unsupported {proto}')

            wait_stop_task = asyncio.create_task(self.__scheduler._stop_event_wait())
            while True:
                readline_task = asyncio.create_task(reader.readline())
                done, pending = await asyncio.wait((readline_task, wait_stop_task), return_when=asyncio.FIRST_COMPLETED)
                if wait_stop_task in done:
                    self.__logger.debug(f'scheduler is stopping: closing ui connections')
                    readline_task.cancel()
                    return
                assert readline_task.done()
                command: bytes = await readline_task
                if command.endswith(b'\n'):
                    command = command[:-1]
                self.__logger.debug(f'got command {command}')
                # get full nodegraph state. only brings in where is which item, no other details
                if command == b'getfullstate':
                    task_groups = []
                    for i in range(struct.unpack('>I', await reader.readexactly(4))[0]):
                        task_groups.append(await read_string())

                    uidata = await self.__scheduler.get_full_ui_state(task_groups)
                    uidata_ser = await uidata.serialize(compress=True)
                    writer.write(struct.pack('>Q', len(uidata_ser)))
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
                    writer.write(struct.pack('>Q', len(data)))
                    writer.write(data)
                elif command == b'gettaskinvoc':
                    task_id = struct.unpack('>Q', await reader.readexactly(8))[0]
                    data = await self.__scheduler.get_task_invocation_serialized(task_id)
                    if data is None:
                        writer.write(struct.pack('>Q', 0))
                    else:
                        writer.write(struct.pack('>Q', len(data)))
                        writer.write(data)
                #
                # node related commands
                elif command == b'listnodetypes':
                    typemetas = []
                    for type_name, module in pluginloader.plugins.items():
                        cls = module.node_class()
                        typemetas.append(NodeTypeMetadata(cls))
                    writer.write(struct.pack('>Q', len(typemetas)))
                    for typemeta in typemetas:
                        data: bytes = await asyncio.get_event_loop().run_in_executor(None, pickle.dumps, typemeta)
                        writer.write(struct.pack('>Q', len(data)))
                        writer.write(data)
                elif command == b'removenode':
                    node_id = struct.unpack('>Q', await reader.readexactly(8))[0]
                    await self.__scheduler.remove_node(node_id)
                    writer.write(b'\1')
                elif command == b'addnode':
                    node_type = await read_string()
                    node_name = await read_string()
                    node_id = await self.__scheduler.add_node(node_type, node_name)
                    writer.write(struct.pack('>Q', node_id))
                elif command == b'wipenode':
                    node_id = struct.unpack('>Q', await reader.readexactly(8))[0]
                    await self.__scheduler.wipe_node_state(node_id)
                    writer.write(b'\1')
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
                        param_value = await read_string()
                    else:
                        raise NotImplementedError()
                    node: BaseNode = await self.__scheduler.get_node_object_by_id(node_id)
                    node.set_param_value(param_name, param_value)
                    #
                    # send back actual result
                    value = node.param(param_name).unexpanded_value()
                    if param_type == NodeParameterType.FLOAT.value:
                        writer.write(struct.pack('>d', value))
                    elif param_type == NodeParameterType.INT.value:
                        writer.write(struct.pack('>q', value))
                    elif param_type == NodeParameterType.BOOL.value:
                        writer.write(struct.pack('>?', value))
                    elif param_type == NodeParameterType.STRING.value:
                        await write_string(value)
                    else:
                        raise NotImplementedError()
                elif command == b'setnodeparamexpression':
                    node_id, set_or_unset = struct.unpack('>Q?', await reader.readexactly(9))
                    param_name = await read_string()
                    node: BaseNode = await self.__scheduler.get_node_object_by_id(node_id)
                    if set_or_unset:
                        expression = await read_string()
                        node.param(param_name).set_expression(expression)
                    else:
                        node.param(param_name).remove_expression()
                    writer.write(b'\1')
                elif command == b'renamenode':
                    node_id = struct.unpack('>Q', await reader.readexactly(8))[0]
                    node_name = await read_string()
                    name = await self.__scheduler.set_node_name(node_id, node_name)
                    await write_string(name)
                #
                # node connection related commands
                elif command == b'changeconnection':
                    connection_id, change_out, change_in, new_id_out, new_id_in = struct.unpack('>Q??QQ', await reader.readexactly(26))
                    in_name, out_name = None, None
                    if change_out:
                        out_name = await read_string()
                    else:
                        new_id_out = None
                    if change_in:
                        in_name = await read_string()
                    else:
                        new_id_in = None
                    await self.__scheduler.change_node_connection(connection_id, new_id_out, out_name, new_id_in, in_name)
                    writer.write(b'\1')
                elif command == b'addconnection':
                    id_out, id_in = struct.unpack('>QQ', await reader.readexactly(16))
                    out_name = await read_string()
                    in_name = await read_string()
                    connection_id = await self.__scheduler.add_node_connection(id_out, out_name, id_in, in_name)
                    writer.write(struct.pack('>Q', connection_id))
                elif command == b'removeconnection':
                    connection_id = struct.unpack('>Q', await reader.readexactly(8))[0]
                    await self.__scheduler.remove_node_connection(connection_id)
                    writer.write(b'\1')
                elif command == b'tpauselst':  # pause tasks
                    task_ids = [-1]
                    numtasks, paused, task_ids[0] = struct.unpack('>Q?Q', await reader.readexactly(17))  # there will be at least 1 task, cannot be zero
                    if numtasks > 1:
                        task_ids += struct.unpack('>' + 'Q'*(numtasks-1), await reader.readexactly(8*(numtasks-1)))
                    await self.__scheduler.set_task_paused(task_ids, bool(paused))
                    writer.write(b'\1')
                elif command == b'tpausegrp':  # pause task group
                    paused = struct.unpack('>?', await reader.readexactly(1))[0]
                    task_group = await read_string()
                    await self.__scheduler.set_task_paused(task_group, bool(paused))
                    writer.write(b'\1')
                elif command == b'tcancel':  # cancel task invocation
                    task_id = struct.unpack('>Q', await reader.readexactly(8))[0]
                    await self.__scheduler.cancel_invocation_for_task(task_id)
                    writer.write(b'\1')
                elif command == b'tsetnode':  # set task node
                    task_id, node_id = struct.unpack('>QQ', await reader.readexactly(16))
                    await self.__scheduler.force_set_node_task(task_id, node_id)
                    writer.write(b'\1')
                elif command == b'tcstate':  # change task state
                    task_ids = [-1]
                    numtasks, state, task_ids[0] = struct.unpack('>QIQ', await reader.readexactly(20))  # there will be at least 1 task, cannot be zero
                    if numtasks > 1:
                        task_ids += struct.unpack('>' + 'Q' * (numtasks - 1), await reader.readexactly(8 * (numtasks - 1)))
                    await self.__scheduler.force_change_task_state(task_ids, TaskState(state))
                    writer.write(b'\1')
                # create new task
                elif command == b'addtask':
                    tasksize = struct.unpack('>Q', await reader.readexactly(8))[0]
                    newtask: NewTask = NewTask.deserialize(await reader.readexactly(tasksize))
                    ret: SpawnStatus = await self.__scheduler.spawn_tasks([newtask])
                    writer.write(struct.pack('>I', ret.value))
                #
                # if conn is closed - result will be b'', but in mostl likely totally impossible case it can be unfinished command.
                # so lets just catch all
                elif reader.at_eof():
                    self.__logger.debug('UI disconnected')
                    return
                else:
                    raise NotImplementedError()

                await writer.drain()
        except ConnectionResetError as e:
            self.__logger.warning('connection was reset. UI disconnected %s', e)
        except ConnectionError as e:
            self.__logger.error('connection error. UI disconnected %s', e)
        except Exception as e:
            self.__logger.exception('unknown error. UI disconnected %s', e)
            raise
        finally:
            writer.close()
            await writer.wait_closed()
