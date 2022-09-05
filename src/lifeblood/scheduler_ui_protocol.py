import struct
import pickle
import json
import asyncio
from . import logging
from .uidata import NodeUi, ParameterLocked, ParameterReadonly
from .enums import NodeParameterType, TaskState, SpawnStatus, TaskGroupArchivedState
from . import pluginloader
from .net_classes import NodeTypeMetadata
from .taskspawn import NewTask
from .snippets import NodeSnippetDataPlaceholder
from .environment_resolver import EnvironmentResolverArguments

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
        self.__saved_references = []
        super(SchedulerUiProtocol, self).__init__(self.__reader, self.connection_cb)

    async def connection_cb(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        # there is a bug in py <=3.8, callback task can be GCd
        # see https://bugs.python.org/issue46309
        # so we HAVE to save a reference to self somewhere
        self.__saved_references.append(asyncio.current_task())

        #
        #
        # commands
        async def comm_get_full_state():  # if command == b'getfullstate':
            task_groups = []
            skip_dead, skip_archived_groups = struct.unpack('>??', await reader.readexactly(2))
            for i in range(struct.unpack('>I', await reader.readexactly(4))[0]):
                task_groups.append(await read_string())

            uidata = await self.__scheduler.get_full_ui_state(task_groups, skip_dead=skip_dead, skip_archived_groups=skip_archived_groups)
            uidata_ser = await uidata.serialize(compress=True)
            writer.write(struct.pack('>Q', len(uidata_ser)))
            writer.write(uidata_ser)

        async def comm_get_invoc_meta():  # elif command in (b'getinvocmeta', b'getlog', b'getalllog'):
            # if command == b'getinvocmeta':
            task_id = struct.unpack('>Q', await reader.readexactly(8))[0]
            all_meta = await self.__scheduler.get_invocation_metadata(task_id)
            data = await asyncio.get_event_loop().run_in_executor(None, pickle.dumps, all_meta)
            writer.write(struct.pack('>I', len(data)))
            writer.write(data)

        async def comm_get_log():
            # elif command == b'getlog':
            task_id, node_id, invocation_id = struct.unpack('>QQQ', await reader.readexactly(24))
            all_logs = await self.__scheduler.get_logs(task_id, node_id, invocation_id)
            data = await asyncio.get_event_loop().run_in_executor(None, pickle.dumps, all_logs)
            writer.write(struct.pack('>I', len(data)))
            writer.write(data)

        async def comm_get_log_all():
            # elif command == b'getalllog':
            # TODO: instead of getting all invocation logs first get invocation list
            # TODO: then bring in logs only for required invocation
            task_id, node_id = struct.unpack('>QQ', await reader.readexactly(16))
            all_logs = await self.__scheduler.get_logs(task_id, node_id)
            data = await asyncio.get_event_loop().run_in_executor(None, pickle.dumps, all_logs)
            writer.write(struct.pack('>I', len(data)))
            writer.write(data)

        # brings in interface data for one particular node
        async def comm_get_node_interface():  # elif command == b'getnodeinterface':
            node_id = struct.unpack('>Q', await reader.readexactly(8))[0]
            async with self.__scheduler.node_object_by_id_for_reading(node_id) as node:
                nodeui: NodeUi = node.get_ui()
                data: bytes = await nodeui.serialize_async()
            writer.write(struct.pack('>I', len(data)))
            writer.write(data)

        async def comm_get_task_attribs():  # elif command == b'gettaskattribs':
            task_id = struct.unpack('>Q', await reader.readexactly(8))[0]
            attribs, env_attribs = await self.__scheduler.get_task_attributes(task_id)

            data_attirbs: bytes = await asyncio.get_event_loop().run_in_executor(None, pickle.dumps, attribs)
            data_env: bytes = b''
            if env_attribs is not None:
                data_env: bytes = await EnvironmentResolverArguments.serialize_async(env_attribs)
            writer.write(struct.pack('>Q', len(data_attirbs)))
            writer.write(data_attirbs)
            writer.write(struct.pack('>Q', len(data_env)))
            writer.write(data_env)

        async def comm_get_task_invocation():  # elif command == b'gettaskinvoc':
            task_id = struct.unpack('>Q', await reader.readexactly(8))[0]
            data = await self.__scheduler.get_task_invocation_serialized(task_id)
            if data is None:
                writer.write(struct.pack('>Q', 0))
            else:
                writer.write(struct.pack('>Q', len(data)))
                writer.write(data)

        # node related commands
        async def comm_list_node_types():  # elif command == b'listnodetypes':
            typemetas = []
            for type_name, module in pluginloader.plugins.items():
                cls = module.node_class()
                typemetas.append(NodeTypeMetadata(cls))
            writer.write(struct.pack('>Q', len(typemetas)))
            for typemeta in typemetas:
                data: bytes = await asyncio.get_event_loop().run_in_executor(None, pickle.dumps, typemeta)
                writer.write(struct.pack('>Q', len(data)))
                writer.write(data)

        async def comm_list_presets():
            preset_metadata = {pack: {pres: NodeSnippetDataPlaceholder.from_nodesnippetdata(snip) for pres, snip in packdata.items()} for pack, packdata in pluginloader.presets.items()}
            data: bytes = await asyncio.get_event_loop().run_in_executor(None, pickle.dumps, preset_metadata)
            writer.write(struct.pack('>Q', len(data)))
            writer.write(data)

        async def comm_get_node_preset():
            package_name = await read_string()
            preset_name = await read_string()
            if package_name not in pluginloader.presets or preset_name not in pluginloader.presets[package_name]:
                self.__logger.warning(f'requested preset {package_name}::{preset_name} is not found')
                writer.write(struct.pack('>?', False))
                return
            writer.write(struct.pack('>?', True))
            data = pluginloader.presets[package_name][preset_name].serialize(ascii=False)
            writer.write(struct.pack('>Q', len(data)))
            writer.write(data)

        async def comm_remove_node():  # elif command == b'removenode':
            node_id = struct.unpack('>Q', await reader.readexactly(8))[0]
            await self.__scheduler.remove_node(node_id)
            writer.write(b'\1')

        async def comm_add_node():  # elif command == b'addnode':
            node_type = await read_string()
            node_name = await read_string()
            node_id = await self.__scheduler.add_node(node_type, node_name)
            writer.write(struct.pack('>Q', node_id))

        async def comm_wipe_node():  # elif command == b'wipenode':
            node_id = struct.unpack('>Q', await reader.readexactly(8))[0]
            await self.__scheduler.wipe_node_state(node_id)
            writer.write(b'\1')

        async def comm_node_has_param():  # elif command == b'nodehasparam':
            node_id = struct.unpack('>Q', await reader.readexactly(8))[0]
            param_name = await read_string()
            try:
                async with self.__scheduler.node_object_by_id_for_reading(node_id) as node:
                    if node.param(param_name) is None:
                        writer.write(b'\0')
                    else:
                        writer.write(b'\1')
            except Exception:
                self.__logger.warning(f'FAILED get node {node_id}')
                writer.write(b'\0')

        async def comm_set_node_param():  # elif command == b'setnodeparam':
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
            try:
                async with self.__scheduler.node_object_by_id_for_writing(node_id) as node:
                    await asyncio.get_event_loop().run_in_executor(None, node.set_param_value, param_name, param_value)
                    value = node.param(param_name).unexpanded_value()
            except ParameterReadonly:
                self.__logger.warning(f'failed request to set node {node_id} parameter "{param_name}"({NodeParameterType(param_type).name}). parameter is READ ONLY')
                writer.write(b'\0')
            except ParameterLocked:
                self.__logger.warning(f'failed request to set node {node_id} parameter "{param_name}"({NodeParameterType(param_type).name}). parameter is LOCKED')
                writer.write(b'\0')
            except Exception as e:
                err_val_prev = str(param_value)
                if len(err_val_prev) > 23:
                    err_val_prev = err_val_prev[:20] + '...'
                self.__logger.warning(f'FAILED request to set node {node_id} parameter "{param_name}"({NodeParameterType(param_type).name}) to {err_val_prev}')
                self.__logger.exception(e)
                writer.write(b'\0')
            else:
                writer.write(b'\1')
                #
                # send back actual result
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

        async def comm_set_node_param_expression():  # elif command == b'setnodeparamexpression':
            node_id, set_or_unset = struct.unpack('>Q?', await reader.readexactly(9))
            param_name = await read_string()
            async with self.__scheduler.node_object_by_id_for_writing(node_id) as node:
                if set_or_unset:
                    expression = await read_string()
                    await asyncio.get_event_loop().run_in_executor(None, node.param(param_name).set_expression, expression)
                    # node.param(param_name).set_expression(expression)
                else:
                    await asyncio.get_event_loop().run_in_executor(None, node.param(param_name).remove_expression)
            writer.write(b'\1')

        async def comm_apply_node_settings():
            node_id = struct.unpack('>Q', await reader.readexactly(8))[0]
            settings_name = await read_string()
            try:
                async with self.__scheduler.node_object_by_id_for_writing(node_id) as node:
                    await asyncio.get_event_loop().run_in_executor(None, node.apply_settings, settings_name)
            except Exception:
                self.__logger.exception(f'FAILED to apply node settings for node {node_id}, settings name "{settings_name}"')
                writer.write(b'\0')
            else:
                writer.write(b'\1')

        async def comm_save_custom_node_settings():
            node_type_name = await read_string()
            settings_name = await read_string()
            datasize = struct.unpack('>Q', await reader.readexactly(8))[0]

            settings = await asyncio.get_event_loop().run_in_executor(None, pickle.loads, await reader.readexactly(datasize))
            try:
                pluginloader.add_settings_to_existing_package('custom_default', node_type_name, settings_name, settings)
            except RuntimeError as e:
                self.__logger.error(f'failed to add custom node settings: {str(e)}')
                writer.write(b'\0')
            else:
                writer.write(b'\1')

        async def comm_set_settings_default():
            node_type_name = await read_string()
            is_setting = struct.unpack('>?', await reader.readexactly(1))[0]
            if is_setting:
                settings_name = await read_string()
            else:
                settings_name = None
            try:
                pluginloader.set_settings_as_default(node_type_name, settings_name)
            except RuntimeError:
                self.__logger.error(f'failed to set node default settings: {str(e)}')
                writer.write(b'\0')
            else:
                writer.write(b'\1')

        async def comm_rename_node():  # if command == b'renamenode':
            node_id = struct.unpack('>Q', await reader.readexactly(8))[0]
            node_name = await read_string()
            name = await self.__scheduler.set_node_name(node_id, node_name)
            await write_string(name)

        async def comm_duplicate_nodes():  # elif command == b'duplicatenodes':
            cnt = struct.unpack('>Q', await reader.readexactly(8))[0]
            node_ids = [struct.unpack('>Q', await reader.readexactly(8))[0] for _ in range(cnt)]
            try:
                res = await self.__scheduler.duplicate_nodes(node_ids)
            except Exception as e:
                writer.write(b'\0')
            else:
                writer.write(b'\1')
                writer.write(struct.pack('>Q', len(res)))
                for old_id, new_id in res.items():
                    writer.write(struct.pack('>QQ', old_id, new_id))

        # node connection related commands
        async def comm_change_connection():  # elif command == b'changeconnection':
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

        async def comm_add_connection():  # elif command == b'addconnection':
            id_out, id_in = struct.unpack('>QQ', await reader.readexactly(16))
            out_name = await read_string()
            in_name = await read_string()
            connection_id = await self.__scheduler.add_node_connection(id_out, out_name, id_in, in_name)
            writer.write(struct.pack('>Q', connection_id))

        async def comm_remove_connection():  # elif command == b'removeconnection':
            connection_id = struct.unpack('>Q', await reader.readexactly(8))[0]
            await self.__scheduler.remove_node_connection(connection_id)
            writer.write(b'\1')

        # task mod
        async def comm_pause_tasks():  # elif command == b'tpauselst':  # pause tasks
            task_ids = [-1]
            numtasks, paused, task_ids[0] = struct.unpack('>Q?Q', await reader.readexactly(17))  # there will be at least 1 task, cannot be zero
            if numtasks > 1:
                task_ids += struct.unpack('>' + 'Q' * (numtasks - 1), await reader.readexactly(8 * (numtasks - 1)))
            await self.__scheduler.set_task_paused(task_ids, bool(paused))
            writer.write(b'\1')

        async def comm_pause_task_group():  # elif command == b'tpausegrp':  # pause task group
            paused = struct.unpack('>?', await reader.readexactly(1))[0]
            task_group = await read_string()
            await self.__scheduler.set_task_paused(task_group, bool(paused))
            writer.write(b'\1')

        async def comm_archive_task_group():  # elif command == b'tarchivegrp':  # archive or unarchive a task group
            group_name = await read_string()
            state = TaskGroupArchivedState(struct.unpack('>I', await reader.readexactly(4))[0])
            await self.__scheduler.set_task_group_archived(group_name, state)
            writer.write(b'\1')

        async def comm_task_cancel():  # elif command == b'tcancel':  # cancel task invocation
            task_id = struct.unpack('>Q', await reader.readexactly(8))[0]
            await self.__scheduler.cancel_invocation_for_task(task_id)
            writer.write(b'\1')

        async def worker_task_cancel():  # workertaskcancel  # cancel invocation for worker
            worker_id = struct.unpack('>Q', await reader.readexactly(8))[0]
            await self.__scheduler.cancel_invocation_for_worker(worker_id)
            writer.write(b'\1')

        async def comm_task_set_node():  # elif command == b'tsetnode':  # set task node
            task_id, node_id = struct.unpack('>QQ', await reader.readexactly(16))
            await self.__scheduler.force_set_node_task(task_id, node_id)
            writer.write(b'\1')

        async def comm_task_change_state():  # elif command == b'tcstate':  # change task state
            task_ids = [-1]
            numtasks, state, task_ids[0] = struct.unpack('>QIQ', await reader.readexactly(20))  # there will be at least 1 task, cannot be zero
            if numtasks > 1:
                task_ids += struct.unpack('>' + 'Q' * (numtasks - 1), await reader.readexactly(8 * (numtasks - 1)))
            await self.__scheduler.force_change_task_state(task_ids, TaskState(state))
            writer.write(b'\1')

        async def comm_task_set_name():  # elif command == b'tsetname':  # change task name
            task_id = struct.unpack('>Q', await reader.readexactly(8))[0]
            new_name = await read_string()
            await self.__scheduler.set_task_name(task_id, new_name)
            writer.write(b'\1')

        async def comm_task_set_groups():  # elif command == b'tsetgroups':  # set task groups
            task_id, strcount = struct.unpack('>QQ', await reader.readexactly(16))
            task_groups = set()
            for _ in range(strcount):
                task_groups.add(await read_string())
            await self.__scheduler.set_task_groups(task_id, task_groups)
            writer.write(b'\1')

        async def comm_task_update_attribs():  # elif command == b'tupdateattribs':  # UPDATE task attribs, not set and override completely
            task_id, update_data_size, strcount = struct.unpack('>QQQ', await reader.readexactly(24))
            attribs_to_update = await asyncio.get_event_loop().run_in_executor(None, pickle.loads, await reader.readexactly(update_data_size))
            attribs_to_delete = set()
            for _ in range(strcount):
                attribs_to_delete.add(await read_string())
            await self.__scheduler.update_task_attributes(task_id, attribs_to_update, attribs_to_delete)
            writer.write(b'\1')

        # create new task
        async def comm_add_task():  # elif command == b'addtask':
            tasksize = struct.unpack('>Q', await reader.readexactly(8))[0]
            newtask: NewTask = NewTask.deserialize(await reader.readexactly(tasksize))
            ret: SpawnStatus = await self.__scheduler.spawn_tasks([newtask])
            writer.write(struct.pack('>I', ret.value))

        #
        async def set_task_environment_resolver_arguments():
            task_id, data_size = struct.unpack('>QQ', await reader.readexactly(16))
            if data_size == 0:
                env_res = None
            else:
                env_res = await EnvironmentResolverArguments.deserialize_async(await reader.readexactly(data_size))
            await self.__scheduler.set_task_environment_resolver_arguments(task_id, env_res)
            writer.write(b'\1')

        #
        async def set_worker_groups():
            hwid, groups_count = struct.unpack('>QQ', await reader.readexactly(16))
            groups = []
            for i in range(groups_count):
                groups.append(await read_string())

            await self.__scheduler.set_worker_groups(hwid, groups)
            writer.write(b'\1')

        #
        #
        commands = {b'getfullstate': comm_get_full_state,
                    b'getinvocmeta': comm_get_invoc_meta,
                    b'getlog': comm_get_log,
                    b'getalllog': comm_get_log_all,
                    b'getnodeinterface': comm_get_node_interface,
                    b'gettaskattribs': comm_get_task_attribs,
                    b'gettaskinvoc': comm_get_task_invocation,
                    b'listnodetypes': comm_list_node_types,
                    b'listnodepresets': comm_list_presets,
                    b'getnodepreset': comm_get_node_preset,
                    b'removenode': comm_remove_node,
                    b'addnode': comm_add_node,
                    b'wipenode': comm_wipe_node,
                    b'nodehasparam': comm_node_has_param,
                    b'setnodeparam': comm_set_node_param,
                    b'setnodeparamexpression': comm_set_node_param_expression,
                    b'applynodesettings': comm_apply_node_settings,
                    b'savecustomnodesettings': comm_save_custom_node_settings,
                    b'setsettingsdefault': comm_set_settings_default,
                    b'renamenode': comm_rename_node,
                    b'duplicatenodes': comm_duplicate_nodes,
                    b'changeconnection': comm_change_connection,
                    b'addconnection': comm_add_connection,
                    b'removeconnection': comm_remove_connection,
                    b'tpauselst': comm_pause_tasks,
                    b'tpausegrp': comm_pause_task_group,
                    b'tarchivegrp': comm_archive_task_group,
                    b'tcancel': comm_task_cancel,
                    b'workertaskcancel': worker_task_cancel,
                    b'tsetnode': comm_task_set_node,
                    b'tcstate': comm_task_change_state,
                    b'tsetname': comm_task_set_name,
                    b'tsetgroups': comm_task_set_groups,
                    b'tupdateattribs': comm_task_update_attribs,
                    b'addtask': comm_add_task,
                    b'settaskenvresolverargs': set_task_environment_resolver_arguments,
                    b'setworkergroups': set_worker_groups}
        #
        # connection callback
        #
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
            while True:  # TODO: there is SOO many commands now - that it's better to refactor them into a dict instead of infinite list of IFs
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

                if command in commands:
                    await commands[command]()
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
            # according to the note in the beginning of the function - now reference can be cleared
            self.__saved_references.remove(asyncio.current_task())
