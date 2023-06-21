import struct
import pickle
import json
import asyncio
from asyncio.exceptions import IncompleteReadError
from . import logging
from .uidata import NodeUi, Parameter, ParameterLocked, ParameterReadonly, ParameterNotFound, ParameterCannotHaveExpressions
from .ui_protocol_data import NodeGraphStructureData, TaskGroupBatchData, TaskBatchData, WorkerBatchData, UiData, InvocationLogData, IncompleteInvocationLogData
from .ui_events import TaskEvent
from .enums import NodeParameterType, TaskState, SpawnStatus, TaskGroupArchivedState
from .exceptions import NotSubscribedError
from . import pluginloader
from .invocationjob import InvocationJob
from .net_classes import NodeTypeMetadata
from .taskspawn import NewTask
from .snippets import NodeSnippetData, NodeSnippetDataPlaceholder
from .environment_resolver import EnvironmentResolverArguments
from .buffered_connection import BufferedConnection

from typing import Any, Dict, Iterable, TYPE_CHECKING, Optional, Tuple, List, Union
if TYPE_CHECKING:
    from .basenode import BaseNode
    from .scheduler import Scheduler


def _serialize_json_dict(d: dict) -> bytes:
    return json.dumps(d).encode('UTF-8')


def _deserialize_json_dict(data: bytes) -> dict:
    return json.loads(data.decode('UTF-8'))


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
        # async def comm_get_full_state():  # if command == b'getfullstate':
        #     task_groups = []
        #     skip_dead, skip_archived_groups = struct.unpack('>??', await reader.readexactly(2))
        #     for i in range(struct.unpack('>I', await reader.readexactly(4))[0]):
        #         task_groups.append(await read_string())
        #
        #     uidata = await self.__scheduler.ui_state_access.get_full_ui_state(task_groups, skip_dead=skip_dead, skip_archived_groups=skip_archived_groups)
        #     await uidata.serialize_to_streamwriter(writer)

        async def comm_get_db_uid():
            writer.write(struct.pack('>Q', self.__scheduler.db_uid()))

        async def comm_get_ui_graph_state_update_id():  # get_ui_graph_state_update_id
            writer.write(struct.pack('>Q', self.__scheduler.ui_state_access.graph_update_id))

        async def comm_get_ui_graph_state():  # get_ui_graph_state
            state = await self.__scheduler.ui_state_access.get_nodes_ui_state()
            update_id = self.__scheduler.ui_state_access.graph_update_id  # there should be no possibility for race condition between aquiring of state and update_id, cus async, but not multithreaded
            writer.write(struct.pack('>Q', update_id))
            await state.serialize_to_streamwriter(writer)

        async def comm_get_ui_task_groups():  # get_ui_task_groups
            skip_archived_groups, = struct.unpack('>?', await reader.readexactly(1))
            state = await self.__scheduler.ui_state_access.get_task_groups_ui_state(fetch_statistics=True, skip_archived_groups=skip_archived_groups)
            await state.serialize_to_streamwriter(writer)

        async def comm_get_ui_tasks_state():  # get_ui_tasks_state
            include_dead, num_groups = struct.unpack('>?Q', await reader.readexactly(9))
            groups = []
            for _ in range(num_groups):
                groups.append(await read_string())
            state = await self.__scheduler.ui_state_access.get_tasks_ui_state(groups, not include_dead)
            await state.serialize_to_streamwriter(writer)

        # ui events

        async def comm_request_subscribe_to_task_events():  # request_task_events
            include_dead, num_groups, logging_time = struct.unpack('>?Qd', await reader.readexactly(17))
            groups = []
            for _ in range(num_groups):
                groups.append(await read_string())
            events = await self.__scheduler.ui_state_access.subscribe_to_task_events_for_groups(groups, not include_dead, logging_time)
            writer.write(struct.pack('>Q', len(events)))
            for event in events:
                await event.serialize_to_streamwriter(writer)

        async def comm_request_task_events_since_id():  # task_events_since_id
            include_dead, num_groups, last_known_event_id = struct.unpack('>?QQ', await reader.readexactly(17))
            groups = []
            for _ in range(num_groups):
                groups.append(await read_string())
            try:
                events = await self.__scheduler.ui_state_access.get_events_for_groups_since_event_id(groups, not include_dead, last_known_event_id)
            except NotSubscribedError:
                writer.write(struct.pack('>?', False))
                return

            writer.write(struct.pack('>?Q', True, len(events)))
            for event in events:
                await event.serialize_to_streamwriter(writer)

        # end of ui events

        async def comm_get_ui_workers_state():  # get_ui_workers_state
            state = await self.__scheduler.ui_state_access.get_workers_ui_state()
            await state.serialize_to_streamwriter(writer)

        async def comm_get_invoc_meta():  # elif command in (b'getinvocmeta', b'getlog', b'getalllog'):
            # if command == b'getinvocmeta':
            task_id = struct.unpack('>Q', await reader.readexactly(8))[0]
            all_meta = await self.__scheduler.get_invocation_metadata(task_id)
            writer.write(struct.pack('>Q', len(all_meta)))
            for node_id, invoc_list in all_meta.items():
                writer.write(struct.pack('>QQ', node_id, len(invoc_list)))
                for log in invoc_list:
                    await log.serialize_to_streamwriter(writer)

        async def comm_get_log():
            # elif command == b'getlog':
            invocation_id, = struct.unpack('>Q', await reader.readexactly(8))
            log = await self.__scheduler.get_log(invocation_id)
            writer.write(struct.pack('>?', log is not None))
            if log is not None:
                await log.serialize_to_streamwriter(writer)

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

            data_attirbs: bytes = await asyncio.get_event_loop().run_in_executor(None, _serialize_json_dict, attribs)
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
            preset_metadata: Dict[str, Dict[str, NodeSnippetDataPlaceholder]] = {pack: {pres: NodeSnippetDataPlaceholder.from_nodesnippetdata(snip) for pres, snip in packdata.items()} for pack, packdata in pluginloader.presets.items()}
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
            deleted = await self.__scheduler.remove_node(node_id)
            writer.write(b'\1' if deleted else b'\0')

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
            node_id, param_type, has_expression = struct.unpack('>QI?', await reader.readexactly(13))
            param_name = await read_string()
            param_expr = None
            if has_expression:
                param_expr = await read_string()
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
                async with self.__scheduler.node_object_by_id_for_writing(node_id) as node:  # type: BaseNode
                    param = await asyncio.get_event_loop().run_in_executor(None, node.param, param_name)
                    # first set expression
                    if param.has_expression() and not has_expression:
                        await asyncio.get_event_loop().run_in_executor(None, param.remove_expression)
                    elif has_expression:
                        assert param_expr is not None
                        await asyncio.get_event_loop().run_in_executor(None, param.set_expression, param_expr)
                    # TODO: if error happens below - we'll end up with half set parameter... not nice
                    # then set value (value can be set independent, remember)
                    await asyncio.get_event_loop().run_in_executor(None, param.set_value, param_value)
                    value = param.unexpanded_value()
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

        async def comm_set_node_params():  # elif command == b'batchsetnodeparams':
            node_id, param_count, want_result = struct.unpack('>QQ?', await reader.readexactly(17))

            stuff_to_set: List[Tuple[str, NodeParameterType, bool, Union[float, int, bool, str]]] = []
            return_order: List[str] = []
            for _ in range(param_count):
                param_name = await read_string()
                param_type_raw, value_is_expression = struct.unpack('>I?', await reader.readexactly(5))
                param_type_raw: int
                value_is_expression: bool

                if value_is_expression:
                    param_value = await read_string()
                elif param_type_raw == NodeParameterType.FLOAT.value:
                    param_value = struct.unpack('>d', await reader.readexactly(8))[0]
                elif param_type_raw == NodeParameterType.INT.value:
                    param_value = struct.unpack('>q', await reader.readexactly(8))[0]
                elif param_type_raw == NodeParameterType.BOOL.value:
                    param_value = struct.unpack('>?', await reader.readexactly(1))[0]
                elif param_type_raw == NodeParameterType.STRING.value:
                    param_value = await read_string()
                else:
                    raise NotImplementedError()
                stuff_to_set.append((param_name, NodeParameterType(param_type_raw), value_is_expression, param_value))
                return_order.append(param_name)

            # now it is possible that some parameters won't exist before other parameters are set
            #  for example: elements of multigroup won't be available until multigroup's count is set
            #  also multigroup params may be nested, so it may be complicated to untangle beforehand
            #  but there may be other
            return_values: Dict[str, Tuple[bool, Optional[Tuple[NodeParameterType, Union[float, int, bool, str]]]]] = {}  # list of param_names to [set success?, final value]
            async with self.__scheduler.node_object_by_id_for_writing(node_id) as node:
                node: BaseNode
                cycle_start = None
                something_was_set_last_loop = False
                for param_name, param_type, value_is_expression, param_value in stuff_to_set:
                    try:
                        await asyncio.get_event_loop().run_in_executor(None, node.param, param_name)
                    except ParameterNotFound:
                        # some parameters may only be present after other parameters are set.
                        #  for example - multigroup element will appear only after count is set
                        #  so we postpone setting missing parameters
                        #  and only consider it a failure if no more parameters can be found at all
                        if cycle_start is None or cycle_start == param_name and something_was_set_last_loop:
                            cycle_start = param_name
                            something_was_set_last_loop = False
                            stuff_to_set.append((param_name, param_type, value_is_expression, param_value))  # put back
                        else:  # means we already checked all parameters since last successful set and set nothing new
                            self.__logger.warning(f'failed request to set node {node_id} parameter "{param_name}" - parameter not found')
                            return_values[param_name] = (False, None)
                        continue
                    try:
                        def _set_helper(node: "BaseNode", param_name: str, value_is_expression: bool, value: Union[float, int, bool, str]):
                            param = node.param(param_name)
                            if value_is_expression:
                                param.set_expression(param_value)
                            else:
                                if param.has_expression():
                                    param.set_expression(None)
                                param.set_value(value)

                        await asyncio.get_event_loop().run_in_executor(None, _set_helper, node, param_name, value_is_expression, param_value)
                        something_was_set_last_loop = True
                        value = node.param(param_name).unexpanded_value()
                    except ParameterReadonly:
                        self.__logger.warning(f'failed request to set node {node_id} parameter "{param_name}"({NodeParameterType(param_type).name}). parameter is READ ONLY')
                        return_values[param_name] = (False, None)
                    except ParameterLocked:
                        self.__logger.warning(f'failed request to set node {node_id} parameter "{param_name}"({NodeParameterType(param_type).name}). parameter is LOCKED')
                        return_values[param_name] = (False, None)
                    except ParameterCannotHaveExpressions:
                        self.__logger.warning(f'failed request to set node {node_id} parameter "{param_name}" expression. parameter cannot have expressions')
                        return_values[param_name] = (False, None)
                    except Exception as e:
                        err_val_prev = str(param_value)
                        if len(err_val_prev) > 23:
                            err_val_prev = err_val_prev[:20] + '...'
                        self.__logger.warning(f'FAILED request to set node {node_id} parameter "{param_name}"({NodeParameterType(param_type).name}) to {err_val_prev}')
                        self.__logger.exception(e)
                        return_values[param_name] = (False, None)
                    else:  # all went well, parameter was set
                        return_values[param_name] = (True, (param_type, value))

            if not want_result:
                writer.write(b'\1')
            else:
                for param_name in return_order:
                    was_set, other = return_values[param_name]
                    if not was_set:
                        writer.write(b'\0')
                        continue
                    writer.write(b'\1')
                    param_type, value = other
                    if param_type == NodeParameterType.FLOAT:
                        writer.write(struct.pack('>d', value))
                    elif param_type == NodeParameterType.INT:
                        writer.write(struct.pack('>q', value))
                    elif param_type == NodeParameterType.BOOL:
                        writer.write(struct.pack('>?', value))
                    elif param_type == NodeParameterType.STRING:
                        await write_string(value)
                    else:  # only possible if we seriously forgot to update this protocol after updating NodeParameterType or smth
                        raise NotImplementedError()
            # note: values written back to sender is:  \? (value) \? (value) ... \? (value)
            #       where \? is \1 if value was set, and \0 if wasn't

        # async def comm_set_node_param_expression():  # elif command == b'setnodeparamexpression':
        #     node_id, set_or_unset = struct.unpack('>Q?', await reader.readexactly(9))
        #     param_name = await read_string()
        #     res = b'\1'
        #     try:
        #         async with self.__scheduler.node_object_by_id_for_writing(node_id) as node:
        #             if set_or_unset:  # means SET
        #                 expression = await read_string()
        #                 await asyncio.get_event_loop().run_in_executor(None, node.param(param_name).set_expression, expression)
        #                 # node.param(param_name).set_expression(expression)
        #             else:
        #                 await asyncio.get_event_loop().run_in_executor(None, node.param(param_name).remove_expression)
        #     except Exception as e:
        #         self.__logger.error(f'error setting parameter expression on {node_id}: {str(e)}')
        #         res = b'\0'
        #     writer.write(res)

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
            ret: Tuple[SpawnStatus, Optional[int]] = await self.__scheduler.spawn_tasks(newtask)
            writer.write(struct.pack('>I?Q', ret[0].value, ret[1] is not None, 0 if ret[1] is None else ret[1]))

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
        commands = {#'getfullstate': comm_get_full_state,
                    'get_db_uid': comm_get_db_uid,
                    'get_ui_graph_state_update_id': comm_get_ui_graph_state_update_id,
                    'get_ui_graph_state': comm_get_ui_graph_state,
                    'get_ui_task_groups': comm_get_ui_task_groups,
                    'get_ui_tasks_state': comm_get_ui_tasks_state,
                    'request_task_events': comm_request_subscribe_to_task_events,
                    'task_events_since_id': comm_request_task_events_since_id,
                    'get_ui_workers_state': comm_get_ui_workers_state,
                    'getinvocmeta': comm_get_invoc_meta,
                    'getlog': comm_get_log,
                    'getnodeinterface': comm_get_node_interface,
                    'gettaskattribs': comm_get_task_attribs,
                    'gettaskinvoc': comm_get_task_invocation,
                    'listnodetypes': comm_list_node_types,
                    'listnodepresets': comm_list_presets,
                    'getnodepreset': comm_get_node_preset,
                    'removenode': comm_remove_node,
                    'addnode': comm_add_node,
                    'wipenode': comm_wipe_node,
                    'nodehasparam': comm_node_has_param,
                    'setnodeparam': comm_set_node_param,
                    'batchsetnodeparams': comm_set_node_params,
                    # 'setnodeparamexpression': comm_set_node_param_expression,
                    'applynodesettings': comm_apply_node_settings,
                    'savecustomnodesettings': comm_save_custom_node_settings,
                    'setsettingsdefault': comm_set_settings_default,
                    'renamenode': comm_rename_node,
                    'duplicatenodes': comm_duplicate_nodes,
                    'changeconnection': comm_change_connection,
                    'addconnection': comm_add_connection,
                    'removeconnection': comm_remove_connection,
                    'tpauselst': comm_pause_tasks,
                    'tpausegrp': comm_pause_task_group,
                    'tarchivegrp': comm_archive_task_group,
                    'tcancel': comm_task_cancel,
                    'workertaskcancel': worker_task_cancel,
                    'tsetnode': comm_task_set_node,
                    'tcstate': comm_task_change_state,
                    'tsetname': comm_task_set_name,
                    'tsetgroups': comm_task_set_groups,
                    'tupdateattribs': comm_task_update_attribs,
                    'addtask': comm_add_task,
                    'settaskenvresolverargs': set_task_environment_resolver_arguments,
                    'setworkergroups': set_worker_groups}
        #
        # connection callback
        #
        self.__logger.debug('UI connected')

        async def read_string() -> str:
            strlen = struct.unpack('>Q', await reader.readexactly(8))[0]
            if strlen == 0:
                return ''
            return (await reader.readexactly(strlen)).decode('UTF-8')

        async def write_string(s: str):
            b = s.encode('UTF-8')
            writer.write(struct.pack('>Q', len(b)))
            writer.write(b)

        try:
            proto = await asyncio.wait_for(reader.readexactly(4), timeout=self.__timeout)
            if proto != b'\0\1\0\0':
                raise NotImplementedError(f'protocol version unsupported {proto}')

            wait_stop_task = asyncio.create_task(self.__scheduler._stop_event_wait())
            while True:
                readline_task = asyncio.create_task(read_string())
                done, pending = await asyncio.wait((readline_task, wait_stop_task), return_when=asyncio.FIRST_COMPLETED)
                if wait_stop_task in done:
                    self.__logger.debug(f'scheduler is stopping: closing ui connections')
                    readline_task.cancel()
                    return
                assert readline_task.done()
                try:
                    command: str = await readline_task
                except IncompleteReadError:  # this means connection was closed
                    self.__logger.debug('UI disconnected: connection closed')
                    break
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
        finally:
            writer.close()
            await writer.wait_closed()
            # according to the note in the beginning of the function - now reference can be cleared
            self.__saved_references.remove(asyncio.current_task())


#
# ======== client ========
#


class UIProtocolSocketClient:
    def __init__(self, host: str, port: int, timeout=30):
        self.__address = (host, port)
        self.__connection: Optional[BufferedConnection] = None
        self.__timeout = timeout

    def initialize(self):
        if self.__connection is None:
            self.__connection = BufferedConnection(self.__address, timeout=self.__timeout)
            self.__connection.writer.write(b'\0\1\0\0')

    def close(self):
        if self.__connection is None:
            return
        self.__connection.close()

    def __enter__(self):
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # def get_full_state(self, skip_dead, skip_archived_groups) -> UiData:
    #     r, w = self.__connection.get_rw_pair()
    #     w.write_string('getfullstate')
    #     w.write(struct.pack('>??', skip_dead, skip_archived_groups))
    #     w.flush()
    #     return UiData.deserialize(r)

    def get_db_uid(self) -> int:
        """

        :return:
        """
        r, w = self.__connection.get_rw_pair()
        w.write_string('get_db_uid')
        w.flush()
        uid, = struct.unpack('>Q', r.readexactly(8))
        return uid

    def get_ui_graph_state_update_id(self) -> int:
        """
        get latest graph update event id
        """
        r, w = self.__connection.get_rw_pair()
        w.write_string('get_ui_graph_state_update_id')
        w.flush()
        uid, = struct.unpack('>Q', r.readexactly(8))
        return uid

    def get_ui_graph_state(self) -> (NodeGraphStructureData, int):
        """
        returns latest graph state and update event id corresponding to it
        that id can be used to check against get_ui_graph_state_update_id() to check if update is available
        """
        r, w = self.__connection.get_rw_pair()
        w.write_string('get_ui_graph_state')
        w.flush()
        update_id, = struct.unpack('>Q', r.readexactly(8))
        data = NodeGraphStructureData.deserialize(r)
        return data, update_id

    def get_ui_task_groups(self, skip_archived_groups) -> TaskGroupBatchData:
        r, w = self.__connection.get_rw_pair()
        w.write_string('get_ui_task_groups')
        w.write(struct.pack('>?', skip_archived_groups))
        w.flush()
        data = TaskGroupBatchData.deserialize(r)
        return data

    def get_ui_tasks_state(self, groups: Iterable[str], include_dead: bool) -> TaskBatchData:
        r, w = self.__connection.get_rw_pair()
        if not isinstance(groups, (list, tuple)):
            groups = list(groups)
        w.write_string('get_ui_tasks_state')
        w.write(struct.pack('>?Q', include_dead, len(groups)))
        for group in groups:
            w.write_string(group)
        w.flush()
        data = TaskBatchData.deserialize(r)
        return data

    # task subscription
    def request_subscribe_to_task_events(self, groups: Iterable[str], include_dead: bool, request_for_seconds: float = 10.0) -> List[TaskEvent]:
        r, w = self.__connection.get_rw_pair()
        if not isinstance(groups, (list, tuple)):
            groups = list(groups)
        w.write_string('request_task_events')
        w.write(struct.pack('>?Qd', include_dead, len(groups), request_for_seconds))
        for group in groups:
            w.write_string(group)
        w.flush()
        num_events, = struct.unpack('>Q', r.readexactly(8))
        events = []
        for i in range(num_events):
            events.append(TaskEvent.deserialize(r))
        return events

    def request_task_events_since_id(self, groups: Iterable[str], include_dead: bool, last_known_event_id: int) -> Optional[List[TaskEvent]]:
        """
        returns None if there is no valid subscription with given parameters,
        returns list of task events otherwise
        """
        r, w = self.__connection.get_rw_pair()
        if not isinstance(groups, (list, tuple)):
            groups = list(groups)
        w.write_string('task_events_since_id')
        w.write(struct.pack('>?QQ', include_dead, len(groups), last_known_event_id))
        for group in groups:
            w.write_string(group)
        w.flush()
        good, = struct.unpack('>?', r.readexactly(1))
        if not good:
            return None
        num_events, = struct.unpack('>Q', r.readexactly(8))
        events = []
        for i in range(num_events):
            events.append(TaskEvent.deserialize(r))
        return events
    #

    def get_ui_workers_state(self) -> WorkerBatchData:
        r, w = self.__connection.get_rw_pair()
        w.write_string('get_ui_workers_state')
        w.flush()
        data = WorkerBatchData.deserialize(r)
        return data

    def get_invoc_meta(self, task_id) -> Dict[int, List[IncompleteInvocationLogData]]:
        r, w = self.__connection.get_rw_pair()
        w.write_string('getinvocmeta')
        w.write(struct.pack('>Q', task_id))
        w.flush()

        ret: Dict[int, List[IncompleteInvocationLogData]] = {}
        node_count, = struct.unpack('>Q', r.readexactly(8))
        for _ in range(node_count):
            node_id, invoc_count = struct.unpack('>QQ', r.readexactly(16))
            for _ in range(invoc_count):
                log = IncompleteInvocationLogData.deserialize(r)
                ret.setdefault(node_id, []).append(log)

        return ret

    def get_log(self, invocation_id) -> Optional[InvocationLogData]:
        r, w = self.__connection.get_rw_pair()
        w.write_string('getlog')
        w.write(struct.pack('>Q', invocation_id))
        w.flush()
        has_data, = struct.unpack('>?', r.readexactly(1))
        log = None
        if has_data:
            log = InvocationLogData.deserialize(r)
        return log

    def get_log_all(self, task_id, node_id):
        raise NotImplementedError()

    def get_node_interface(self, node_id) -> NodeUi:
        r, w = self.__connection.get_rw_pair()
        w.write_string('getnodeinterface')
        w.write(struct.pack('>Q', node_id))
        w.flush()
        rcvsize = struct.unpack('>I', r.readexactly(4))[0]
        nodeui: NodeUi = pickle.loads(r.readexactly(rcvsize))
        return nodeui

    def get_task_attribs(self, task_id) -> Tuple[Dict[str, Any], Optional[EnvironmentResolverArguments]]:
        r, w = self.__connection.get_rw_pair()
        w.write_string('gettaskattribs')
        w.write(struct.pack('>Q', task_id))
        w.flush()
        rcvsize = struct.unpack('>Q', r.readexactly(8))[0]
        attribs = _deserialize_json_dict(r.readexactly(rcvsize))
        rcvsize = struct.unpack('>Q', r.readexactly(8))[0]
        env_attrs = None
        if rcvsize > 0:
            env_attrs = EnvironmentResolverArguments.deserialize(r.readexactly(rcvsize))
        return attribs, env_attrs

    def get_task_invocation(self, task_id) -> InvocationJob:
        r, w = self.__connection.get_rw_pair()
        w.write_string('gettaskinvoc')
        w.write(struct.pack('>Q', task_id))
        w.flush()
        rcvsize = struct.unpack('>Q', r.readexactly(8))[0]
        if rcvsize == 0:
            invoc = InvocationJob([])
        else:
            invoc = InvocationJob.deserialize(r.readexactly(rcvsize))
        return invoc

    def list_node_types(self) -> Dict[str, NodeTypeMetadata]:
        r, w = self.__connection.get_rw_pair()
        metas: List[NodeTypeMetadata] = []
        w.write_string('listnodetypes')
        w.flush()
        elemcount, = struct.unpack('>Q', r.readexactly(8))
        for i in range(elemcount):
            btlen, = struct.unpack('>Q', r.readexactly(8))
            metas.append(pickle.loads(r.readexactly(btlen)))
        nodetypes = {n.type_name: n for n in metas}
        return nodetypes

    def list_presets(self) -> Dict[str, Dict[str, NodeSnippetDataPlaceholder]]:
        r, w = self.__connection.get_rw_pair()
        w.write_string('listnodepresets')
        w.flush()
        btlen, = struct.unpack('>Q', r.readexactly(8))
        presets = pickle.loads(r.readexactly(btlen))
        return presets

    def get_node_preset(self, package_name, preset_name) -> Optional[NodeSnippetData]:
        r, w = self.__connection.get_rw_pair()
        w.write_string('getnodepreset')
        w.write_string(package_name)
        w.write_string(preset_name)
        w.flush()
        good, = struct.unpack('>?', r.readexactly(1))
        if not good:
            return None
        btlen = struct.unpack('>Q', r.readexactly(8))[0]
        snippet: NodeSnippetData = NodeSnippetData.deserialize(r.readexactly(btlen))
        return snippet

    def remove_node(self, node_id: int) -> bool:
        r, w = self.__connection.get_rw_pair()
        w.write_string('removenode')
        w.write(struct.pack('>Q', node_id))
        w.flush()
        return r.readexactly(1) == b'\1'

    def add_node(self, node_type: str, node_name: str) -> int:
        r, w = self.__connection.get_rw_pair()
        w.write_string('addnode')
        w.write_string(node_type)
        w.write_string(node_name)
        w.flush()
        node_id, = struct.unpack('>Q', r.readexactly(8))
        return node_id

    def wipe_node(self, node_id: int):
        r, w = self.__connection.get_rw_pair()
        w.write_string('wipenode')
        w.write(struct.pack('>Q', node_id))
        w.flush()
        assert r.readexactly(1) == b'\1'

    def node_has_param(self, node_id: int, param_name: str) -> bool:
        r, w = self.__connection.get_rw_pair()
        w.write_string('nodehasparam')
        w.write(struct.pack('>Q', node_id))
        w.write_string(param_name)
        w.flush()
        good = r.readexactly(1) == b'\1'
        return good

    def set_node_param(self, node_id: int, param: Parameter) -> Union[int, float, str, bool, None]:
        """

        :param node_id:
        :param param:
        :return: newly set parameter value, or None if failed
        """
        r, w = self.__connection.get_rw_pair()
        param_type = param.type()
        param_value = param.unexpanded_value()
        w.write_string('setnodeparam')
        w.write(struct.pack('>QI?', node_id, param_type.value, param.has_expression()))
        w.write_string(param.name())
        if param.has_expression():
            w.write_string(param.expression())
        newval = None
        if param_type == NodeParameterType.FLOAT:
            w.write(struct.pack('>d', param_value))
            w.flush()
            if r.readexactly(1) == b'\1':
                newval, = struct.unpack('>d', r.readexactly(8))
        elif param_type == NodeParameterType.INT:
            w.write(struct.pack('>q', param_value))
            w.flush()
            if r.readexactly(1) == b'\1':
                newval, = struct.unpack('>q', r.readexactly(8))
        elif param_type == NodeParameterType.BOOL:
            w.write(struct.pack('>?', param_value))
            w.flush()
            if r.readexactly(1) == b'\1':
                newval, = struct.unpack('>?', r.readexactly(1))
        elif param_type == NodeParameterType.STRING:
            w.write_string(param_value)
            w.flush()
            if r.readexactly(1) == b'\1':
                newval = r.read_string()
        else:
            raise NotImplementedError()
        return newval

    def set_node_params(self, node_id: int, params: Iterable[Parameter], want_result: bool = False) -> Optional[Dict[str, Union[int, float, str, bool, None]]]:
        r, w = self.__connection.get_rw_pair()
        params = list(params)
        w.write_string('batchsetnodeparams')
        w.write(struct.pack('>QQ?', node_id, len(params), want_result))
        for param in params:
            param_type = param.type()
            w.write_string(param.name())
            value_is_expression = param.has_expression()
            w.write(struct.pack('>I?', param_type.value, value_is_expression))
            if value_is_expression:
                w.write_string(param.expression())
            else:
                param_value = param.unexpanded_value()
                if param_type == NodeParameterType.FLOAT:
                    w.write(struct.pack('>d', param_value))
                elif param_type == NodeParameterType.INT:
                    w.write(struct.pack('>q', param_value))
                elif param_type == NodeParameterType.BOOL:
                    w.write(struct.pack('>?', param_value))
                elif param_type == NodeParameterType.STRING:
                    w.write_string(param_value)
                else:
                    raise NotImplementedError()
        w.flush()
        if not want_result:
            assert r.readexactly(1) == b'\1'  # not expect result, just ack
            return
        result = {}
        for param in params:
            if r.readexactly(1) == b'\0':
                result[param.name()] = None
                continue
            param_type = param.type()
            if param_type == NodeParameterType.FLOAT:
                val, = struct.unpack('>d', r.readexactly(8))
            elif param_type == NodeParameterType.INT:
                val, = struct.unpack('>q', r.readexactly(8))
            elif param_type == NodeParameterType.BOOL:
                val, = struct.unpack('>?', r.readexactly(1))
            elif param_type == NodeParameterType.STRING:
                val = r.read_string()
            else:
                raise NotImplementedError()
            result[param.name()] = val
        return result

    def set_node_param_expression(self, node_id: int, param_name: str, expression: Optional[str]) -> bool:
        r, w = self.__connection.get_rw_pair()
        set_or_unset = expression is not None
        w.write_string('setnodeparamexpression')
        w.write(struct.pack('>Q?', node_id, set_or_unset))
        w.write_string(param_name)
        if set_or_unset:
            w.write_string(expression)
        w.flush()
        return r.readexactly(1) == b'\1'

    def apply_node_settings(self, node_id: int, settings_name: str) -> bool:
        r, w = self.__connection.get_rw_pair()
        w.write_string('applynodesettings')
        w.write(struct.pack('>Q', node_id))
        w.write_string(settings_name)
        w.flush()
        return r.readexactly(1) == b'\1'

    def save_custom_node_settings(self, node_type_name: str, settings_name: str, settings: Dict[str, Any]) -> bool:
        r, w = self.__connection.get_rw_pair()
        w.write_string('savecustomnodesettings')
        w.write_string(node_type_name)
        w.write_string(settings_name)
        settings_data = pickle.dumps(settings)
        w.write(struct.pack('>Q', len(settings_data)))
        w.write(settings_data)
        w.flush()
        return r.readexactly(1) == b'\1'

    def set_settings_default(self, node_type_name: str, settings_name: Optional[str]):
        r, w = self.__connection.get_rw_pair()
        w.write_string('setsettingsdefault')
        w.write_string(node_type_name)
        w.write(struct.pack('>?', settings_name is not None))
        if settings_name is not None:
            w.write_string(settings_name)
        w.flush()
        return r.readexactly(1) == b'\1'

    def rename_node(self, node_id: int, node_name: str) -> str:
        r, w = self.__connection.get_rw_pair()
        w.write_string('renamenode')
        w.write(struct.pack('>Q', node_id))
        w.write_string(node_name)
        w.flush()
        return r.read_string()

    def duplicate_nodes(self, node_ids: List[int]) -> Optional[Dict[int, int]]:
        """

        :param node_ids:
        :return:  mapping of old node ids to new node ids
        """
        r, w = self.__connection.get_rw_pair()
        w.write_string('duplicatenodes')
        w.write(struct.pack('>Q', len(node_ids)))
        for node_id in node_ids:
            w.write(struct.pack('>Q', node_id))
        w.flush()
        result = r.readexactly(1)
        if result == b'\0':
            return None
        cnt, = struct.unpack('>Q', r.readexactly(8))
        ret = {}
        for i in range(cnt):
            old_id, new_id = struct.unpack('>QQ', r.readexactly(16))
            assert old_id in node_ids
            ret[old_id] = new_id
        return ret

    def change_connection_by_id(self, connection_id: int, outnode_id: Optional[int], out_name: Optional[str], innode_id: Optional[int], in_name: Optional[str]):
        r, w = self.__connection.get_rw_pair()
        w.write_string('changeconnection')
        w.write(struct.pack('>Q??QQ', connection_id, outnode_id is not None, innode_id is not None, outnode_id or 0, innode_id or 0))
        if outnode_id is not None:
            w.write_string(out_name)
        if innode_id is not None:
            w.write_string(in_name)
        w.flush()
        assert r.readexactly(1) == b'\1'

    def add_connection(self, outnode_id: int, out_name: str, innode_id: int, in_name: str) -> int:
        """

        :param outnode_id:
        :param out_name:
        :param innode_id:
        :param in_name:
        :return: new or existing connection id
        """
        r, w = self.__connection.get_rw_pair()
        w.write_string('addconnection')
        w.write(struct.pack('>QQ', outnode_id, innode_id))
        w.write_string(out_name)
        w.write_string(in_name)
        w.flush()
        new_or_existing_id, = struct.unpack('>Q', r.readexactly(8))
        return new_or_existing_id

    def remove_connection_by_id(self, connection_id: int):
        r, w = self.__connection.get_rw_pair()
        w.write_string('removeconnection')
        w.write(struct.pack('>Q', connection_id))
        w.flush()
        assert r.readexactly(1) == b'\1'

    def pause_tasks(self, task_ids: Iterable[int], paused: bool):
        r, w = self.__connection.get_rw_pair()
        task_ids = list(task_ids)
        numtasks = len(task_ids)
        if numtasks == 0:
            return
        w.write_string('tpauselst')
        w.write(struct.pack('>Q?Q', numtasks, paused, task_ids[0]))
        if numtasks > 1:
            w.write(struct.pack('>' + 'Q' * (numtasks - 1), *task_ids[1:]))
        w.flush()
        assert r.readexactly(1) == b'\1'

    def pause_task_group(self, task_group_name, paused):
        r, w = self.__connection.get_rw_pair()
        w.write_string('tpausegrp')
        w.write(struct.pack('>?', paused))
        w.write_string(task_group_name)
        w.flush()
        assert r.readexactly(1) == b'\1'

    def archive_task_group(self, archived_state: TaskGroupArchivedState, task_group_name: str):
        r, w = self.__connection.get_rw_pair()
        w.write_string('tarchivegrp')
        w.write_string(task_group_name)
        w.write(struct.pack('>I', archived_state.value))
        w.flush()
        assert r.readexactly(1) == b'\1'

    def cancel_invocation_for_task(self, task_id: int):
        r, w = self.__connection.get_rw_pair()
        w.write_string('tcancel')
        w.write(struct.pack('>Q', task_id))
        w.flush()
        assert r.readexactly(1) == b'\1'

    def cancel_invocation_for_worker(self, worker_id: int):
        r, w = self.__connection.get_rw_pair()
        w.write_string('workertaskcancel')
        w.write(struct.pack('>Q', worker_id))
        w.flush()
        assert r.readexactly(1) == b'\1'

    def set_node_for_task(self, task_id: int, node_id: int):
        r, w = self.__connection.get_rw_pair()
        w.write_string('tsetnode')
        w.write(struct.pack('>QQ', task_id, node_id))
        w.flush()
        assert r.readexactly(1) == b'\1'

    def change_tasks_state(self, task_ids: Iterable[int], state: TaskState):
        r, w = self.__connection.get_rw_pair()
        task_ids = list(task_ids)
        numtasks = len(task_ids)
        if numtasks == 0:
            return
        w.write_string('tcstate')
        w.write(struct.pack('>QIQ', numtasks, state.value, task_ids[0]))
        if numtasks > 1:
            w.write(struct.pack('>' + 'Q' * (numtasks - 1), *task_ids[1:]))
        w.flush()
        assert r.readexactly(1) == b'\1'

    def set_task_name(self, task_id: int, new_name: str):
        r, w = self.__connection.get_rw_pair()
        w.write_string('tsetname')
        w.write(struct.pack('>Q', task_id))
        w.write_string(new_name)
        w.flush()
        assert r.readexactly(1) == b'\1'

    def set_task_groups(self, task_id: int, task_groups: Iterable[str]):
        r, w = self.__connection.get_rw_pair()
        task_groups = list(task_groups)
        w.write_string('tsetgroups')
        w.write(struct.pack('>QQ', task_id, len(task_groups)))
        for group in task_groups:
            w.write_string(group)
        w.flush()
        assert r.readexactly(1) == b'\1'

    def update_task_attributes(self, task_id, attribs_to_set: Dict[str, Any], attribs_to_delete: Iterable[str]):
        r, w = self.__connection.get_rw_pair()
        attribs_to_delete = set(attribs_to_delete)
        w.write_string('tupdateattribs')
        data_bytes = pickle.dumps(attribs_to_set)
        w.write(struct.pack('>QQQ', task_id, len(data_bytes), len(attribs_to_delete)))
        w.write(data_bytes)
        for attr in attribs_to_delete:
            w.write_string(attr)
        w.flush()
        assert r.readexactly(1) == b'\1'

    def add_task(self, new_task: NewTask) -> Optional[int]:
        """

        :param new_task:
        :return:  new task id if succeeded, else - None
        """
        r, w = self.__connection.get_rw_pair()
        data = new_task.serialize()
        w.write_string('addtask')
        w.write(struct.pack('>Q', len(data)))
        w.write(data)
        w.flush()
        spawn_status_value, good, new_id = struct.unpack('>I?Q', r.readexactly(13))  # reply that we don't care about for now
        if SpawnStatus(spawn_status_value) == SpawnStatus.FAILED or not good:
            return None
        return new_id

    def set_task_environment_resolver_arguments(self, task_id: int, env_args: Optional[EnvironmentResolverArguments]):
        r, w = self.__connection.get_rw_pair()
        w.write_string('settaskenvresolverargs')
        w.write(struct.pack('>Q', task_id))
        if env_args is None:
            w.write(struct.pack('>Q', 0))
        else:
            data = env_args.serialize()
            w.write(struct.pack('>Q', len(data)))
            w.write(data)
        w.flush()
        assert r.readexactly(1) == b'\1'

    def set_worker_groups(self, worker_hwid: int, groups: Iterable[str]):
        groups = list(groups)
        r, w = self.__connection.get_rw_pair()
        w.write_string('setworkergroups')
        w.write(struct.pack('>QQ', worker_hwid, len(groups)))
        for group in groups:
            w.write_string(group)
        w.flush()
        assert r.readexactly(1) == b'\1'
