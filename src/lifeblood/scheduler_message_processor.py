import asyncio

from . import logging
from . import invocationjob
from .taskspawn import TaskSpawn
from .enums import WorkerState, WorkerType, SpawnStatus, InvocationState, InvocationMessageResult
from .worker_message_processor_client import WorkerControlClient
from .hardware_resources import HardwareResources
from .worker_metadata import WorkerMetadata
from .net_messages.impl.tcp_simple_command_message_processor import TcpCommandMessageProcessor
from .net_messages.impl.clients import CommandJsonMessageClient
from .net_messages.address import DirectAddress
from .net_messages.messages import Message
from .net_messages.exceptions import MessageTransferTimeoutError, MessageTransferError
from .net_messages.impl.message_haldlers import CommandMessageHandlerBase
from .scheduler.scheduler_core import SchedulerCore

from typing import Awaitable, Callable, Dict, Iterable, Optional, Tuple, Union


class SchedulerCommandHandler(CommandMessageHandlerBase):
    def __init__(self, scheduler: SchedulerCore):
        super().__init__()
        self.__scheduler = scheduler

    def command_mapping(self) -> Dict[str, Callable[[dict, CommandJsonMessageClient, Message], Awaitable[None]]]:
        return {
            'pulse': self._command_pulse,
            'what_is_my_address': self._command_what_is_my_address,
            '_pulse3way_': self._command_pulse3way,  # TODO: remove this when handlers are implemented
            'resource_defs': self._command_resource_definitions,
            # worker-specific
            'worker.ping': self._command_ping,
            'worker.done': self._command_done,
            'worker.dropped': self._command_dropped,
            'worker.hello': self._command_hello,
            'worker.bye': self._command_bye,
            'worker.progress_report': self._command_progress_report,
            'forward_invocation_message': self._command_forward_invocation_message,
        }

    #
    # commands
    #

    async def _command_ping(self, args: dict, client: CommandJsonMessageClient, original_message: Message):  # 'ping'
        """
        expects keys:
            worker_addr: address of the worker to query
        returns keys:
            state: worker status as seen by scheduler
        """
        # when worker pings scheduler - scheduler returns the state it thinks the worker is in
        addr = args['worker_addr']
        wid = await self.__scheduler.worker_id_from_address(addr)
        if wid is None:
            state = WorkerState.UNKNOWN
        else:
            state = await self.__scheduler.get_worker_state(wid)
        await client.send_message_as_json({'state': state.value})

    async def _command_pulse(self, args: dict, client: CommandJsonMessageClient, original_message: Message):  # 'pulse'
        """
        expects keys:
        returns keys:
            ok: ok is ok
        """
        await client.send_message_as_json({'ok': True})

    async def _command_what_is_my_address(self, args: dict, client: CommandJsonMessageClient, original_message: Message):  # 'what_is_my_address'
        """
        expects keys:
        returns keys:
            ok: ok is ok
            my_address: address chain of the client who called this command
        """
        await client.send_message_as_json({
            'ok': True,
            'my_address': str(original_message.message_source()),
        })

    async def _command_done(self, args: dict, client: CommandJsonMessageClient, original_message: Message):  # 'done'
        """
        expects keys:
            task: serialized task
            stdout: task's stdout log (str)
            stderr: task's stderr log (str)
        returns keys:
            ok: ok is ok
        """
        task = await asyncio.get_event_loop().run_in_executor(None, invocationjob.Invocation.deserialize_from_data, args['task'])

        stdout = args['stdout']
        stderr = args['stderr']
        self._logger.debug('command: worker done invoc %s', task.invocation_id())
        await self.__scheduler.task_done_reported(task, stdout, stderr)
        await client.send_message_as_json({'ok': True})

    async def _command_dropped(self, args: dict, client: CommandJsonMessageClient, original_message: Message):  # 'dropped'
        """
        expects keys:
        returns keys:
            ok: ok is ok
        """
        task = await asyncio.get_event_loop().run_in_executor(None, invocationjob.Invocation.deserialize_from_data, args['task'])

        stdout = args['stdout']
        stderr = args['stderr']
        self._logger.debug('command: worker dropped invoc %s', task.invocation_id())
        await self.__scheduler.task_cancel_reported(task, stdout, stderr)
        await client.send_message_as_json({'ok': True})

    async def _command_hello(self, args: dict, client: CommandJsonMessageClient, original_message: Message):  # 'hello'
        """
        worker reports for duty

        expects keys:
            worker_addr: worker address to talk to
            worker_type: worker type enum value
            worker_res: serialized worker resource capabilities
        returns keys:
            db_uid: scheduler's database uid
        """
        addr = args['worker_addr']
        metadata = WorkerMetadata(args.get('meta_hostname', str(addr)))
        workertype: WorkerType = WorkerType(args['worker_type'])
        res_data = args['worker_res'].encode('latin1')

        self._logger.debug('command: worker hello %s', addr)
        worker_hardware: HardwareResources = HardwareResources.deserialize(res_data)
        await self.__scheduler.add_worker(addr, workertype, worker_hardware, assume_active=True, worker_metadata=metadata)
        await client.send_message_as_json({'db_uid': self.__scheduler.db_uid()})

    async def _command_bye(self, args: dict, client: CommandJsonMessageClient, original_message: Message):  # 'bye'
        """
        worker reports he's quitting

        expects keys:
            worker_addr: worker address to talk to
        returns keys:
            ok: ok is ok
        """
        addr = args['worker_addr']
        self._logger.debug('command: worker bye %s', addr)
        await self.__scheduler.worker_stopped(addr)
        await client.send_message_as_json({'ok': True})

    async def _command_progress_report(self, args: dict, client: CommandJsonMessageClient, original_message: Message):
        """
        worker reports progress on currently running invocation
        """
        invoc_id = args['invocation_id']
        progress = args['progress']
        self._logger.debug('command: update progress of %d to %f', invoc_id, progress)
        await self.__scheduler.update_invocation_progress(invoc_id, progress)
        await client.send_message_as_json({'ok': True})

    async def _command_pulse3way(self, args: dict, client: CommandJsonMessageClient, original_message: Message):
        """
        TODO: remove this when handlers are implemented
        This command exists for test purposes only
        """
        await client.send_message_as_json({'phase': 1})
        msg2 = await client.receive_message()
        await client.send_message_as_json({'phase': 2})

    async def _command_resource_definitions(self, args: dict, client: CommandJsonMessageClient, original_message: Message):
        """
        returns keys:
            resources:
            device_types:
        """
        ress = self.__scheduler.config_provider.hardware_resource_definitions()
        defs = self.__scheduler.config_provider.hardware_device_type_definitions()

        data = {
            'resources': [r.to_json_dict() for r in ress],
            'device_types': [d.to_json_dict() for d in defs],
        }
        await client.send_message_as_json(data)

    # worker task message forwarding
    async def _command_forward_invocation_message(self, args: dict, client: CommandJsonMessageClient, original_message: Message):
        """
        expects keys:
            dst_invoc_id: receiver's invocation id
            src_invoc_id: sender's invocation id
            addressee: address id, where to address message within worker
            message_data_raw: message_data_raw
            addressee_timeout: timeout in seconds of how long to wait for addressee to start receiving
            overall_timeout: overall delivery waiting timeout. since this is just delivery, the only real waiting
                             may actually happen when waiting for addressee to start receiving, so this timeout
                             is very unlikely to ever hit, unless overall comm is disrupted
        returns keys:
            result: str, operation result
        """
        invoc_id = args['dst_invoc_id']
        invoc_state = await self.__scheduler.get_invocation_state(invoc_id)
        # invoking is included too, as message might be sent before submission is finalized on scheduler's side
        if invoc_state not in (InvocationState.IN_PROGRESS, InvocationState.INVOKING):
            await client.send_message_as_json({
                'result': InvocationMessageResult.ERROR_IID_NOT_RUNNING.value
            })
            return

        address = await self.__scheduler.get_invocation_worker(invoc_id)
        if address is None:
            await client.send_message_as_json({
                'result': InvocationMessageResult.ERROR_BAD_IID.value
            })
            return

        try:
            with WorkerControlClient.get_worker_control_client(address, self.__scheduler.message_processor()) as worker_client:  # type: WorkerControlClient
                result = await worker_client.send_invocation_message(invoc_id,
                                                                     args['addressee'],
                                                                     args['src_invoc_id'],
                                                                     args['message_data_raw'].encode('latin1'),
                                                                     args['addressee_timeout'],
                                                                     args['overall_timeout'])
        except MessageTransferTimeoutError:
            self._logger.error('could not deliver invocation message, timeout')
            result = InvocationMessageResult.ERROR_DELIVERY_TIMEOUT
        except MessageTransferError:
            self._logger.exception('message transfer failed')
            result = InvocationMessageResult.ERROR_TRANSFER_ERROR
        except Exception:
            self._logger.exception('something wend wrong')
            result = InvocationMessageResult.ERROR_UNEXPECTED

        await client.send_message_as_json({
            'result': result.value
        })


class SchedulerExtraCommandHandler(CommandMessageHandlerBase):
    def __init__(self, scheduler: SchedulerCore):
        super().__init__()
        self.__scheduler = scheduler

    def command_mapping(self) -> Dict[str, Callable[[dict, CommandJsonMessageClient, Message], Awaitable[None]]]:
        return {
            'spawn': self.comm_spawn,
            'nodenametoid': self.comm_node_name_to_id,
            'tupdateattribs': self.comm_update_task_attributes,
        }

    async def comm_spawn(self, args: dict, client: CommandJsonMessageClient, original_message: Message):
        """
        spawn a new task

        expects keys:
            task: serialized TaskSpawn
        returns keys:
            status: SpawnStatus value
            task_id: spawned task id or None if no tasks were spawned
        """
        task_data = args['task'].encode('latin1')
        taskspawn: TaskSpawn = TaskSpawn.deserialize(task_data)

        ret: Tuple[SpawnStatus, Optional[int]] = await self.__scheduler.spawn_tasks(taskspawn)
        await client.send_message_as_json({
            'status': ret[0].value,
            'task_id': ret[1]
        })

    async def comm_node_name_to_id(self, args: dict, client: CommandJsonMessageClient, original_message: Message):
        """
        node name to node id if found

        expects keys:
            name: name of the node to find
        returns keys:
            node_ids: list of int, ids of the nodes with given name
        """
        ids = await self.__scheduler.node_name_to_id(args['name'])
        await client.send_message_as_json({
            'node_ids': list(ids)
        })

    async def comm_update_task_attributes(self, args: dict, client: CommandJsonMessageClient, original_message: Message):
        """
        update task attributes

        expects keys:
            task_id: id of the task to update attributes of
            attribs_to_update: dict of attribute names to attribute values
            attribs_to_delete: list of attribute names to delete
        returns keys:
            ok: ok is ok
        """
        task_id = args['task_id']
        attribs_to_update = args['attribs_to_update']
        attribs_to_delete = set(args['attribs_to_delete'])
        await self.__scheduler.update_task_attributes(task_id, attribs_to_update, attribs_to_delete)
        await client.send_message_as_json({
            'ok': True
        })


class SchedulerMessageProcessor(TcpCommandMessageProcessor):
    def __init__(
            self,
            scheduler: SchedulerCore,
            listening_address_or_addresses: Union[Tuple[str, int], Iterable[Tuple[str, int]], DirectAddress, Iterable[DirectAddress]],
            *,
            backlog=4096,
            connection_pool_cache_time=300
    ):
        super().__init__(listening_address_or_addresses,
                         backlog=backlog,
                         connection_pool_cache_time=connection_pool_cache_time,
                         message_handlers=(SchedulerCommandHandler(scheduler),
                                           SchedulerExtraCommandHandler(scheduler)))
        self.__logger = logging.get_logger('scheduler.message_processor')
