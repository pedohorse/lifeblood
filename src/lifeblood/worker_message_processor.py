import os
import asyncio
import aiofiles
from .exceptions import NotEnoughResources, ProcessInitializationError, WorkerNotAvailable, \
    InvocationMessageWrongInvocationId, InvocationMessageAddresseeTimeout
from .environment_resolver import ResolutionImpossibleError
from . import logging
from . import invocationjob
from .exceptions import AlreadyRunning
from .enums import WorkerPingReply, TaskScheduleStatus, InvocationMessageResult
from .net_messages.impl.tcp_simple_command_message_processor import TcpCommandMessageProcessor
from .net_messages.impl.clients import CommandJsonMessageClient
from .net_messages.address import AddressChain, DirectAddress
from .net_messages.messages import Message
from .net_messages.impl.message_haldlers import CommandMessageHandlerBase
from .worker_core import WorkerCore
from .message_processor_ping_generic_handler import PingGenericHandler

from typing import Iterable, Tuple, Union


class WorkerPingHandler(PingGenericHandler):
    def __init__(self, worker: WorkerCore):
        super().__init__()
        self.__worker = worker
        
    async def produce_reply(self, data: dict) -> dict:
        if self.__worker.is_task_running():
            pstatus = WorkerPingReply.BUSY.value
            pvalue = int(self.__worker.task_status() or 0)
        else:
            pstatus = WorkerPingReply.IDLE.value
            pvalue = 0

        return {
            'status': pstatus,
            'progress': pvalue,
        }


class WorkerCommandHandler(CommandMessageHandlerBase):
    def __init__(self, worker: WorkerCore):
        super().__init__()
        self.__logger = logging.get_logger('worker.message_handler')
        self.__worker = worker

    def command_mapping(self):
        return {
            'task': self._command_task,
            'quit': self._command_quit,
            'drop': self._command_drop,
            'status': self._command_status,
            'log': self._command_log,
            'invocation_message': self._command_invocation_message
        }

    #
    # commands
    #

    #
    # command enqueue task
    async def _command_task(self, args: dict, client: CommandJsonMessageClient, original_message: Message):
        """
        expects keys:
            task: serialized InvocationJob
            reply_to: AddressChain where to reply about completion
        returns keys:
            status: TaskScheduleStatus, status of the operation
        """
        task = await asyncio.get_event_loop().run_in_executor(None, invocationjob.Invocation.deserialize_from_data, args['task'])
        addr = AddressChain(args['reply_to']) if args.get('reply_to') else original_message.message_source()
        reply = {}

        self.__logger.debug(f'got task: {task}, reply result to {addr}')
        try:
            self.__logger.debug('taking the task')
            await self.__worker.run_task(task, addr)
            reply['status'] = TaskScheduleStatus.SUCCESS.value
        except AlreadyRunning:
            self.__logger.debug('BUSY. rejecting task')
            reply['status'] = TaskScheduleStatus.BUSY.value
            reply['message'] = 'worker already working on a task'
        except ResolutionImpossibleError:
            self.__logger.info('Worker failed to resolve required environment. rejecting task')
            reply['status'] = TaskScheduleStatus.FAILED.value
            reply['message'] = 'failed to resolve requested environment'
            reply['error_class'] = 'environment_resolver'
        except ProcessInitializationError:
            self.__logger.info('Failed to initialize payload process. rejecting task')
            reply['status'] = TaskScheduleStatus.FAILED.value
            reply['message'] = 'failed to start the process'
            reply['error_class'] = 'spawn'
        except NotEnoughResources:  # currently not raised by worker
            self.__logger.warning('Not enough resources (this is unusual error - scheduler should know our resources). rejecting task')
            reply['status'] = TaskScheduleStatus.FAILED.value
            reply['message'] = 'not enough resources'
            reply['error_class'] = 'resources'
        except WorkerNotAvailable:
            self.__logger.warning('Got a task, but Worker is not available. Most probably is stopping right now')
            reply['status'] = TaskScheduleStatus.FAILED.value
            reply['message'] = 'worker is stopping'
            reply['error_class'] = 'stopping'
        except Exception as e:
            self.__logger.exception('no, cuz %s', e)
            reply['status'] = TaskScheduleStatus.FAILED.value
            reply['message'] = f'error happened: {e}'
            reply['error_class'] = 'exception'

        await client.send_message_as_json(reply)

    #
    # quit worker
    async def _command_quit(self, args: dict, client: CommandJsonMessageClient, original_message: Message):
        """
        expects keys:
        returns keys:
        """
        self.__worker.stop()
        await client.send_message_as_json({})

    #
    # command drop/cancel current task
    async def _command_drop(self, args: dict, client: CommandJsonMessageClient, original_message: Message):
        """
        expects keys:
        returns keys:
        """
        try:
            await self.__worker.cancel_task()
        except Exception:
            self.__logger.exception('task drop failed')
        await client.send_message_as_json({})

    #
    # command check worker status
    async def _command_status(self, reader, args: dict, client: CommandJsonMessageClient, original_message: Message):
        raise NotImplementedError()

    #
    # command to get worker's logs
    async def _command_log(self, args: dict, client: CommandJsonMessageClient, original_message: Message):
        """
        expects keys:
            invoc_id: invocation id to get logs of
        returns keys:
            stdout:
            stderr:
        """
        invocation_id = args['invoc_id']
        result = {}
        for key, logfilepath in (('stdout', self.__worker.get_log_filepath('output', invocation_id)),
                                 ('stderr', self.__worker.get_log_filepath('error', invocation_id))):
            if not os.path.exists(logfilepath):
                result[key] = ''
                continue
            async with aiofiles.open(logfilepath, 'r') as f:
                all_data = await f.read()  #TODO: what if there is binary crap somehow in the log?
                result[key] = all_data

        await client.send_message_as_json(result)

    #
    # commands for inter-task communication
    async def _command_invocation_message(self, args: dict, client: CommandJsonMessageClient, original_message: Message):
        """
        expects keys:
            dst_invoc_id: receiver's invocation id
            src_invoc_id: sender's invocation id
            addressee: address id, where to address message within worker
            message_data_raw: message_data_raw
            addressee_timeout: timeout in seconds of how long to wait for addressee to start receiving
        returns keys:
            result: str, operation result
        """
        result = 'unknown'
        try:
            await self.__worker.deliver_invocation_message(args['dst_invoc_id'],
                                                           args['addressee'],
                                                           args['src_invoc_id'],
                                                           args['message_data_raw'].encode('latin1'),
                                                           args['addressee_timeout'])
            result = InvocationMessageResult.DELIVERED.value
        except InvocationMessageWrongInvocationId:
            # it is possible that between sched checking for inv id and message received by worker
            # invocation finished, and we have to catch and report it
            # we report that iid is not running anymore
            result = InvocationMessageResult.ERROR_IID_NOT_RUNNING.value
        except InvocationMessageAddresseeTimeout:
            # we waited enough for addressee to start listening
            result = InvocationMessageResult.ERROR_RECEIVER_TIMEOUT.value
        except Exception:
            result = InvocationMessageResult.ERROR_UNEXPECTED.value

        await client.send_message_as_json({
            'result': result
        })


class WorkerMessageProcessor(TcpCommandMessageProcessor):
    def __init__(
            self,
            worker: WorkerCore,
            listening_address_or_addresses: Union[Tuple[str, int], Iterable[Tuple[str, int]], DirectAddress, Iterable[DirectAddress]],
            *,
            backlog=4096,
            connection_pool_cache_time=300
    ):
        super().__init__(listening_address_or_addresses,
                         backlog=backlog,
                         connection_pool_cache_time=connection_pool_cache_time,
                         message_handlers=(WorkerPingHandler(worker), WorkerCommandHandler(worker),))
