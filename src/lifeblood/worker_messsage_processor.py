import os
import asyncio
import aiofiles
from contextlib import contextmanager
from .exceptions import NotEnoughResources, ProcessInitializationError, WorkerNotAvailable
from .environment_resolver import ResolutionImpossibleError
from . import logging
from . import invocationjob
from .exceptions import AlreadyRunning
from .enums import WorkerPingReply, TaskScheduleStatus
from .net_messages.tcp_impl.tcp_simple_command_message_processor import TcpCommandMessageProcessor, CommandJsonMessageClient
from .net_messages.address import AddressChain
from .net_messages.messages import Message


from typing import Optional, Tuple, TYPE_CHECKING
if TYPE_CHECKING:
    from .worker import Worker


class WorkerMessageProcessor(TcpCommandMessageProcessor):
    def __init__(self, worker: "Worker", listening_address: Tuple[str, int], *, backlog=4096, connection_pool_cache_time=300):
        super().__init__(listening_address, backlog=backlog, connection_pool_cache_time=connection_pool_cache_time)
        self.__logger = logging.get_logger('worker.message_processor')
        self.__worker = worker

    async def should_process(self, orig_message: Message):
        return not self.__worker.is_stopping()

    def command_mapping(self):
        return {'ping': self._command_ping,
                'task': self._command_task,
                'quit': self._command_quit,
                'drop': self._command_drop,
                'status': self._command_status,
                'log': self._command_log}

    #
    # commands
    #

    #
    # command ping
    async def _command_ping(self, args: dict, client: CommandJsonMessageClient, original_message: Message):
        """
        expects keys:
        returns keys:
            ps: ping status
            pv: task completion percentage (0-1) if any
        """
        # TODO: implement this shit too
        # if self.__worker.is_stopping():
        #     pstats = WorkerPingReply.OFF.value
        #     pvalue = 0
        # el
        if self.__worker.is_task_running():
            pstatus = WorkerPingReply.BUSY.value
            pvalue = int(await self.__worker.task_status() or 0)
        else:
            pstatus = WorkerPingReply.IDLE.value
            pvalue = 0
        await client.send_message_as_json({'ps': pstatus,
                                           'pv': pvalue})

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
        task = invocationjob.InvocationJob.deserialize(args['task'].encode('latin1'))
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
        except ResolutionImpossibleError:
            self.__logger.info('Worker failed to resolve required environment. rejecting task')
            reply['status'] = TaskScheduleStatus.FAILED.value
        except ProcessInitializationError:
            self.__logger.info('Failed to initialize payload process. rejecting task')
            reply['status'] = TaskScheduleStatus.FAILED.value
        except NotEnoughResources:
            self.__logger.warning('Not enough resources (this is unusual error - scheduler should know our resources). rejecting task')
            reply['status'] = TaskScheduleStatus.FAILED.value
        except WorkerNotAvailable:
            self.__logger.warning('Got a task, but Worker is not available. Most probably is stopping right now')
            reply['status'] = TaskScheduleStatus.FAILED.value
        except Exception as e:
            self.__logger.exception('no, cuz %s', e)
            reply['status'] = TaskScheduleStatus.FAILED.value

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
# Client
#


class WorkerControlClient:
    def __init__(self, client: CommandJsonMessageClient):
        self.__client = client

    @classmethod
    @contextmanager
    def get_worker_control_client(cls, scheduler_address: AddressChain, processor: TcpCommandMessageProcessor) -> "WorkerControlClient":
        with processor.message_client(scheduler_address) as message_client:
            yield WorkerControlClient(message_client)

    async def ping(self) -> Tuple[WorkerPingReply, float]:
        await self.__client.send_command('ping', {})

        reply_message = await self.__client.receive_message()
        data_json = await reply_message.message_body_as_json()
        return WorkerPingReply(data_json['ps']), float(data_json['pv'])

    async def give_task(self, task: invocationjob.InvocationJob, reply_address: Optional[AddressChain] = None) -> TaskScheduleStatus:
        """
        if reply_address is not given - message source address will be used
        """
        await self.__client.send_command('task', {
            'task': (await task.serialize_async()).decode('latin1'),
            'reply_to': str(reply_address) if reply_address else None
        })

        reply = await (await self.__client.receive_message()).message_body_as_json()
        return TaskScheduleStatus(reply['status'])

    async def quit_worker(self):
        await self.__client.send_command('quit', {})

        await self.__client.receive_message()

    async def cancel_task(self) -> None:
        await self.__client.send_command('drop', {})

        await self.__client.receive_message()

    async def status(self):
        raise NotImplementedError()

    async def get_log(self, invocation_id) -> Tuple[str, str]:
        await self.__client.send_command('log', {
            'invoc_id': invocation_id
        })

        reply = await (await self.__client.receive_message()).message_body_as_json()
        return str(reply['stdout']), str(reply['stderr'])
