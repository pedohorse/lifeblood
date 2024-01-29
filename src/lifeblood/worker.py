import random
import sys
import os
import errno
import shutil
import copy
import traceback
import threading
import asyncio
import subprocess
import aiofiles
import aiosqlite
import json
import psutil
import datetime
import time
import tempfile
import signal
from enum import Enum
from . import logging
from .nethelpers import get_addr_to, get_default_addr, get_localhost, address_to_ip_port, get_hostname
from .net_classes import WorkerResources
from .worker_metadata import WorkerMetadata
# from .worker_task_protocol import WorkerTaskServerProtocol
from .exceptions import NotEnoughResources, ProcessInitializationError, WorkerNotAvailable, AlreadyRunning,\
    InvocationMessageWrongInvocationId, InvocationMessageAddresseeTimeout, InvocationMessageError
# from .scheduler_task_protocol import SchedulerTaskClient
from .worker_messsage_processor import WorkerMessageProcessor
from .scheduler_message_processor import SchedulerWorkerControlClient
from .worker_invocation_protocol import WorkerInvocationProtocolHandlerV10, WorkerInvocationServerProtocol
#from .worker_pool_protocol import WorkerPoolClient
from .worker_pool_message_processor import WorkerPoolControlClient
from .broadcasting import await_broadcast
from .invocationjob import InvocationJob
from .config import get_config, create_default_user_config_file
from . import environment_resolver
from .enums import WorkerType, WorkerState, ProcessPriorityAdjustment
from .paths import config_path
from .local_notifier import LocalMessageExchanger
from .filelock import FileCoupledLock
from .process_utils import create_process, kill_process_tree
from . import db_misc
from .misc import DummyLock
from .net_messages.address import AddressChain, DirectAddress
# from .net_messages.impl.tcp_message_processor import TcpMessageProcessor
from .net_messages.exceptions import MessageTransferError
from .defaults import worker_pool_port as default_worker_pool_port, \
                      scheduler_port as default_scheduler_port, \
                      worker_start_port as default_worker_start_port

from .worker_runtime_pythonpath import lifeblood_connection
import inspect

from typing import Any, Optional, Dict, Tuple, Union


is_posix = not sys.platform.startswith('win')


class Worker:
    def __init__(self, scheduler_addr: AddressChain, *,
                 child_priority_adjustment: ProcessPriorityAdjustment = ProcessPriorityAdjustment.NO_CHANGE,
                 worker_type: WorkerType = WorkerType.STANDARD,
                 singleshot: bool = False,
                 scheduler_ping_interval: float = 10,
                 scheduler_ping_miss_threshold: int = 6,
                 worker_id: Optional[int] = None,
                 pool_address: Optional[AddressChain] = None):
        """

        :param scheduler_addr:
        :param scheduler_port:
        :param worker_type:
        :param singleshot:
        """
        config = get_config('worker')
        self.__logger = logging.get_logger('worker')
        self.log_root_path: str = ''
        for self.log_root_path in (os.path.expandvars(config.get_option_noasync('worker.logpath', config_path('logs', 'worker'))),
                                   os.path.join(tempfile.gettempdir(), 'lifeblood', 'worker_logs')):
            logs_ok = True
            try:
                if not os.path.exists(self.log_root_path):
                    os.makedirs(self.log_root_path, exist_ok=True)
            except PermissionError:
                logs_ok = False
            except OSError as e:
                if e.errno == errno.EACCES:
                    logs_ok = False
            logs_ok = logs_ok and os.access(self.log_root_path, os.W_OK)
            if logs_ok:
                break
            self.__logger.warning(f'could not use location {self.log_root_path} for logs, trying another...')
        else:
            raise RuntimeError('could not initialize logs directory')
        self.__logger.info(f'using {self.log_root_path} for logs')

        self.__status = {}
        self.__scheduler_db_uid: int = 0  # unsigned 64bit int
        self.__running_process: Optional[asyncio.subprocess.Process] = None
        self.__running_process_start_time: float = 0
        self.__running_task: Optional[InvocationJob] = None
        self.__running_task_progress: Optional[float] = None
        self.__running_awaiter = None
        self.__previous_notrunning_awaiter = None  # here we will temporarily save running_awaiter before it is set to None again when task canceled or finished, to avoid task being GCd while in work
        self.__message_processor: Optional[WorkerMessageProcessor] = None
        self.__local_invocation_server: Optional[asyncio.Server] = None
        self.__local_invocation_server_address_string: str = ''

        self.__local_shared_dir = config.get_option_noasync("local_shared_dir_path", os.path.join(tempfile.gettempdir(), 'lifeblood_worker', 'shared'))
        self.__my_resources = WorkerResources(cpu_count=config.get_option_noasync('resources.cpu_count'),
                                              cpu_mem=config.get_option_noasync('resources.cpu_mem'),
                                              gpu_count=config.get_option_noasync('resources.gpu_count'),
                                              gpu_mem=config.get_option_noasync('resources.gpu_mem'))
        self.__task_changing_state_lock = asyncio.Lock()
        self.__stop_lock = threading.Lock()
        self.__start_lock = asyncio.Lock()  # cant use threading lock in async methods - it can yeild out, and deadlock on itself
        self.__where_to_report: Optional[AddressChain] = None
        self.__ping_interval = scheduler_ping_interval
        self.__ping_missed_threshold = scheduler_ping_miss_threshold
        self.__ping_missed = 0
        self.__scheduler_addr = scheduler_addr
        self.__scheduler_pinger = None
        self.__components_stop_event = asyncio.Event()
        self.__extra_files_base_dir = None
        self.__my_addr: Optional[AddressChain] = None
        self.__worker_id = worker_id
        self.__pool_address: Optional[AddressChain] = pool_address
        if self.__worker_id is None and self.__pool_address is not None \
                or self.__worker_id is not None and self.__pool_address is None:
            raise RuntimeError('pool_address must be given together with worker_id')

        self.__worker_task_comm_queues: Dict[str, asyncio.Queue] = {}

        self.__worker_type: WorkerType = worker_type
        self.__singleshot: bool = singleshot or worker_type == WorkerType.SCHEDULER_HELPER

        self.__child_priority_adjustment = child_priority_adjustment
        # this below is a placeholder solution. the easiest way to implement priority lowering without testing on different platrofms
        if self.__child_priority_adjustment == ProcessPriorityAdjustment.LOWER:
            if sys.platform.startswith('win'):
                assert hasattr(psutil, 'BELOW_NORMAL_PRIORITY_CLASS')
                psutil.Process().nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
            else:
                psutil.Process().nice(10)

        # deploy a copy of runtime module somewhere in temp
        rtmodule_code = inspect.getsource(lifeblood_connection)

        filepath = os.path.join(tempfile.gettempdir(), 'lifeblood', 'lifeblood_runtime', 'lifeblood_connection.py')
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        existing_code = None
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                existing_code = f.read()

        if existing_code != rtmodule_code:
            with open(filepath, 'w') as f:
                f.write(rtmodule_code)
        self.__rt_module_dir = os.path.dirname(filepath)

        self.__stopping_waiters = []
        self.__finished = asyncio.Event()
        self.__started = False
        self.__started_event = asyncio.Event()
        self.__stopped = False

    def message_processor(self) -> WorkerMessageProcessor:
        return self.__message_processor

    def scheduler_message_address(self) -> AddressChain:
        return self.__scheduler_addr

    async def start(self):
        if self.__started:
            return
        if self.__stopped:
            raise RuntimeError('already stopped, cannot start again')

        async with self.__start_lock:
            abort_start = False

            # start local server for invocation api connections
            loop = asyncio.get_event_loop()
            localhost = get_localhost()
            localport_start = 10101
            localport_end = 11111
            localport = None
            for _ in range(localport_end - localport_start):  # big but finite
                localport = random.randint(localport_start, localport_end)
                try:
                    self.__local_invocation_server = await loop.create_server(
                        lambda: WorkerInvocationServerProtocol(self, [WorkerInvocationProtocolHandlerV10(self)]),
                        localhost,
                        localport
                    )
                    break
                except OSError as e:
                    if e.errno != errno.EADDRINUSE:
                        raise
                    continue
            else:
                raise RuntimeError('could not find an opened port!')
            self.__local_invocation_server_address_string = f'{localhost}:{localport}'

            # start message processor
            my_ip = get_addr_to(self.__scheduler_addr.split_address()[0])
            my_port = default_worker_start_port()
            for i in range(1024):  # big but finite
                try:
                    self.__message_processor = WorkerMessageProcessor(self, (my_ip, my_port))
                    await self.__message_processor.start()
                    break
                except OSError as e:
                    if e.errno != errno.EADDRINUSE:
                        raise
                    my_port += 1
                    continue
            else:
                raise RuntimeError('could not find an opened port!')

            if self.__pool_address:
                self.__my_addr = AddressChain.join_address((self.__pool_address, DirectAddress.from_host_port(my_ip, my_port)))
            else:
                self.__my_addr = AddressChain.from_host_port(my_ip, my_port)

            # now report our address to the scheduler
            metadata = WorkerMetadata(get_hostname())
            try:
                with SchedulerWorkerControlClient.get_scheduler_control_client(self.__scheduler_addr, self.__message_processor) as client:  # type: SchedulerWorkerControlClient
                    self.__scheduler_db_uid = await client.say_hello(self.__my_addr, self.__worker_type, self.__my_resources, metadata)
            except MessageTransferError as e:
                self.__logger.error('error connecting to scheduler during start')
                abort_start = True
            #
            # and report to the pool
            try:
                if self.__worker_id is not None:
                    assert self.__pool_address is not None
                    with WorkerPoolControlClient.get_worker_pool_control_client(self.__pool_address, self.__message_processor) as wpclient:  # type: WorkerPoolControlClient
                        await wpclient.report_state(self.__worker_id, WorkerState.IDLE)
            except ConnectionError as e:
                self.__logger.error('error connecting to worker pool during start')
                abort_start = True

            self.__scheduler_pinger = asyncio.create_task(self.scheduler_pinger())
            self.__started = True
            self.__started_event.set()
        if abort_start:
            self.__logger.error('error during stating worker, aborting!')
            self.stop()
        else:
            self.__logger.info('worker started')

    def is_started(self):
        return self.__started

    def wait_till_starts(self):  # we can await this function cuz it returns a future...
        return self.__started_event.wait()

    def stop(self):
        async def _send_byebye():
            try:
                self.__logger.debug('saying bye to scheduler')
                with SchedulerWorkerControlClient.get_scheduler_control_client(self.__scheduler_addr, self.__message_processor) as client:  # type: SchedulerWorkerControlClient
                    await client.say_bye(self.__my_addr)
            except MessageTransferError:  # if scheduler or route is down
                self.__logger.info('couldn\'t say bye to scheduler as it seem to be down')
            except Exception:
                self.__logger.exception('couldn\'t say bye to scheduler for unknown reason')

        if not self.__started or self.__stopped:
            return
        with self.__stop_lock:  # NOTE: there is literally no threading in worker, so this is excessive
            self.__logger.info('STOPPING WORKER')
            self.__components_stop_event.set()

            async def _finalizer():
                await self.__scheduler_pinger  # to ensure pinger stops and won't try to contact scheduler any more
                await self.cancel_task()  # then we cancel task, here we still can report it to the scheduler.
                # no new tasks will be picked up cuz __stopped is already set
                self.__local_invocation_server.close()
                await _send_byebye()  # saying bye, don't bother us. (some delayed comms may still come through to the __server
                await self.__local_invocation_server.wait_closed()
                self.__message_processor.stop()
                await self.__message_processor.wait_till_stops()
                self.__logger.info('message processor stopped')

            self.__stopping_waiters.append(asyncio.create_task(_finalizer()))
            self.__finished.set()
            self.__stopped = True

    async def wait_till_stops(self):
        # if self.__scheduler_pinger is not None:
        #     #try:
        #     await self.__scheduler_pinger
        #     #except asyncio.CancelledError:
        #     #    self.__logger.debug('wait_to_finished: scheduler_pinger was cancelled')
        #     #    #raise
        #     self.__scheduler_pinger = None
        # await self.__server.wait_closed()
        await self.__finished.wait()
        self.__logger.info('server closed')
        await self.__scheduler_pinger
        self.__logger.info('pinger closed')
        for waiter in self.__stopping_waiters:
            await waiter

    def get_log_filepath(self, level, invocation_id: int = None):  # TODO: think of a better, more generator-style way of returning logs
        if self.__running_task is None and invocation_id is None:
            return os.path.join(self.log_root_path, f'db_{self.__scheduler_db_uid:016x}', 'common', level)
        else:
            return os.path.join(self.log_root_path, f'db_{self.__scheduler_db_uid:016x}', 'invocations', str(invocation_id or self.__running_task.invocation_id()), level)

    async def delete_logs(self, invocation_id: int):
        self.__logger.debug(f'removing logs for {invocation_id}')
        path = os.path.join(self.log_root_path, f'db_{self.__scheduler_db_uid:016x}', 'invocations', str(invocation_id or self.__running_task.invocation_id()))
        await asyncio.get_event_loop().run_in_executor(None, shutil.rmtree, path)  # assume that deletion MAY take time, so allow util tasks to be processed while we wait

    async def run_task(self, task: InvocationJob, report_to: AddressChain):
        if self.__stopped:
            raise WorkerNotAvailable()
        self.__logger.debug(f'locks are {self.__task_changing_state_lock.locked()}')
        async with self.__task_changing_state_lock:
            self.__logger.debug('run_task: task_change_state locks acquired')
            # we must ensure picking up and finishing tasks is in critical section
            assert len(task.args()) > 0
            if self.__running_process is not None:
                raise AlreadyRunning('Task already in progress')

            # prepare logging
            self.__logger.info(f'running task {task}')

            # save external files
            self.__extra_files_base_dir = None
            extra_files_map: Dict[str, str] = {}
            if len(task.extra_files()) > 0:
                self.__extra_files_base_dir = tempfile.mkdtemp(prefix='lifeblood_efs_')  # TODO: add base temp dir to config
                self.__logger.debug(f'creating extra file temporary dir at {self.__extra_files_base_dir}')
            for exfilepath, exfiledata in task.extra_files().items():
                self.__logger.info(f'saving extra job file {exfilepath}')
                exfilepath_parts = exfilepath.split('/')
                tmpfilepath = os.path.join(self.__extra_files_base_dir, *exfilepath_parts)
                os.makedirs(os.path.dirname(tmpfilepath), exist_ok=True)
                with open(tmpfilepath, 'w' if isinstance(exfiledata, str) else 'wb') as f:
                    f.write(exfiledata)
                extra_files_map[exfilepath] = tmpfilepath

            # check args for extra file references
            if len(task.extra_files()) > 0:
                args = []
                for arg in task.args():
                    if isinstance(arg, str) and arg.startswith(':/') and arg[2:] in task.extra_files():
                        args.append(extra_files_map[arg[2:]])
                    else:
                        args.append(arg)
            else:
                args = task.args()

            try:
                if task.environment_resolver_arguments() is None:
                    config = get_config('worker')
                    env = environment_resolver.get_resolver(config.get_option_noasync('default_env_wrapper.name', 'TrivialEnvironmentResolver'))\
                        .get_environment(config.get_option_noasync('default_env_wrapper.arguments', {}))
                else:
                    env = task.environment_resolver_arguments().get_environment()
            except environment_resolver.ResolutionImpossibleError as e:
                self.__logger.error(f'cannot run the task: Unable to resolve environment: {str(e)}')
                raise

            # TODO: resolver args get_environment() acually does resolution so should be renamed to like resolve_environment()
            #  Environment's resolve() actually just expands and merges everything, so naming it "resolve" is misleading next to EnvironmentResolver

            env = task.env().resolve(env)

            env.prepend('PYTHONPATH', self.__rt_module_dir)
            env['LIFEBLOOD_RUNTIME_IID'] = task.invocation_id()
            env['LIFEBLOOD_RUNTIME_TID'] = task.task_id()
            env['LIFEBLOOD_RUNTIME_SCHEDULER_ADDR'] = self.__local_invocation_server_address_string
            for aname, aval in task.attributes().items():
                if aname.startswith('_'):  # skip attributes starting with _
                    continue
                # TODO: THINK OF A BETTER LOGIC !
                if isinstance(aval, (str, int, float)):
                    env[f'LBATTR_{aname}'] = str(aval)

            # env['LBATTRS_JSON'] = json.dumps(dict(task.attributes()))
            if self.__extra_files_base_dir is not None:
                env['LB_EF_ROOT'] = self.__extra_files_base_dir
            try:
                #with open(self.get_log_filepath('output', task.invocation_id()), 'a') as stdout:
                #    with open(self.get_log_filepath('error', task.invocation_id()), 'a') as stderr:
                # TODO: proper child process priority adjustment should be done, for now it's implemented in constructor.
                self.__running_process_start_time = time.time()

                if os.path.isabs(args[0]):
                    bin_path = args[0]
                else:
                    bin_path = shutil.which(args[0], path=env.get('PATH'))
                if bin_path is None:
                    raise ProcessInitializationError(f'"{args[0]}" was not found. Check environment resolver arguments and system setup')

                self.__running_process: asyncio.subprocess.Process = await create_process(args, env, os.path.dirname(bin_path))
            except Exception as e:
                self.__logger.exception('task creation failed with error: %s' % (repr(e),))
                raise

            self.__running_task = task
            self.__running_awaiter = asyncio.create_task(self._awaiter())
            self.__running_task_progress = 0
            if self.__worker_id is not None:  # TODO: gracefully handle connection fails here \/
                assert self.__pool_address is not None
                self.__where_to_report = AddressChain.join_address((self.__pool_address, report_to))
                with WorkerPoolControlClient.get_worker_pool_control_client(self.__pool_address, self.__message_processor) as wpclient:  # type: WorkerPoolControlClient
                    await wpclient.report_state(self.__worker_id, WorkerState.BUSY)
            else:
                self.__where_to_report = report_to

    # callback awaiter
    async def _awaiter(self):
        stdout_path = self.get_log_filepath('output', self.__running_task.invocation_id())
        stderr_path = self.get_log_filepath('error', self.__running_task.invocation_id())
        os.makedirs(os.path.dirname(stdout_path), exist_ok=True)
        os.makedirs(os.path.dirname(stderr_path), exist_ok=True)
        async with aiofiles.open(stdout_path, 'wb') as stdout:
            async with aiofiles.open(stderr_path, 'wb') as stderr:
                async def _flush():
                    await asyncio.sleep(1)  # ensure to flush every 1 second
                    await stdout.flush()
                    await stderr.flush()

                await stdout.write(datetime.datetime.now().strftime('[SYS][%d.%m.%y %H:%M:%S] task initialized\n').encode('UTF-8'))
                tasks_to_wait = {}
                try:
                    rout_task = asyncio.create_task(self.__running_process.stdout.readline())
                    rerr_task = asyncio.create_task(self.__running_process.stderr.readline())
                    done_task = asyncio.create_task(self.__running_process.wait())
                    flush_task = asyncio.create_task(_flush())
                    tasks_to_wait = {rout_task, rerr_task, done_task, flush_task}
                    while len(tasks_to_wait) != 0:
                        done, tasks_to_wait = await asyncio.wait(tasks_to_wait, return_when=asyncio.FIRST_COMPLETED)
                        if rout_task in done:
                            str = rout_task.result()
                            progress = self.__running_task.match_stdout_progress(str)
                            if progress is not None:
                                self.__running_task_progress = progress
                            if str != b'':  # this can only happen at eof
                                await stdout.write(datetime.datetime.now().strftime('[OUT][%H:%M:%S] ').encode('UTF-8') + str)
                                rout_task = asyncio.create_task(self.__running_process.stdout.readline())
                                tasks_to_wait.add(rout_task)
                        if rerr_task in done:
                            str = rerr_task.result()
                            progress = self.__running_task.match_stderr_progress(str)
                            if progress is not None:
                                self.__running_task_progress = progress
                            if str != b'':  # this can only happen at eof
                                message = datetime.datetime.now().strftime('[ERR][%H:%M:%S] ').encode('UTF-8') + str
                                await asyncio.gather(
                                    stderr.write(message),
                                    stdout.write(message)
                                )
                                rerr_task = asyncio.create_task(self.__running_process.stderr.readline())
                                tasks_to_wait.add(rerr_task)
                        if flush_task in done and not done_task.done():
                            flush_task = asyncio.create_task(_flush())
                            tasks_to_wait.add(flush_task)
                    await stdout.write(datetime.datetime.now().strftime('[SYS][%d.%m.%y %H:%M:%S] task finished\n').encode('UTF-8'))
                except asyncio.CancelledError:
                    self.__logger.debug('task awaiter was cancelled')
                    for task in tasks_to_wait:
                        task.cancel()
                    raise
                finally:
                    # report to the pool
                    if self.__worker_id is not None:
                        try:
                            assert self.__pool_address is not None
                            with WorkerPoolControlClient.get_worker_pool_control_client(self.__pool_address, self.__message_processor) as wpclient:  # type: WorkerPoolControlClient
                                await wpclient.report_state(self.__worker_id, WorkerState.IDLE)
                        except (Exception, asyncio.CancelledError):
                            self.__logger.error('failed to report task cancellation to worker pool. stopping worker')
                            self.stop()

        await self.__running_process.wait()
        await self.task_finished()

    def is_task_running(self) -> bool:
        return self.__running_task is not None

    def running_invocation(self) -> Optional[InvocationJob]:
        return self.__running_task

    async def deliver_invocation_message(self, destination_invocation_id: int, destination_addressee: str, source_invocation_id: Optional[int], message_body: bytes, addressee_timeout: float = 90.0):
        """
        deliver message to task
        """
        while True:
            # while we wait - invocation MAY change.
            running_invocation = self.running_invocation()
            if running_invocation is None or destination_invocation_id != running_invocation.invocation_id():
                raise InvocationMessageWrongInvocationId()

            while destination_addressee not in self.__worker_task_comm_queues:
                wait_start_timestamp = time.time()
                await asyncio.sleep(0.05)  # we MOST LIKELY are already waiting for this, so timeout occurs
                addressee_timeout -= time.time() - wait_start_timestamp
                if addressee_timeout <= 0:
                    raise InvocationMessageAddresseeTimeout()
                # important to keep checking if invocation was changed,
                # and important to have no awaits (no interruptions) between check and enqueueing
                running_invocation = self.running_invocation()
                if running_invocation is None or destination_invocation_id != running_invocation.invocation_id():
                    raise InvocationMessageWrongInvocationId()

            queue = self.__worker_task_comm_queues[destination_addressee]

            if not queue.empty():
                # need to return control to loop in case 2 deliver_invocation_message calls happen to happen at the same time,
                # and one is stuck in the loop of upper while being satisfied, but queue not empty already
                await asyncio.sleep(0.01)
                continue
            queue.put_nowait((source_invocation_id, message_body))
            queue.put_nowait(())
            break

    async def worker_task_addressee_wait(self, addressee: str, timeout: float = 30) -> Tuple[int, bytes]:
        """
        wait for a data message to addressee to be delivered

        :returns: sender invocation id, message body
        """
        if addressee not in self.__worker_task_comm_queues:
            self.__worker_task_comm_queues[addressee] = asyncio.Queue()
        try:
            value = await asyncio.wait_for(self.__worker_task_comm_queues[addressee].get(), timeout=timeout)
            assert self.__worker_task_comm_queues[addressee].get_nowait() == ()
            # this way above we ensure one single deliver_task deliver to one single addressee_wait
        finally:
            self.__worker_task_comm_queues.pop(addressee)

        return value

    def is_stopping(self) -> bool:
        """
        returns True is stop was called on worker,
        so worker is closed or in the process of closing
        """
        return self.__stopped

    async def cancel_task(self):
        async with self.__task_changing_state_lock:
            self.__logger.debug('cancel_task: task_change_state locks acquired')
            if self.__running_process is None:
                return
            self.__logger.info('cancelling running task')
            self.__running_awaiter.cancel()
            cancelling_awaiter = self.__running_awaiter
            self.__running_awaiter = None

            await kill_process_tree(self.__running_process)
            self.__running_task.finish(None, time.time() - self.__running_process_start_time)

            self.__running_process._transport.close()  # sometimes not closed straight away transport ON EXIT may cause exceptions in __del__ that event loop is closed

            # report to scheduler that cancel was a success
            self.__logger.info(f'reporting cancel back to {self.__where_to_report}')

            proc_stdout_filepath = self.get_log_filepath('output', self.__running_task.invocation_id())
            proc_stderr_filepath = self.get_log_filepath('error', self.__running_task.invocation_id())

            # we want to append worker's message that job was killed
            try:
                message = datetime.datetime.now().strftime('\n[WORKER][%d.%m.%y %H:%M:%S] ').encode('UTF-8') + b'killed by worker.\n'
                async with aiofiles.open(proc_stdout_filepath, 'ab') as stdout, \
                           aiofiles.open(proc_stderr_filepath, 'ab') as stderr:
                    await asyncio.gather(
                        stderr.write(message),
                        stdout.write(message)
                    )
            except Exception as e:
                self.__logger.warning("failed to append worker message to the logs")

            try:
                with SchedulerWorkerControlClient.get_scheduler_control_client(self.__where_to_report, self.__message_processor) as client:  # type: SchedulerWorkerControlClient
                    await client.report_task_canceled(self.__running_task,
                                                      proc_stdout_filepath,
                                                      proc_stderr_filepath)
            except Exception as e:
                self.__logger.exception(f'could not report cuz of {e}')
            except:
                self.__logger.exception('could not report cuz i have no idea')
            # end reporting

            try:
                await self.delete_logs(self.__running_task.invocation_id())
            except OSError:
                self.__logger.exception("failed to delete logs, ignoring")

            self.__running_task = None
            self.__worker_task_comm_queues = {}
            self.__running_process = None
            self.__where_to_report = None
            self.__running_task_progress = None
            await self._cleanup_extra_files()

            await asyncio.wait((cancelling_awaiter,))  # ensure everything is done before we proceed

            # stop ourselves if we are a small task helper
            if self.__singleshot:
                self.stop()

    def task_status(self) -> Optional[float]:
        return self.__running_task_progress

    async def task_finished(self):
        """
        is called when current process finishes
        :return:
        """
        async with self.__task_changing_state_lock:
            self.__logger.debug('task_finished: task_change_state locks acquired')
            if self.__running_process is None:
                self.__logger.warning('task_finished called, but there is no running task. This can only normally happen if a task_cancel happened the same moment as finish.')
                return
            self.__logger.info('task finished')
            process_exit_code = await self.__running_process.wait()
            self.__running_task.finish(process_exit_code, time.time() - self.__running_process_start_time)

            # report to scheduler
            self.__logger.info(f'reporting done back to {self.__where_to_report}')
            try:
                with SchedulerWorkerControlClient.get_scheduler_control_client(self.__where_to_report, self.__message_processor) as client:  # type: SchedulerWorkerControlClient
                    await client.report_task_done(self.__running_task,
                                                  self.get_log_filepath('output', self.__running_task.invocation_id()),
                                                  self.get_log_filepath('error', self.__running_task.invocation_id()))
            except Exception as e:
                self.__logger.exception(f'could not report cuz of {e}')
            except:
                self.__logger.exception('could not report cuz i have no idea')
            # end reporting
            self.__logger.debug(f'done reporting done back to {self.__where_to_report}')

            try:
                await self.delete_logs(self.__running_task.invocation_id())
            except OSError:
                self.__logger.exception("failed to delete logs, ignoring")

            self.__where_to_report = None
            self.__running_task = None
            self.__worker_task_comm_queues = {}
            self.__running_process = None
            self.__previous_notrunning_awaiter = self.__running_awaiter  # this is JUST so task is not GCd
            self.__running_awaiter = None  # TODO: lol, this function can be called from awaiter, and if we hand below - awaiter can be gcd, and it's all fucked
            self.__running_task_progress = None
            await self._cleanup_extra_files()

            # stop ourselves if we are a small task helper
            if self.__singleshot:
                self.stop()

    async def _cleanup_extra_files(self):
        """
        cleanup extra files transfered with the task
        :return:
        """
        if self.__extra_files_base_dir is None:
            return
        try:
            shutil.rmtree(self.__extra_files_base_dir)
        except:
            self.__logger.exception('could not cleanup extra files')

    #
    # simply ping scheduler once in a while
    async def scheduler_pinger(self):
        """
        ping scheduler once in a while. if it misses too many pings - close worker and wait for new broadcasts
        :return:
        """

        async def _reintroduce_ourself():
            for attempt in range(5):
                self.__logger.debug(f'trying to reintroduce myself, attempt: {attempt + 1}')
                metadata = WorkerMetadata(get_hostname())
                try:
                    with SchedulerWorkerControlClient.get_scheduler_control_client(self.__scheduler_addr, self.__message_processor) as client:  # type: SchedulerWorkerControlClient
                        assert self.__my_addr is not None
                        addr = self.__my_addr
                        self.__logger.debug('saying bye')
                        await client.say_bye(addr)
                        self.__logger.debug('cancelling task')
                        await self.cancel_task()
                        self.__logger.debug('saying hello')
                        self.__scheduler_db_uid = await client.say_hello(addr, self.__worker_type, self.__my_resources, metadata)
                        self.__logger.debug('reintroduce done')
                    break
                except Exception:
                    self.__logger.exception('failed to reintroduce myself. sleeping a bit and retrying')
                    await asyncio.sleep(10)
            else:  # failed to reintroduce. consider that something is wrong with the network, stop
                self.__logger.error('failed to reintroduce myself. assuming network problems, exiting')
                self.stop()

        exit_wait = asyncio.create_task(self.__components_stop_event.wait())
        while True:
            done, pend = await asyncio.wait((exit_wait, ), timeout=self.__ping_interval, return_when=asyncio.FIRST_COMPLETED)
            if exit_wait in done:
                await exit_wait
                break
            #await asyncio.sleep(self.__ping_interval)
            if self.__ping_missed_threshold == 0:
                continue
            # Here we are locking to prevent unexpected task state changes while checking for state inconsistencies
            async with self.__task_changing_state_lock:
                self.__logger.debug('pinger: task_change_state locks acquired')
                try:
                    self.__logger.debug('pinging scheduler')
                    with SchedulerWorkerControlClient.get_scheduler_control_client(self.__scheduler_addr, self.__message_processor) as client:  # type: SchedulerWorkerControlClient
                        result = await client.ping(self.__my_addr)
                    self.__logger.debug(f'scheduler pinged: sees me as {result}')
                except MessageTransferError as mte:
                    self.__logger.error('ping message delivery failed')
                    result = None
                except Exception as e:
                    self.__logger.exception('unexpected exception happened')
                    result = None
                task_running = self.is_task_running()

            if result is None:  # this means EOF
                self.__ping_missed += 1
                self.__logger.info(f'server ping missed. total misses: {self.__ping_missed}')
            if self.__ping_missed >= self.__ping_missed_threshold:
                # assume scheruler down, drop everything and look for another scheruler
                self.stop()
                return

            if result in (WorkerState.OFF, WorkerState.UNKNOWN):
                # something is wrong, lets try to reintroduce ourselves.
                # Note that we can be sure that there cannot be race conditions here:
                # pinger starts working always AFTER hello, OR it saz hello itself.
                # and scheduler will immediately switch worker state on hello, so ping coming after confirmed hello will ALWAYS get newer state
                self.__logger.warning(f'scheduler replied it thinks i\'m {result.name}. canceling tasks if any and reintroducing myself')
                await _reintroduce_ourself()
            elif result == WorkerState.BUSY and not task_running:
                # Note: the order is:
                # - sched sets worker to INVOKING
                # - shced sends "task"
                # - worker receives task, sets is_task_running
                # - worker answers to sched
                # - sched sets worker to BUSY
                # and when finished:
                # - worker reports done             |
                # - sched sets worker to IDLE       | under __task_changing_state_lock
                # - worker unsets is_task_running   |
                # so there is no way it can be not task_running AND sched state busy.
                # if it is - it must be an error
                self.__logger.warning(f'scheduler replied it thinks i\'m BUSY, but i\'m free, so something is inconsistent. resolving by reintroducing myself')
                await _reintroduce_ourself()
            elif result == WorkerState.IDLE and task_running:
                # Note from scheme above - this is not possible,
                #  the only period where scheduler can think IDLE while is_task_running set is in __task_changing_state_lock-ed area
                #  but we aquired sched state and our is_task_running above inside that __task_changing_state_lock
                self.__logger.warning(f'scheduler replied it thinks i\'m IDLE, but i\'m doing a task, so something is inconsistent. resolving by reintroducing myself')
                await _reintroduce_ourself()
            elif result == WorkerState.ERROR:
                # currently the only way it can be error is because of shitty network
                # ideally here we would check ourselves
                # but there's nothing to check right now
                self.__logger.warning('scheduler replied it thinks i\'m ERROR, but i\'m doing fine. probably something is wrong with the network. waiting for scheduler to resolve the problem')
                # no we don't reintroduce - error state on scheduler side just means he won't give us tasks for now
                # and since error is most probably due to network - it will either resolve itself, or there is no point reintroducing if connection cannot be established anyway
            elif result is not None:
                self.__ping_missed = 0
