import os
import errno
import shutil
import traceback
import threading
import asyncio
import subprocess
import aiofiles
import json
import psutil
import datetime
import tempfile
import signal
from . import logging
from .nethelpers import get_addr_to, get_default_addr, get_localhost, address_to_ip_port
from .net_classes import WorkerResources
from .worker_task_protocol import WorkerTaskServerProtocol, AlreadyRunning
from .scheduler_task_protocol import SchedulerTaskClient
from .worker_pool_protocol import WorkerPoolClient
from .broadcasting import await_broadcast
from .invocationjob import InvocationJob
from .config import get_config, create_default_user_config_file
from . import environment_resolver
from .enums import WorkerType, WorkerState
from .paths import config_path

from .worker_runtime_pythonpath import lifeblood_connection
import inspect

from typing import Optional, Dict, Tuple


async def create_worker(scheduler_ip: str, scheduler_port: int, *, worker_type: WorkerType = WorkerType.STANDARD, singleshot: bool = False, worker_id: Optional[int] = None, pool_address: Optional[Tuple[str, int]] = None):
    worker = Worker(scheduler_ip, scheduler_port, worker_type=worker_type, singleshot=singleshot, worker_id=worker_id, pool_address=pool_address)

    await worker.start()  # note that server is already started at this point
    return worker


class Worker:
    def __init__(self, scheduler_addr: str, scheduler_port: int, worker_type: WorkerType = WorkerType.STANDARD, singleshot: bool = False, worker_id: Optional[int] = None, pool_address: Optional[Tuple[str, int]] = None):
        """

        :param scheduler_addr:
        :param scheduler_port:
        :param worker_type:
        :param singleshot:
        """
        config = get_config('worker')
        self.__logger = logging.get_logger('worker')
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
        self.__running_process: Optional[asyncio.subprocess.Process] = None
        self.__running_task: Optional[InvocationJob] = None
        self.__running_task_progress: Optional[float] = None
        self.__running_awaiter = None
        self.__server: asyncio.AbstractServer = None
        self.__task_changing_state_lock = asyncio.Lock()
        self.__stop_lock = threading.Lock()
        self.__start_lock = asyncio.Lock()  # cant use threading lock in async methods - it can yeild out, and deadlock on itself
        self.__where_to_report = None
        self.__ping_interval = 10
        self.__ping_missed_threshold = 6
        self.__ping_missed = 0
        self.__scheduler_addr = (scheduler_addr, scheduler_port)
        self.__scheduler_pinger = None
        self.__scheduler_pinger_stop_event = asyncio.Event()
        self.__extra_files_base_dir = None
        self.__my_addr: Optional[Tuple[str, int]] = None
        self.__worker_id = worker_id
        if pool_address is None:
            self.__pool_address = (get_localhost(), 7959)
        else:
            self.__pool_address: Tuple[str, int] = pool_address

        self.__worker_type: WorkerType = worker_type
        self.__singleshot: bool = singleshot or worker_type == WorkerType.SCHEDULER_HELPER

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

    async def start(self):
        if self.__started:
            return
        if self.__stopped:
            raise RuntimeError('already stopped, cannot start again')

        async with self.__start_lock:
            loop = asyncio.get_event_loop()
            my_ip = get_addr_to(self.__scheduler_addr[0])
            my_port = 6969
            for i in range(1024):  # big but finite
                try:
                    self.__server = await loop.create_server(lambda: WorkerTaskServerProtocol(self), my_ip, my_port, backlog=16)
                    addr = f'{my_ip}:{my_port}'
                    break
                except OSError as e:
                    if e.errno != errno.EADDRINUSE:
                        raise
                    my_port += 1
                    continue
            else:
                raise RuntimeError('could not find an opened port!')
            self.__my_addr = (my_ip, my_port)

            # now report our address to the scheduler
            async with SchedulerTaskClient(*self.__scheduler_addr) as client:
                await client.say_hello(addr, self.__worker_type, WorkerResources())
            #
            # and report to the pool
            if self.__worker_id is not None:
                async with WorkerPoolClient(*self.__pool_address, worker_id=self.__worker_id) as client:
                    await client.report_state(WorkerState.IDLE)

            self.__scheduler_pinger = asyncio.create_task(self.scheduler_pinger())
            self.__started = True
            self.__started_event.set()

    def is_started(self):
        return self.__started

    def wait_till_starts(self):  # we can await this function cuz it returns a future...
        return self.__started_event.wait()

    def stop(self):
        async def _send_byebye():
            try:
                async with SchedulerTaskClient(*self.__scheduler_addr) as client:
                    await client.say_bye('%s:%d' % self.__my_addr)
            except ConnectionRefusedError:  # if scheduler is down
                self.__logger.info('couldn\'t say bye to scheduler as it seem to be down')
            except Exception:
                self.__logger.exception('couldn\'t say bye to scheduler for unknown reason')

        if not self.__started or self.__stopped:
            return
        with self.__stop_lock:
            self.__logger.info('STOPPING WORKER')
            self.__server.close()
            self.__scheduler_pinger_stop_event.set()

            async def _finalizer():
                await self.__scheduler_pinger  # to ensure pinger stops and won't try to contact scheduler any more
                await self.__server.wait_closed()  # before doing anything else we wait for server to fully close all connections
                await self.cancel_task()  # then we cancel task, here we still can report it to the scheduler
                await _send_byebye()  # and only after that we report OFF to scheduler

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
        await self.__server.wait_closed()
        await self.__scheduler_pinger
        for waiter in self.__stopping_waiters:
            await waiter

    def get_log_filepath(self, level, invocation_id: int = None):  # TODO: think of a better, more generator-style way of returning logs
        if self.__running_task is None and invocation_id is None:
            return os.path.join(self.log_root_path, 'common', level)
        else:
            return os.path.join(self.log_root_path, 'invocations', str(invocation_id or self.__running_task.invocation_id()), level)

    async def run_task(self, task: InvocationJob, report_to: str):
        async with self.__task_changing_state_lock:
            assert len(task.args()) > 0
            if self.__running_process is not None:
                raise AlreadyRunning('Task already in progress')
            # prepare logging
            self.__logger.info(f'running task {task}')
            logbasedir = os.path.dirname(self.get_log_filepath('output', task.invocation_id()))

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

            if task.environment_resolver_arguments() is None:
                config = get_config('worker')
                env = environment_resolver.get_resolver(config.get_option_noasync('default_env_wrapper.name', 'TrivialEnvironmentResolver'))\
                    .get_environment(config.get_option_noasync('default_env_wrapper.arguments', {}))
            else:
                env = task.environment_resolver_arguments().get_environment()

            env = task.env().resolve(env)

            env.prepend('PYTHONPATH', self.__rt_module_dir)
            env['LIFEBLOOD_RUNTIME_IID'] = task.invocation_id()
            env['LIFEBLOOD_RUNTIME_SCHEDULER_ADDR'] = report_to
            for aname, aval in task.attributes().items():
                env['LBATTR_%s' % aname] = str(aval)
            env['LBATTRS_JSON'] = json.dumps(dict(task.attributes())).encode('UTF-8')
            if self.__extra_files_base_dir is not None:
                env['LB_EF_ROOT'] = self.__extra_files_base_dir
            if not os.path.exists(logbasedir):
                os.makedirs(logbasedir)
            try:
                #with open(self.get_log_filepath('output', task.invocation_id()), 'a') as stdout:
                #    with open(self.get_log_filepath('error', task.invocation_id()), 'a') as stderr:
                self.__running_process: asyncio.subprocess.Process = \
                    await asyncio.create_subprocess_exec(
                        *args,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        env=env
                    )
            except Exception as e:
                self.__logger.exception('task creation failed with error: %s' % (repr(e),))
                raise

            self.__running_task = task
            self.__where_to_report = report_to
            if self.__worker_id is not None:
                async with WorkerPoolClient(*self.__pool_address, worker_id=self.__worker_id) as client:
                    await client.report_state(WorkerState.BUSY)
            self.__running_awaiter = asyncio.create_task(self._awaiter())
            self.__running_task_progress = 0

    # callback awaiter
    async def _awaiter(self):
        async with aiofiles.open(self.get_log_filepath('output', self.__running_task.invocation_id()), 'ab') as stdout:
            async with aiofiles.open(self.get_log_filepath('error', self.__running_task.invocation_id()), 'ab') as stderr:

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
                                await asyncio.gather(stderr.write(datetime.datetime.now().strftime('[ERR][%H:%M:%S] ').encode('UTF-8') + str),
                                                     stdout.write(datetime.datetime.now().strftime('[ERR][%H:%M:%S] ').encode('UTF-8') + str))
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
                        async with WorkerPoolClient(*self.__pool_address, worker_id=self.__worker_id) as client:
                            await client.report_state(WorkerState.IDLE)

        await self.__running_process.wait()
        await self.task_finished()

    def is_task_running(self) -> bool:
        return self.__running_task is not None

    async def cancel_task(self):
        async with self.__task_changing_state_lock:
            if self.__running_process is None:
                return
            self.__logger.info('cancelling running task')
            self.__running_awaiter.cancel()
            cancelling_awaiter = self.__running_awaiter
            self.__running_awaiter = None
            try:
                puproc = psutil.Process(self.__running_process.pid)
            except psutil.NoSuchProcess:
                self.__logger.info(f'cannot find process with pid {self.__running_process.pid}. Assuming it finished. retcode={self.__running_process.returncode}')
            else:
                all_proc = puproc.children(recursive=True)
                all_proc.append(puproc)
                for proc in all_proc:
                    try:
                        proc.terminate()
                    except psutil.NoSuchProcess:
                        pass
                for i in range(20):  # TODO: make a parameter out of this!
                    if not all(not proc.is_running() for proc in all_proc):
                        await asyncio.sleep(0.5)
                    else:
                        break
                else:
                    for proc in all_proc:
                        if not proc.is_running():
                            continue
                        try:
                            proc.kill()
                        except psutil.NoSuchProcess:
                            pass

            await self.__running_process.wait()

            # report to scheduler that cancel was a success
            self.__logger.info(f'reporting cancel back to {self.__where_to_report}')
            try:
                ip, port = self.__where_to_report.split(':', 1)
                async with SchedulerTaskClient(ip, int(port)) as client:
                    await client.report_task_canceled(self.__running_task,
                                                      self.get_log_filepath('output', self.__running_task.invocation_id()),
                                                      self.get_log_filepath('error', self.__running_task.invocation_id()))
            except Exception as e:
                self.__logger.exception(f'could not report cuz of {e}')
            except:
                self.__logger.exception('could not report cuz i have no idea')
            # end reporting

            self.__running_task = None
            self.__running_process = None
            self.__where_to_report = None
            self.__running_task_progress = None
            await self._cleanup_extra_files()

            await asyncio.wait((cancelling_awaiter,))  # ensure everything is done before we proceed

            # stop ourselves if we are a small task helper
            if self.__singleshot:
                self.stop()

    async def task_status(self) -> Optional[float]:
        return self.__running_task_progress

    async def task_finished(self):
        """
        is called when current process finishes
        :return:
        """
        async with self.__task_changing_state_lock:
            if self.__running_process is None:
                self.__logger.warning('task_finished called, but there is no running task. This can only normally happen if a task_cancel happened the same moment as finish.')
                return
            self.__logger.info('task finished')
            self.__logger.info(f'reporting done back to {self.__where_to_report}')
            self.__running_task.finish(await self.__running_process.wait())
            try:
                ip, port = self.__where_to_report.split(':', 1)
                async with SchedulerTaskClient(ip, int(port)) as client:
                    await client.report_task_done(self.__running_task,
                                                  self.get_log_filepath('output', self.__running_task.invocation_id()),
                                                  self.get_log_filepath('error', self.__running_task.invocation_id()))
            except Exception as e:
                self.__logger.exception(f'could not report cuz of {e}')
            except:
                self.__logger.exception('could not report cuz i have no idea')
            self.__where_to_report = None
            self.__running_task = None
            self.__running_process = None
            self.__running_awaiter = None
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
                try:
                    async with SchedulerTaskClient(*self.__scheduler_addr) as client:
                        assert self.__my_addr is not None
                        addr = '%s:%d' % self.__my_addr
                        await client.say_bye(addr)
                        await self.cancel_task()
                        await client.say_hello(addr, self.__worker_type, WorkerResources())
                    break
                except Exception:
                    self.__logger.exception('failed to reintroduce myself. sleeping a bit and retrying')
                    await asyncio.sleep(10)
            else:  # failed to reintroduce. consider that something is wrong with the network, stop
                self.__logger.error('failed to reintroduce myself. assuming network problems, exiting')
                self.stop()

        exit_wait = asyncio.create_task(self.__scheduler_pinger_stop_event.wait())
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
                try:
                    async with SchedulerTaskClient(*self.__scheduler_addr) as client:
                        result = await client.ping(f'{self.__my_addr[0]}:{self.__my_addr[1]}')
                except ConnectionRefusedError as e:
                    self.__logger.error('scheduler ping connection was refused')
                    result = None
                except ConnectionResetError as e:
                    self.__logger.error('scheduler ping connection was reset')
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
            elif result is not None:
                self.__ping_missed = 0


async def main_async(worker_type=WorkerType.STANDARD, singleshot: bool = False, worker_id: Optional[int] = None, pool_address=None, noloop=False):
    """
    listen to scheduler broadcast in a loop.
    if received - create the worker and work
    if worker cannot ping the scheduler a number of times - it stops
    and listenting for broadcast starts again
    :return: Never!
    """

    def graceful_closer():
        nonlocal noloop
        noloop = True
        stop_event.set()
        if worker is not None:
            worker.stop()

    worker = None
    stop_event = asyncio.Event()
    asyncio.get_event_loop().add_signal_handler(signal.SIGINT, graceful_closer)
    asyncio.get_event_loop().add_signal_handler(signal.SIGTERM, graceful_closer)
    config = get_config('worker')
    logger = logging.get_logger('worker')
    if await config.get_option('worker.listen_to_broadcast', True):
        stop_task = asyncio.create_task(stop_event.wait())
        while True:
            logger.info('listening for scheduler broadcasts...')
            broadcast_task = asyncio.create_task(await_broadcast('lifeblood_scheduler'))
            done, _ = await asyncio.wait((broadcast_task, stop_task), return_when=asyncio.FIRST_COMPLETED)
            if stop_task in done:
                broadcast_task.cancel()
                logger.info('broadcast listening cancelled')
                break
            assert broadcast_task.done()
            message = await broadcast_task
            scheduler_info = json.loads(message)
            logger.debug('received', scheduler_info)
            addr = scheduler_info['worker']
            ip, sport = addr.split(':')  # TODO: make a proper protocol handler or what? at least ip/ipv6
            port = int(sport)
            try:
                worker = await create_worker(ip, port, worker_type=worker_type, singleshot=singleshot, worker_id=worker_id, pool_address=pool_address)
            except Exception:
                logger.exception('could not start the worker')
            else:
                await worker.wait_till_stops()
                logger.info('worker quited')
            if noloop:
                break
    else:
        logger.info('boradcast listening disabled')
        while True:
            ip = await config.get_option('worker.scheduler_ip', get_default_addr())
            port = await config.get_option('worker.scheduler_port', 7979)
            logger.debug(f'using {ip}:{port}')
            try:
                worker = await create_worker(ip, port, worker_type=worker_type, singleshot=singleshot, worker_id=worker_id, pool_address=pool_address)
            except ConnectionRefusedError as e:
                logger.exception('Connection error', str(e))
                await asyncio.sleep(10)
                continue
            await worker.wait_till_stops()
            logger.info('worker quited')
            if noloop:
                break


default_config = '''
[worker]
listen_to_broadcast = true

[default_env_wrapper]
## here you can uncomment lines below to specify your own default environment wrapper and default arguments
## this will only be used by invocation jobs that have NO environment wrappers specified
# name = TrivialEnvironmentResolver
# arguments = [ "project_name", "or", "config_name", "idunno", "maybe rez packages requirements?", [1,4,11] ]
'''


def main(argv):
    # import signal
    # prev = None
    # def signal_handler(sig, frame):
    #     print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! You pressed Ctrl+C !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    #     prev(sig, frame)
    #
    # prev = signal.signal(signal.SIGINT, signal_handler)
    import argparse
    parser = argparse.ArgumentParser('lifeblood.worker', description='executes invocations from scheduler')
    parser.add_argument('--scheduler-address', help='manually specify scheduler to connect to. if not specified - by default worker will start listening to broadcasts from schedulers')
    parser.add_argument('--no-listen-broadcast', action='store_true', help='do not listen to scheduler\'s broadcast, use config')
    parser.add_argument('--no-loop', action='store_true', help='by default worker will return into the loop of waiting for scheduler every time it quits because of connection loss, or other errors. '
                                                               'but this flag will force worker to just completely quit instead')
    parser.add_argument('--singleshot', action='store_true', help='worker will pick one job and exit after that job is completed or cancelled. '
                                                                  'this is on by default when type=SCHEDULER_HELPER')
    parser.add_argument('--type', choices=('STANDARD', 'SCHEDULER_HELPER'), default='STANDARD')
    parser.add_argument('--id', help='integer identifier which worker should use when talking to worker pool')
    parser.add_argument('--pool-address', help='if this worker is a part of a pool - pool address. currently pool can only be on the same host')
    args = parser.parse_args(argv)
    if args.type == 'STANDARD':
        wtype = WorkerType.STANDARD
    elif args.type == 'SCHEDULER_HELPER':
        wtype = WorkerType.SCHEDULER_HELPER
    else:
        raise NotImplementedError(f'worker type {args.type} is not yet implemented')
    global_logger = logging.get_logger('worker')

    # check and create default config if none
    create_default_user_config_file('worker', default_config)

    # check legality of the address
    paddr = None
    if args.pool_address is not None:
        paddr = address_to_ip_port(args.pool_address)
    config = get_config('worker')
    if args.no_listen_broadcast:
        config.set_override('worker.listen_to_broadcast', False)
    if args.scheduler_address is not None:
        config.set_override('worker.listen_to_broadcast', False)
        saddr = address_to_ip_port(args.scheduler_address)
        config.set_override('worker.scheduler_ip', saddr[0])
        config.set_override('worker.scheduler_port', saddr[1])
    try:
        asyncio.run(main_async(wtype, singleshot=args.singleshot, worker_id=int(args.id) if args.id is not None else None, pool_address=paddr, noloop=args.no_loop))
    except KeyboardInterrupt:
        # if u see errors in pycharm around this area when running from scheduler -
        # it's because pycharm sends it's own SIGINT to this child process on top of SIGINT that pool sends
        global_logger.warning('SIGINT caught')
        global_logger.info('SIGINT caught. Worker is stopped now.')


if __name__ == '__main__':
    import sys
    main(sys.argv)
