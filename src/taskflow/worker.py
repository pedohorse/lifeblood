import os
import errno
import traceback
import asyncio
import subprocess
import aiofiles
import json
import psutil
import datetime
import tempfile
from . import logging
from .nethelpers import get_addr_to, get_default_addr
from .worker_task_protocol import WorkerTaskServerProtocol, AlreadyRunning
from .scheduler_task_protocol import SchedulerTaskClient
from .broadcasting import await_broadcast
from .invocationjob import InvocationJob, Environment
from .config import get_config
from .environment_wrapper import TrivialEnvironmentWrapper


from .worker_runtime_pythonpath import taskflow_connection
import inspect

from typing import Optional


async def create_worker(scheduler_ip: str, scheduler_port: int, loop=None):
    worker = Worker(scheduler_ip, scheduler_port)
    if loop is None:
        loop = asyncio.get_event_loop()
    my_ip = get_addr_to(scheduler_ip)
    for i in range(1024):  # big but finite
        try:
            server = await loop.create_server(worker.proto_server_factory, my_ip, 6969 + i, backlog=16)
            addr = f'{my_ip}:{6969 + i}'
            break
        except OSError as e:
            if e.errno != errno.EADDRINUSE:
                raise
            continue
    else:
        raise RuntimeError('could not find an opened port!')
    worker._set_working_server(server)

    # now report our address to the scheduler
    async with SchedulerTaskClient(scheduler_ip, scheduler_port) as client:
        await client.say_hello(addr)
    #
    worker._start()
    return worker


class Worker:
    def __init__(self, scheduler_addr: str, scheduler_ip: int):
        config = get_config('worker')
        self.__logger = logging.getLogger('worker')
        self.log_root_path = os.path.expandvars(config.get_option_noasync('worker.logpath', os.path.join(tempfile.gettempdir(), 'taskflow', 'worker_logs')))

        if not os.path.exists(self.log_root_path):
            os.makedirs(self.log_root_path)
        self.__status = {}
        self.__running_process: Optional[asyncio.subprocess.Process] = None
        self.__running_task: Optional[InvocationJob] = None
        self.__running_task_progress: Optional[float] = None
        self.__running_awaiter = None
        self.__server: asyncio.AbstractServer = None
        self.__where_to_report = None
        self.__ping_interval = 5
        self.__ping_missed_threshold = 2
        self.__ping_missed = 0
        self.__scheduler_addr = (scheduler_addr, scheduler_ip)
        self.__scheduler_pinger = None

        self.__env_writer = TrivialEnvironmentWrapper()

        # deploy a copy of runtime module somewhere in temp
        rtmodule_code = inspect.getsource(taskflow_connection)

        filepath = os.path.join(tempfile.gettempdir(), 'taskflow', 'taskflow_runtime', 'taskflow_connection.py')
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        existing_code = None
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                existing_code = f.read()

        if existing_code != rtmodule_code:
            with open(filepath, 'w') as f:
                f.write(rtmodule_code)
        self.__rt_module_dir = os.path.dirname(filepath)

    def _start(self):
        self.__scheduler_pinger = asyncio.create_task(self.scheduler_pinger())

    def _set_working_server(self, server: asyncio.AbstractServer):  # TODO: i dont like this unclear way of creating worker. either hide constructor somehow, or use it
        self.__server = server

    def proto_server_factory(self):
        return WorkerTaskServerProtocol(self)

    async def log_error(self, *args):
        await self.log('error', *args)

    async def log(self, level, *args):
        logpath = self.get_log_filepath(level)

        if not os.path.exists(logpath):
            os.makedirs(os.path.dirname(logpath), exist_ok=True)
        async with aiofiles.open(logpath, 'a') as f:
            await f.write(', '.join(args))

    def get_log_filepath(self, level, invocation_id: int = None):  # TODO: think of a better, more generator-style way of returning logs
        if self.__running_task is None and invocation_id is None:
            return os.path.join(self.log_root_path, 'common', level)
        else:
            return os.path.join(self.log_root_path, 'invocations', str(invocation_id or self.__running_task.invocation_id()), level)

    async def run_task(self, task: InvocationJob, report_to: str):
        assert len(task.args()) > 0
        if self.__running_process is not None:
            raise AlreadyRunning('Task already in progress')
        # prepare logging
        self.__logger.info(f'running task {task}')
        logbasedir = os.path.dirname(self.get_log_filepath('output', task.invocation_id()))
        env = self.__env_writer.get_environment(None, task.env())

        env.prepend('PYTHONPATH', self.__rt_module_dir)
        env['TASKFLOW_RUNTIME_IID'] = task.invocation_id()
        env['TASKFLOW_RUNTIME_SCHEDULER_ADDR'] = report_to
        for aname, aval in task.attributes().items():
            env['TFATTR_%s' % aname] = str(aval)
        env['TFATTRS_JSON'] = json.dumps(dict(task.attributes())).encode('UTF-8')
        if not os.path.exists(logbasedir):
            os.makedirs(logbasedir)
        try:
            #with open(self.get_log_filepath('output', task.invocation_id()), 'a') as stdout:
            #    with open(self.get_log_filepath('error', task.invocation_id()), 'a') as stderr:
            self.__running_process: asyncio.subprocess.Process = \
                await asyncio.create_subprocess_exec(  # TODO: process subprocess exceptions
                    *task.args(),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=env
                )
        except Exception as e:
            await self.log_error('task failed with error: %s\n%s' % (repr(e), traceback.format_exc()))
            raise

        self.__running_task = task
        self.__where_to_report = report_to
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

        await self.__running_process.wait()
        await self.task_finished()

    def is_task_running(self) -> bool:
        return self.__running_task is not None

    async def cancel_task(self):
        if self.__running_process is None:
            return
        self.__logger.info('cancelling running task')
        self.__running_awaiter.cancel()
        self.__running_awaiter = None
        puproc = psutil.Process(self.__running_process.pid)
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

    async def task_status(self) -> Optional[float]:
        return self.__running_task_progress

    async def task_finished(self):
        """
        is called when current process finishes
        :return:
        """
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

    #
    # simply ping scheduler once in a while
    async def scheduler_pinger(self):
        """
        ping scheduler once in a while. if it misses too many pings - close worker and wait for new broadcasts
        :return:
        """
        while True:
            await asyncio.sleep(self.__ping_interval)
            if self.__ping_missed_threshold == 0:
                continue
            try:
                async with SchedulerTaskClient(*self.__scheduler_addr) as client:
                    result = await client.ping()
            except ConnectionRefusedError as e:
                self.__logger.error('scheduler ping connection was refused')
                result = b''
            except ConnectionResetError as e:
                self.__logger.error('scheduler ping connection was reset')
                result = b''
            if result == b'':  # this means EOF
                self.__ping_missed += 1
                self.__logger.info(f'server ping missed. total misses: {self.__ping_missed}')
            if self.__ping_missed >= self.__ping_missed_threshold:
                # assume scheruler down, drop everything and look for another scheruler
                await self.cancel_task()
                self.__server.close()
                return

    async def wait_to_finish(self):
        if self.__scheduler_pinger is not None:
            await self.__scheduler_pinger
            self.__scheduler_pinger = None
        await self.__server.wait_closed()


async def main_async():
    """
    listen to scheduler broadcast in a loop.
    if received - create the worker and work
    if worker cannot ping the scheduler a number of times - it stops
    and listenting for broadcast starts again
    :return: Never!
    """
    config = get_config('worker')
    logger = logging.getLogger('worker')
    if await config.get_option('worker.listen_to_broadcast', True):
        while True:
            logger.info('listening for scheduler broadcasts...')
            message = await await_broadcast('taskflow_scheduler')
            scheduler_info = json.loads(message)
            logger.debug('received', scheduler_info)
            addr = scheduler_info['worker']
            ip, sport = addr.split(':')  # TODO: make a proper protocol handler or what? at least ip/ipv6
            port = int(sport)
            worker = await create_worker(ip, port)
            await worker.wait_to_finish()
            logger.info('worker quited')
    else:
        while True:
            ip = await config.get_option('worker.scheduler_ip', get_default_addr())
            port = await config.get_option('worker.scheduler_port', 7979)
            try:
                worker = await create_worker(ip, port)
            except ConnectionRefusedError as e:
                logger.exception('Connection error', str(e))
                await asyncio.sleep(10)
                continue
            await worker.wait_to_finish()
            logger.info('worker quited')


def main():
    asyncio.run(main_async())


if __name__ == '__main__':
    main()
