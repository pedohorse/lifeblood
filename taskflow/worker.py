import os
import errno
import traceback
import asyncio
import aiofiles
import json
from .nethelpers import get_addr_to
from .worker_task_protocol import WorkerTaskServerProtocol, AlreadyRunning
from .scheduler_task_protocol import SchedulerTaskClient
from .broadcasting import await_broadcast
from .invocationjob import InvocationJob, Environment

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

    return worker


class Worker:
    log_root_path = '/tmp/workerlogs'  # TODO: !!! this is a placeholder

    def __init__(self, scheduler_addr: str, scheduler_ip: int):
        if not os.path.exists(self.log_root_path):
            os.makedirs(self.log_root_path)
        self.__status = {}
        self.__running_process = None  # type: Optional[asyncio.subprocess.Process]
        self.__running_task = None  # type: Optional[InvocationJob]
        self.__running_awaiter = None
        self.__server: asyncio.AbstractServer = None
        self.__where_to_report = None
        self.__ping_interval = 5
        self.__ping_missed_threshold = 2
        self.__ping_missed = 0
        self.__scheduler_addr = (scheduler_addr, scheduler_ip)
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
        print(f'running task {task}')
        logbasedir = os.path.dirname(self.get_log_filepath('output', task.invocation_id()))
        env = Environment(os.environ)
        env.prepend('PYTHONPATH', os.path.join(os.path.dirname(__file__), 'worker_runtime_pythonpath'))
        env['TASKFLOW_RUNTIME_IID'] = task.invocation_id()
        env['TASKFLOW_RUNTIME_SCHEDULER_ADDR'] = report_to
        env = task.env().resolve(base_env=env)
        if not os.path.exists(logbasedir):
            os.makedirs(logbasedir)
        try:
            with open(self.get_log_filepath('output', task.invocation_id()), 'a') as stdout:
                with open(self.get_log_filepath('error', task.invocation_id()), 'a') as stderr:
                    self.__running_process: asyncio.subprocess.Process = \
                        await asyncio.create_subprocess_exec(
                            *task.args(),
                            stdout=stdout,
                            stderr=stderr,
                            env=env
                        )
        except Exception as e:
            await self.log_error('task failed with error: %s\n%s' % (repr(e), traceback.format_exc()))
            raise

        # callback awaiter
        async def _awaiter():
            await self.__running_process.wait()
            await self.task_finished()

        self.__running_awaiter = asyncio.create_task(_awaiter())
        self.__running_task = task
        self.__where_to_report = report_to

    def is_task_running(self) -> bool:
        return self.__running_task is not None

    async def stop_task(self):
        if self.__running_process is None:
            return
        self.__running_awaiter.cancel()
        self.__running_awaiter = None
        self.__running_process.terminate()
        for i in range(20):  # TODO: make a parameter out of this!
            if self.__running_process.returncode is None:
                await asyncio.sleep(0.5)
            else:
                break
        else:
            self.__running_process.kill()
        await self.__running_process.wait()
        self.__running_task = None
        self.__running_process = None
        self.__where_to_report = None

    async def task_status(self):
        raise NotImplementedError()

    async def task_finished(self):
        """
        is called when current process finishes
        :return:
        """
        print('task finished')
        print(f'reporting back to {self.__where_to_report}')
        try:
            ip, port = self.__where_to_report.split(':', 1)
            async with SchedulerTaskClient(ip, int(port)) as client:
                await client.report_task_done(self.__running_task,
                                              await self.__running_process.wait(),
                                              self.get_log_filepath('output', self.__running_task.invocation_id()),
                                              self.get_log_filepath('error', self.__running_task.invocation_id()))
        except Exception as e:
            print(f'could not report cuz of {e}')
        except:
            print('could not report cuz i have no idea')
        self.__where_to_report = None
        self.__running_task = None
        self.__running_process = None
        self.__running_awaiter = None

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
                result = b''
            if result == b'':  # this means EOF
                self.__ping_missed += 1
                print(f'server ping missed. total misses: {self.__ping_missed}')
            if self.__ping_missed >= self.__ping_missed_threshold:
                # assume scheruler down, drop everything and look for another scheruler
                await self.stop_task()
                self.__server.close()
                return

    async def wait_to_finish(self):
        await self.__scheduler_pinger
        await self.__server.wait_closed()
        # await asyncio.gather(self.__server.wait_closed(),
        #                      self.__scheduler_pinger)


async def main():
    """
    listen to scheduler broadcast in a loop.
    if received - create the worker and work
    if worker cannot ping the scheduler a number of times - it stops
    and listenting for broadcast starts again
    :return: Never!
    """
    while True:
        print('listening for scheduler broadcasts...')
        message = await await_broadcast('taskflow_scheduler')
        scheduler_info = json.loads(message)
        print('received', scheduler_info)
        addr = scheduler_info['worker']
        ip, sport = addr.split(':')  # TODO: make a proper protocol handler or what? at least ip/ipv6
        port = int(sport)
        worker = await create_worker(ip, port)
        await worker.wait_to_finish()
        print('worker quited')


if __name__ == '__main__':
    asyncio.run(main())
