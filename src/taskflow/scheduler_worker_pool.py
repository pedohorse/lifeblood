import sys
import asyncio
import signal
import itertools

from .logging import get_logger

from typing import Set, Dict, List


class SchedulerWorkerPool:
    def __init__(self):
        # local helper workers' pool
        self.__worker_pool: Dict[asyncio.Future, asyncio.subprocess.Process] = {}
        self.__workers_to_merge: List[asyncio.subprocess.Process] = []
        self.__pool_task = None
        self.__stop_event = asyncio.Event()
        self.__logger = get_logger(self.__class__.__name__.lower())

    def start(self):
        if self.__pool_task is not None:
            return
        self.__pool_task = asyncio.create_task(self.local_worker_pool_manager())

    def stop(self):
        if self.__pool_task is None:
            return
        self.__stop_event.set()

    async def add_worker(self):
        self.__workers_to_merge.append(await asyncio.create_subprocess_exec(sys.executable, '-m', 'taskflow.launch', 'worker', '--type', 'SCHEDULER_HELPER'))
        self.__logger.debug(f'adding new worker to the pool, total: {len(self.__workers_to_merge) + len(self.__worker_pool)}')

    #
    # local worker pool manager
    async def local_worker_pool_manager(self):
        """
        this task is responsible for local worker management.
        kill them if aborted
        :return:
        """
        check_timeout = 10
        stop_waiter = asyncio.create_task(self.__stop_event.wait())
        try:
            while True:
                done, pending = await asyncio.wait(itertools.chain(self.__worker_pool.keys(), (stop_waiter,)), timeout=check_timeout, return_when=asyncio.FIRST_COMPLETED)
                time_to_stop = False
                for x in done:
                    if x == stop_waiter:
                        time_to_stop = True
                        self.__logger.info('stopping worker pool...')
                        break
                    del self.__worker_pool[x]
                    self.__logger.debug(f'removing finished worker from the pool, total: {len(self.__workers_to_merge) + len(self.__worker_pool)}')
                if time_to_stop:
                    break
                for proc in self.__workers_to_merge:
                    self.__worker_pool[asyncio.create_task(proc.wait())] = proc
                self.__workers_to_merge.clear()
        except asyncio.CancelledError:
            self.__logger.info('cancelled! stopping worker pool...')
            raise
        finally:
            async def _proc_waiter(proc: asyncio.subprocess.Process):
                try:
                    await asyncio.wait_for(proc.wait(), timeout=10)
                except asyncio.TimeoutError:
                    self.__logger.warning('worker ignored SIGINT. killing instead.')
                    proc.kill()
                    await proc.wait()
                except Exception as e:
                    self.__logger.exception('very unexpected exception. pretending like it hasn\'t happened')

            # cleanup
            wait_tasks = []
            for proc in itertools.chain(self.__worker_pool.values(), self.__workers_to_merge):
                try:
                    proc.send_signal(signal.SIGINT)
                except ProcessLookupError:
                    continue
                wait_tasks.append(_proc_waiter(proc))
            await asyncio.gather(*wait_tasks)

            self.__logger.info('worker pool stopped')
        # tidyup
        for fut in self.__worker_pool:
            fut.cancel()
