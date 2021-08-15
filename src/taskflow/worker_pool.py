import sys
import errno
import asyncio
import signal
import itertools
from types import MappingProxyType

from .logging import get_logger
from .worker_pool_protocol import WorkerPoolProtocol
from .nethelpers import get_localhost
from .enums import WorkerState, WorkerType

from typing import Set, Dict, List, Optional


async def create_worker_pool(worker_type: WorkerType = WorkerType.STANDARD, *, minimal_total_to_ensure=0, minimal_idle_to_ensure=0):
    swp = WorkerPool(worker_type, minimal_total_to_ensure=minimal_total_to_ensure, minimal_idle_to_ensure=minimal_idle_to_ensure)
    await swp.start()
    return swp


class ProcData:
    __slots__ = {'process', 'id', 'state'}

    def __init__(self, process: asyncio.subprocess.Process, id: int):
        self.process = process
        self.id = id
        self.state: WorkerState = WorkerState.IDLE


class WorkerPool:
    def __init__(self, worker_type: WorkerType = WorkerType.STANDARD, *, minimal_total_to_ensure=0, minimal_idle_to_ensure=0):
        # local helper workers' pool
        self.__worker_pool: Dict[asyncio.Future, ProcData] = {}
        self.__workers_to_merge: List[asyncio.subprocess.Process] = []
        self.__pool_task = None
        self.__worker_server: Optional[asyncio.AbstractServer] = None
        self.__stop_event = asyncio.Event()
        self.__poke_event = asyncio.Event()
        self.__logger = get_logger(self.__class__.__name__.lower())
        self.__ensure_minimum_total = minimal_total_to_ensure
        self.__ensure_minimum_idle = minimal_idle_to_ensure
        self.__maximum_total = 256
        self.__worker_type = worker_type

        self.__id_to_procdata: Dict[int, ProcData] = {}
        self.__next_wid = 0
        self.__my_addr = get_localhost()
        self.__my_port = 7957

        self.__poke_event.set()

    async def start(self):
        if self.__pool_task is not None and not self.__pool_task.done():
            return
        self.__pool_task = asyncio.create_task(self.local_worker_pool_manager())

        for i in range(1024):  # big but finite
            try:
                self.__worker_server = await asyncio.get_event_loop().create_server(lambda: WorkerPoolProtocol(self),
                                                                                    self.__my_addr,
                                                                                    self.__my_port)
                break
            except OSError as e:
                if e.errno != errno.EADDRINUSE:
                    raise
                self.__my_port += 1
                continue

        else:
            raise RuntimeError('could not find an opened port!')
        self.__logger.debug(f'worker pool protocol listening on {self.__my_port}')

    def stop(self):
        self.__worker_server.close()
        self.__stop_event.set()

    def __await__(self):
        return self.wait_till_stops().__await__()

    async def wait_till_stops(self):
        if self.__pool_task is None:
            return
        await self.__pool_task
        await self.__worker_server.wait_closed()

    async def add_worker(self):
        if len(self.__id_to_procdata) + len(self.__workers_to_merge) >= self.__maximum_total:
            self.__logger.warning(f'maximum worker limit reached ({self.__maximum_total})')
            return
        self.__workers_to_merge.append(await asyncio.create_subprocess_exec(sys.executable, '-m', 'taskflow.launch', 'worker', '--type', self.__worker_type.name, '--pool-address', f'{self.__my_addr}:{self.__my_port}'))
        self.__logger.debug(f'adding new worker to the pool, total: {len(self.__workers_to_merge) + len(self.__worker_pool)}')
        self.__poke_event.set()

    def list_workers(self):
        return MappingProxyType(self.__id_to_procdata)

    def set_minimum_total_workers(self, minimum_total: int):
        self.__ensure_minimum_total = minimum_total

    def set_minimum_idle_workers(self, minimum_idle: int):
        self.__ensure_minimum_idle = minimum_idle

    def set_maximum_workers(self, maximum: int):
        self.__maximum_total = maximum

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
        poke_waiter = asyncio.create_task(self.__poke_event.wait())
        try:
            while True:
                done, pending = await asyncio.wait(itertools.chain(self.__worker_pool.keys(), (stop_waiter, poke_waiter)), timeout=check_timeout, return_when=asyncio.FIRST_COMPLETED)
                time_to_stop = False
                for x in done:
                    if x == stop_waiter:
                        time_to_stop = True
                        self.__logger.info('stopping worker pool...')
                        break
                    elif x == poke_waiter:
                        self.__poke_event.clear()
                        poke_waiter = asyncio.create_task(self.__poke_event.wait())
                        continue
                    wid = self.__worker_pool[x].id
                    del self.__worker_pool[x]
                    del self.__id_to_procdata[wid]

                    self.__logger.debug(f'removing finished worker from the pool, total: {len(self.__workers_to_merge) + len(self.__worker_pool)}')
                if time_to_stop:
                    break
                for proc in self.__workers_to_merge:
                    procdata = ProcData(proc, self.__next_wid)
                    self.__worker_pool[asyncio.create_task(proc.wait())] = procdata
                    self.__id_to_procdata[self.__next_wid] = procdata
                    self.__next_wid += 1
                self.__workers_to_merge.clear()

                # ensure the ensure
                just_added = 0
                if len(self.__worker_pool) < self.__ensure_minimum_total:
                    for _ in range(self.__ensure_minimum_total - len(self.__worker_pool)):
                        await self.add_worker()
                        just_added += 1
                idle_guys = len([k for k, v in self.__id_to_procdata.items() if v.state == WorkerState.IDLE])
                if idle_guys + just_added < self.__ensure_minimum_idle:
                    for _ in range(self.__ensure_minimum_idle - idle_guys - just_added):
                        await self.add_worker()

            # debug logging
            self.__logger.debug(f'total workers: {len(self.__worker_pool)}, idle: {len([k for k, v in self.__id_to_procdata.items() if v.state == WorkerState.IDLE])}')
            # more verbose debug:
            if True:
                for wid, procdata in self.__id_to_procdata.items():
                    self.__logger.debug(f'worker id {wid}, pid {procdata.process.pid}: {procdata.state}')
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
            for proc in itertools.chain((x.process for x in self.__worker_pool.values()), self.__workers_to_merge):
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

    #
    # callbacks
    async def _worker_state_change(self, worker_id: int, state: WorkerState):
        if worker_id not in self.__id_to_procdata:
            self.__logger.warning(f'reported state {state} for worker {worker_id} that DOESN\'T BELONG TO US')
            return

        if self.__id_to_procdata[worker_id].state != state:
            self.__id_to_procdata[worker_id].state = state
            self.__poke_event.set()
