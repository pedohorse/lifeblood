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

from typing import Tuple, Dict, List, Optional


async def create_worker_pool(worker_type: WorkerType = WorkerType.STANDARD, *, minimal_total_to_ensure=0, minimal_idle_to_ensure=0, scheduler_address: Optional[Tuple[str, int]] = None):
    swp = WorkerPool(worker_type, minimal_total_to_ensure=minimal_total_to_ensure, minimal_idle_to_ensure=minimal_idle_to_ensure, scheduler_address=scheduler_address)
    await swp.start()
    return swp


class ProcData:
    __slots__ = {'process', 'id', 'state'}

    def __init__(self, process: asyncio.subprocess.Process, id: int):
        self.process = process
        self.id = id
        self.state: WorkerState = WorkerState.OFF


class WorkerPool:
    def __init__(self, worker_type: WorkerType = WorkerType.STANDARD, *, minimal_total_to_ensure=0, minimal_idle_to_ensure=0, scheduler_address: Optional[Tuple[str, int]] = None):
        """
        manages a pool of workers.
        :param worker_type: workers are created of given type
        :param minimal_total_to_ensure:  at minimum this amount of workers will be always upheld
        :param minimal_idle_to_ensure:  at minimum this amount of IDLE or OFF(as we assume they are OFF only while they are booting up) workers will be always upheld
        :param scheduler_address:  force created workers to use this scheduler address. otherwise workers will use their configuration
        """
        # local helper workers' pool
        self.__worker_pool: Dict[asyncio.Future, ProcData] = {}
        self.__workers_to_merge: List[ProcData] = []
        self.__pool_task = None
        self.__worker_server: Optional[asyncio.AbstractServer] = None
        self.__stop_event = asyncio.Event()
        self.__poke_event = asyncio.Event()
        self.__logger = get_logger(self.__class__.__name__.lower())
        self.__ensure_minimum_total = minimal_total_to_ensure
        self.__ensure_minimum_idle = minimal_idle_to_ensure
        self.__maximum_total = 256
        self.__worker_type = worker_type
        self.__scheduler_address = scheduler_address

        self.__id_to_procdata: Dict[int, ProcData] = {}
        self.__next_wid = 0
        self.__my_addr = get_localhost()
        self.__my_port = 7957

        self.__poke_event.set()
        self.__stopped = False

    async def start(self):
        if self.__pool_task is not None and not self.__pool_task.done():
            return

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

        self.__pool_task = asyncio.create_task(self.local_worker_pool_manager())
        self.__logger.debug(f'worker pool protocol listening on {self.__my_port}')

    def stop(self):
        self.__worker_server.close()
        self.__stop_event.set()
        self.__stopped = True

    def __await__(self):
        return self.wait_till_stops().__await__()

    async def wait_till_stops(self):
        if self.__pool_task is None:
            return
        await self.__pool_task
        await self.__worker_server.wait_closed()

    async def add_worker(self):
        if self.__stopped:
            self.__logger.warning('add_worker called after stop()')
            return
        if len(self.__id_to_procdata) + len(self.__workers_to_merge) >= self.__maximum_total:
            self.__logger.warning(f'maximum worker limit reached ({self.__maximum_total})')
            return
        args = [sys.executable, '-m', 'lifeblood.launch',
                'worker',
                '--type', self.__worker_type.name,
                '--no-loop',
                '--id', str(self.__next_wid),
                '--pool-address', f'{self.__my_addr}:{self.__my_port}']
        if self.__scheduler_address is not None:
            args += ['--scheduler-address', ':'.join(str(x) for x in self.__scheduler_address)]

        self.__workers_to_merge.append(ProcData(await asyncio.create_subprocess_exec(*args,
                                                                                     close_fds=True
                                                                                     ), self.__next_wid))
        self.__logger.debug(f'adding new worker (id: {self.__next_wid}) to the pool, total: {len(self.__workers_to_merge) + len(self.__worker_pool)}')
        self.__next_wid += 1
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
                        if not poke_waiter.done():
                            poke_waiter.cancel()
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
                for procdata in self.__workers_to_merge:
                    self.__worker_pool[asyncio.create_task(procdata.process.wait())] = procdata
                    self.__id_to_procdata[procdata.id] = procdata

                self.__workers_to_merge.clear()

                # ensure the ensure
                just_added = 0
                if len(self.__worker_pool) < self.__ensure_minimum_total:
                    for _ in range(self.__ensure_minimum_total - len(self.__worker_pool)):
                        await self.add_worker()
                        just_added += 1
                idle_guys = len([k for k, v in self.__id_to_procdata.items() if v.state in (WorkerState.IDLE, WorkerState.OFF)])  # consider OFF ones as IDLEs that just boot up
                if idle_guys + just_added < self.__ensure_minimum_idle:
                    for _ in range(self.__ensure_minimum_idle - idle_guys - just_added):
                        await self.add_worker()

            # debug logging
            self.__logger.debug(f'at pool closing, before cleanup: total workers: {len(self.__worker_pool)}, idle: {len([k for k, v in self.__id_to_procdata.items() if v.state in (WorkerState.IDLE, WorkerState.OFF)])}')
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
                    self.__logger.debug(f'{proc.pid} has gracefully ended with {await proc.wait()}')
                except asyncio.TimeoutError:
                    self.__logger.warning('worker ignored SIGINT. killing instead.')
                    proc.kill()
                    await proc.wait()
                except Exception as e:
                    self.__logger.exception('very unexpected exception. pretending like it hasn\'t happened')

            # cleanup
            wait_tasks = []
            for procdata in itertools.chain(self.__worker_pool.values(), self.__workers_to_merge):
                try:
                    self.__logger.debug(f'sending SIGINT to {procdata.process.pid}')
                    procdata.process.send_signal(signal.SIGINT)
                except ProcessLookupError:
                    continue
                wait_tasks.append(_proc_waiter(procdata.process))
            await asyncio.gather(*wait_tasks)
            await asyncio.gather(*self.__worker_pool.keys())  # since all processes are killed now - this SHOULD take no time at all

            self.__logger.info('worker pool stopped')
        # tidyup
        for fut in self.__worker_pool:
            if not fut.done():
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
