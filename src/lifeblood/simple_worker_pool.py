import sys
import errno
import argparse
import asyncio
import signal
import time
import itertools
from types import MappingProxyType
import json
from .broadcasting import await_broadcast
from .pulse_checker import PulseChecker
from .process_utils import create_worker_process, send_stop_signal_to_worker

from .logging import get_logger
from .worker_pool_protocol import WorkerPoolProtocol
from .nethelpers import get_localhost
from .enums import WorkerState, WorkerType, ProcessPriorityAdjustment
from .defaults import worker_pool_port as default_worker_pool_port

from typing import Tuple, Dict, List, Optional


async def create_worker_pool(worker_type: WorkerType = WorkerType.STANDARD, *,
                             minimal_total_to_ensure=0, minimal_idle_to_ensure=0, maximum_total=256,
                             idle_timeout=10, worker_suspicious_lifetime=4, priority=ProcessPriorityAdjustment.NO_CHANGE, scheduler_address: Optional[Tuple[str, int]] = None):
    swp = WorkerPool(worker_type,
                     minimal_total_to_ensure=minimal_total_to_ensure, minimal_idle_to_ensure=minimal_idle_to_ensure, maximum_total=maximum_total,
                     idle_timeout=idle_timeout, worker_suspicious_lifetime=worker_suspicious_lifetime, priority=priority, scheduler_address=scheduler_address)
    await swp.start()
    return swp


class ProcData:
    __slots__ = {'process', 'id', 'state', 'state_entering_time', 'start_time', 'sent_term_signal'}

    def __init__(self, process: asyncio.subprocess.Process, id: int):
        self.process = process
        self.id = id
        self.state: WorkerState = WorkerState.OFF
        self.state_entering_time = 0
        self.start_time = time.time()
        self.sent_term_signal = False


class WorkerPool:  # TODO: split base class, make this just one of implementations
    def __init__(self, worker_type: WorkerType = WorkerType.STANDARD, *,
                 minimal_total_to_ensure=0, minimal_idle_to_ensure=0, maximum_total=256,
                 idle_timeout=10, worker_suspicious_lifetime=4, priority=ProcessPriorityAdjustment.NO_CHANGE,
                 scheduler_address: Optional[Tuple[str, int]] = None):
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
        self.__server_closer_waiter = None
        self.__poke_event = asyncio.Event()
        self.__logger = get_logger(self.__class__.__name__.lower())
        self.__ensure_minimum_total = minimal_total_to_ensure
        self.__ensure_minimum_idle = minimal_idle_to_ensure
        self.__maximum_total = maximum_total
        self.__worker_type = worker_type
        self.__idle_timeout = idle_timeout  # after this amount of idling worker will be stopped if total count is above minimum
        self.__worker_priority = priority
        self.__scheduler_address = scheduler_address

        # workers are not created as singleshot, so lifetime of less then this should be considered a sign of possible error
        self.__suspiciously_short_process_time = worker_suspicious_lifetime

        self.__pulse_checker = PulseChecker(self.__scheduler_address, interval=10, maximum_misses=10)
        self.__pulse_checker.add_pulse_fail_callback(self._on_pulse_fail)

        self.__id_to_procdata: Dict[int, ProcData] = {}
        self.__next_wid = 0
        self.__my_addr = get_localhost()
        self.__my_port = default_worker_pool_port()

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
        await self.__pulse_checker.start()
        self.__logger.debug(f'worker pool protocol listening on {self.__my_port}')

    def stop(self):
        async def _server_closer():
            await self.__pool_task  # ensure local manager is stopped before closing server. here it will ensure all workers are terminated
            self.__worker_server.close()
            await self.__worker_server.wait_closed()

        if self.__stopped:
            return
        self.__stop_event.set()  # stops local_worker_pool_manager
        self.__server_closer_waiter = asyncio.create_task(_server_closer())  # server will be closed here
        self.__pulse_checker.stop()
        self.__stopped = True

    def __await__(self):
        return self.wait_till_stops().__await__()

    async def wait_till_stops(self):
        if self.__pool_task is None:
            return
        await self.__pool_task
        await self.__worker_server.wait_closed()
        await self.__pulse_checker
        await self.__server_closer_waiter

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
                '--priority', self.__worker_priority.name,
                '--no-loop',
                '--id', str(self.__next_wid),
                '--pool-address', f'{self.__my_addr}:{self.__my_port}']
        if self.__scheduler_address is not None:
            args += ['--scheduler-address', ':'.join(str(x) for x in self.__scheduler_address)]

        self.__workers_to_merge.append(ProcData(await create_worker_process(args), self.__next_wid))
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
        async def _wait_and_reset_event(event: asyncio.Event, timeout):
            await asyncio.sleep(timeout)
            event.clear()

        check_timeout = 10
        stop_waiter = asyncio.create_task(self.__stop_event.wait())
        poke_waiter = asyncio.create_task(self.__poke_event.wait())
        no_adding_workers = asyncio.Event()
        wait_event_task = None
        try:
            while True:
                done, pending = await asyncio.wait(itertools.chain(self.__worker_pool.keys(), (stop_waiter, poke_waiter)), timeout=check_timeout, return_when=asyncio.FIRST_COMPLETED)
                time_to_stop = False
                if wait_event_task is not None and wait_event_task.done():
                    wait_event_task = None

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
                    # if not those 2 - x must be a process awaiting task
                    if (span := time.time() - self.__worker_pool[x].start_time) < self.__suspiciously_short_process_time:
                        self.__logger.warning(f'worker died within suspicious time threshold: {span}s. pausing worker creation for a bit')
                        if wait_event_task is None:
                            no_adding_workers.set()
                            wait_event_task = asyncio.create_task(_wait_and_reset_event(no_adding_workers, 5))
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

                # check for idle workers
                idle_guys = len([k for k, v in self.__id_to_procdata.items() if v.state in (WorkerState.IDLE, WorkerState.OFF)])  # consider OFF ones as IDLEs that just boot up
                if idle_guys > self.__ensure_minimum_idle:
                    max_to_kill = idle_guys - self.__ensure_minimum_idle
                    # if we above minimum - we can kill some idle ones
                    now = time.time()
                    for procdata in self.__worker_pool.values():
                        if max_to_kill <= 0:
                            break
                        if procdata.state != WorkerState.IDLE or now - procdata.state_entering_time < self.__idle_timeout or procdata.sent_term_signal:
                            continue
                        try:
                            send_stop_signal_to_worker(procdata.process)
                            procdata.sent_term_signal = True
                        except ProcessLookupError:
                            # probability is low, but this can happen. though if this happens often - something is wrong
                            self.__logger.warning("tried kill some idle workers, but it was already dead")
                        else:
                            max_to_kill -= 1
                            self.__poke_event.set()  # poke ourselves to clean up finished processes

                # ensure the ensure
                if not no_adding_workers.is_set():
                    just_added = 0
                    if len(self.__worker_pool) < self.__ensure_minimum_total:
                        for _ in range(self.__ensure_minimum_total - len(self.__worker_pool)):
                            await self.add_worker()
                            just_added += 1  # cuz add_worker will not add to __id_to_procdata or __worker_pool - we do on next iteration

                    if idle_guys + just_added < self.__ensure_minimum_idle:
                        for _ in range(self.__ensure_minimum_idle - idle_guys - just_added):
                            await self.add_worker()
                else:
                    self.__logger.debug('temporarily not adding workers')

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
                    self.__logger.debug(f'sending SIGTERM to {procdata.process.pid}')
                    send_stop_signal_to_worker(procdata.process)
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
    async def _on_pulse_fail(self):
        self.stop()

    #
    # callbacks from protocol
    async def _worker_state_change(self, worker_id: int, state: WorkerState):
        if worker_id not in self.__id_to_procdata:
            self.__logger.warning(f'reported state {state} for worker {worker_id} that DOESN\'T BELONG TO US')
            return

        if self.__id_to_procdata[worker_id].state != state:
            self.__id_to_procdata[worker_id].state = state
            self.__id_to_procdata[worker_id].state_entering_time = time.time()
            self.__poke_event.set()


async def async_main(argv):
    logger = get_logger('simple_worker_pool')
    parser = argparse.ArgumentParser('lifeblood pool simple')
    parser.add_argument('--min-idle', '-m',
                        dest='minimal_idle_to_ensure',
                        default=1, type=int,
                        help='worker pool will ensure at least this amount of workers is up idle (default=1)')
    parser.add_argument('--min-total',
                        dest='minimal_total_to_ensure',
                        default=0, type=int,
                        help='worker pool will ensure at least this amount of workers is up total (default=0)')
    parser.add_argument('--max', '-M',
                        dest='maximum_total',
                        default=256, type=int,
                        help='no more than this amount of workers will be run locally at the same time (default=256)')
    parser.add_argument('--priority', choices=tuple(x.name for x in ProcessPriorityAdjustment), default=ProcessPriorityAdjustment.LOWER.name, help='pass to spawned workers: adjust child process priority')

    opts = parser.parse_args(argv)
    opts.priority = [x for x in ProcessPriorityAdjustment if x.name == opts.priority][0]  # there MUST be exactly 1 match

    graceful_closer_no_reentry = False

    def graceful_closer(*args):
        nonlocal graceful_closer_no_reentry
        if graceful_closer_no_reentry:
            print('DOUBLE SIGNAL CAUGHT: ALREADY EXITING')
            return
        graceful_closer_no_reentry = True
        logger.info('SIGINT/SIGTERM caught')
        nonlocal noloop
        noloop = True
        stop_event.set()
        if pool:
            pool.stop()

    noasync_do_close = False

    def noasync_windows_graceful_closer_event(*args):
        nonlocal noasync_do_close
        noasync_do_close = True

    async def windows_graceful_closer():
        while not noasync_do_close:
            await asyncio.sleep(1)
        graceful_closer()

    logger.debug(f'starting {__name__} with: ' + ', '.join(f'{key}={val}' for key, val in opts.__dict__.items()))
    pool = None
    noloop = False  # TODO: add arg

    # override event handlers
    win_signal_waiting_task = None
    try:
        asyncio.get_event_loop().add_signal_handler(signal.SIGINT, graceful_closer)
        asyncio.get_event_loop().add_signal_handler(signal.SIGTERM, graceful_closer)
    except NotImplementedError:  # solution for windows
        signal.signal(signal.SIGINT, noasync_windows_graceful_closer_event)
        signal.signal(signal.SIGBREAK, noasync_windows_graceful_closer_event)
        win_signal_waiting_task = asyncio.create_task(windows_graceful_closer())
    #

    stop_event = asyncio.Event()
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
            pool = await create_worker_pool(WorkerType.STANDARD, scheduler_address=(ip, port), **opts.__dict__)
        except Exception:
            logger.exception('could not start the pool')
        else:
            await pool.wait_till_stops()
            logger.info('pool quited')
        if noloop:
            break

    if win_signal_waiting_task is not None:
        if not win_signal_waiting_task.done():
            win_signal_waiting_task.cancel()
    logger.info('pool loop stopped')


def main(argv):
    try:
        asyncio.run(async_main(argv))
    except KeyboardInterrupt:
        get_logger('simple_worker_pool').warning('SIGINT caught where it wasn\'t supposed to be caught')

