import aiosqlite
import asyncio
import time
from .. import logging
from ..worker_messsage_processor import WorkerControlClient
from ..enums import WorkerState, InvocationState, WorkerPingState, WorkerPingReply
from ..ui_protocol_data import TaskDelta
from .scheduler_component_base import SchedulerComponentBase
from ..config import get_config
from ..net_messages.address import AddressChain
from ..net_messages.exceptions import MessageTransferError, MessageTransferTimeoutError

from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:  # TODO: maybe separate a subset of scheduler's methods to smth like SchedulerData class, or idunno, for now no obvious way to separate, so having a reference back
    from .scheduler import Scheduler


class Pinger(SchedulerComponentBase):
    def __init__(self, scheduler: "Scheduler"):
        super().__init__(scheduler)
        self.__pinger_logger = logging.get_logger('scheduler.worker_pinger')
        config = get_config('scheduler')

        self.__ping_interval = config.get_option_noasync('scheduler.pinger.ping_interval', 1)  # inverval for active workers (workers doing work)
        self.__ping_idle_interval = config.get_option_noasync('scheduler.pinger.ping_idle_interval', 10)  # interval for idle workers
        self.__ping_off_interval = config.get_option_noasync('scheduler.pinger.ping_off_interval', 30)  # interval for off/errored workers  (not really used since workers need to report back first)
        self.__dormant_mode_ping_interval_multiplier = config.get_option_noasync('scheduler.pinger.dormant_ping_multiplier', 6)
        self.__ping_interval_mult = 1

    def _main_task(self):
        return self.worker_pinger()

    def _my_sleep(self):
        self.__ping_interval_mult = self.__dormant_mode_ping_interval_multiplier

    def _my_wake(self):
        self.__ping_interval_mult = 1
        self.poke()

    def set_pinger_interval_multiplier(self, multiplier: float):
        self.__ping_interval_mult = multiplier

    async def _set_worker_state(self, wid: int, state: WorkerState, con: Optional[aiosqlite.Connection] = None, nocommit: bool = False) -> None:
        if con is None:
            # TODO: safe check table and field, allow only text
            # TODO: handle db errors
            async with self.scheduler.data_access.data_connection() as con:
                await con.execute("UPDATE workers SET state = ? WHERE id = ?", (state.value, wid))
                if not nocommit:
                    await con.commit()
        else:
            await con.execute("UPDATE workers SET state = ? WHERE id = ?", (state.value, wid))
            if not nocommit:
                await con.commit()

    async def _set_value(self, table: str, field: str, wid: int, value: Any, con: Optional[aiosqlite.Connection] = None, nocommit: bool = False) -> None:
        if con is None:
            # TODO: safe check table and field, allow only text
            # TODO: handle db errors
            async with self.scheduler.data_access.data_connection() as con:
                await con.execute("UPDATE %s SET %s= ? WHERE id = ?" % (table, field), (value, wid))
                if not nocommit:
                    await con.commit()
        else:
            await con.execute("UPDATE %s SET %s = ? WHERE id = ?" % (table, field), (value, wid))
            if not nocommit:
                await con.commit()

    async def _iter_iter_func(self, worker_row):
        # This function currently runs as a separate task in the main thread
        # this operates on the same worker data task_processor operates on, therefore
        # this MUST NOT interfere with usual scheduler business
        # here we only write to DB when:
        # * a super obvious error is detected, like when there is an error pinging worker for some significant time
        # * a worker reappeared after being in Error state  # TODO: actually this can be resolved on worker side - it pings, sees error, reintroduces
        async with self.scheduler.data_access.data_connection() as con:
            con.row_factory = aiosqlite.Row

            async def _check_lastseen_and_drop_invocations(switch_state_on_reset: Optional[WorkerState] = None) -> bool:
                if worker_row['last_seen'] is not None and time.time() - worker_row['last_seen'] < 64:  # TODO: make this time a configurable parameter
                    return False
                if switch_state_on_reset is not None:
                    await self._set_worker_state(worker_row['id'], switch_state_on_reset, con, nocommit=True)
                need_commit = (await self.scheduler.reset_invocations_for_worker(worker_row['id'], con, also_update_resources=True))
                return need_commit or switch_state_on_reset is not None

            self.__pinger_logger.debug('    :: pinger started')
            self.scheduler.data_access.mem_cache_workers_state[worker_row['id']]['ping_state'] = WorkerPingState.CHECKING.value
            self.scheduler.data_access.mem_cache_workers_state[worker_row['id']]['last_checked'] = int(time.time())

            try:
                addr = AddressChain(worker_row['last_address'])
            except ValueError:
                self.__pinger_logger.debug(f'    :: malformed address "{addr}"')
                self.scheduler.data_access.mem_cache_workers_state[worker_row['id']]['ping_state'] = WorkerPingState.ERROR.value
                await self._set_worker_state(worker_row['id'], WorkerState.ERROR, con, nocommit=True)
                await _check_lastseen_and_drop_invocations()
                await con.commit()
                return

            self.__pinger_logger.debug(f'    :: checking {addr}')

            try:
                with WorkerControlClient.get_worker_control_client(addr, self.scheduler.message_processor()) as client:  # type: WorkerControlClient
                    # TODO: lower message timeout
                    ping_code, pvalue = await client.ping()
                    self.__pinger_logger.debug(f'    :: {addr} is {ping_code}')
            except MessageTransferTimeoutError:
                self.__pinger_logger.info(f'    :: network timeout {addr}')
                self.scheduler.data_access.mem_cache_workers_state[worker_row['id']]['ping_state'] = WorkerPingState.ERROR.value
                if await _check_lastseen_and_drop_invocations(switch_state_on_reset=WorkerState.ERROR):  # TODO: maybe give it a couple of tries before declaring a failure?
                    await con.commit()
                return
            except MessageTransferError as e:
                self.__pinger_logger.info(f'    :: host/route down {addr} {e.wrapped_exception()}')
                self.scheduler.data_access.mem_cache_workers_state[worker_row['id']]['ping_state'] = WorkerPingState.OFF.value
                if await _check_lastseen_and_drop_invocations(switch_state_on_reset=WorkerState.OFF):
                    await con.commit()
                return
            except Exception as e:
                self.__pinger_logger.info(f'    :: ping failed {addr} {type(e)}, {e}')
                self.scheduler.data_access.mem_cache_workers_state[worker_row['id']]['ping_state'] = WorkerPingState.ERROR.value
                if await _check_lastseen_and_drop_invocations(switch_state_on_reset=WorkerState.OFF):
                    await con.commit()
                return

            # at this point we sure to have received a reply
            # fixing possibly inconsistent worker states
            # this inconsistencies should only occur shortly after scheduler restart
            # due to desync of still working workers and scheduler
            workerstate = await self.scheduler.get_worker_state(worker_row['id'], con=con)
            if workerstate == WorkerState.OFF:
                # there can be race conditions (theoretically) if worker saz goodbye right after getting the ping, so we get OFF state from db. or all vice-versa
                # so there is nothing but warnings here. inconsistencies should be reliably resolved by worker
                if ping_code == WorkerPingReply.IDLE:
                    self.__pinger_logger.warning(f'worker {worker_row["id"]} is marked off, but pinged as IDLE... have scheduler been restarted recently? waiting for worker to ping me and resolve this inconsistency...')
                    # await self._set_worker_state(worker_row['id'], WorkerState.IDLE, con=con, nocommit=True)
                elif ping_code == WorkerPingReply.BUSY:
                    self.__pinger_logger.warning(f'worker {worker_row["id"]} is marked off, but pinged as BUSY... have scheduler been restarted recently? waiting for worker to ping me and resolve this inconsistency...')
                    # await self._set_worker_state(worker_row['id'], WorkerState.BUSY, con=con, nocommit=True)

            if ping_code == WorkerPingReply.IDLE:  # TODO, just like above - add warnings, but leave solving to worker
                pass
            elif ping_code == WorkerPingReply.BUSY:
                # in this case received pvalue is current task's progress. u cannot rely on it's precision: some invocations may not support progress reporting
                # TODO: think, can there be race condition here so that worker is already doing something else?
                inv_id = None
                async with con.execute('SELECT "id", task_id FROM invocations WHERE "state" = ? AND "worker_id" = ?', (InvocationState.IN_PROGRESS.value, worker_row['id'])) as invcur:
                    inv_row = await invcur.fetchone()
                    if inv_row is not None:
                        inv_id = inv_row['id']
                        task_id = inv_row['task_id']
                if inv_id is not None:
                    if inv_id not in self.scheduler.data_access.mem_cache_invocations:
                        self.scheduler.data_access.mem_cache_invocations[inv_id] = {}
                    self.scheduler.data_access.mem_cache_invocations[inv_id].update({'progress': pvalue})  # Note: this in theory AND IN PRACTICE causes racing with del on task finished/cancelled.
                    self.scheduler.ui_state_access.scheduler_reports_task_updated(TaskDelta(task_id, progress=pvalue))
                    # Therefore additional cleanup needed later - still better than lock things or check too hard

            else:
                raise NotImplementedError(f'not a known ping_code {ping_code}')

            self.scheduler.data_access.mem_cache_workers_state[worker_row['id']]['ping_state'] = WorkerPingState.WORKING.value
            self.scheduler.data_access.mem_cache_workers_state[worker_row['id']]['last_seen'] = int(time.time())
            if worker_row['state'] == WorkerState.ERROR.value:  # so we thought worker had a network error, but now it's all fine
                await self._set_worker_state(worker_row['id'], workerstate)

            self.__pinger_logger.debug('    :: %s', ping_code)

    #
    # pinger task
    async def worker_pinger(self):
        """
        one of main constantly running coroutines
        responsible for pinging all the workers once in a while in separate tasks each
        TODO: test how well this approach works for 1000+ workers
        :return: NEVER !!
        """

        tasks = []
        stop_task = asyncio.create_task(self._stop_event.wait())
        wakeup_task = asyncio.create_task(self._poke_event.wait())
        self._main_task_is_ready_now()
        while not self._stop_event.is_set():
            nowtime = time.time()

            self.__pinger_logger.debug('    ::selecting workers...')
            async with self.scheduler.data_access.data_connection() as con:
                con.row_factory = aiosqlite.Row
                async with con.execute('SELECT '
                                       '"id", last_address, worker_type, hwid, state '
                                       'FROM workers '
                                       'WHERE state != ? AND state != ?', (WorkerState.UNKNOWN.value, WorkerState.OFF.value)  # so we don't bother to ping UNKNOWN ones, until they hail us and stop being UNKNOWN
                                       # 'WHERE tmp_workers_states.ping_state != ?', (WorkerPingState.CHECKING.value,)
                                       ) as cur:
                    all_rows = await cur.fetchall()
            for row in all_rows:
                row = dict(row)
                if row['last_address'] is None:
                    continue
                for cached_field in ('last_seen', 'last_checked', 'ping_state'):
                    row[cached_field] = self.scheduler.data_access.mem_cache_workers_state[row['id']][cached_field]
                if row['ping_state'] == WorkerPingState.CHECKING.value:  # TODO: this check could happen in the very beginning of this loop... too sleepy now to blindly move it
                    continue

                time_delta = nowtime - (row['last_checked'] or 0)
                if row['state'] == WorkerState.BUSY.value:
                    tasks.append(asyncio.create_task(self._iter_iter_func(row)))
                elif row['state'] == WorkerState.IDLE.value and time_delta > self.__ping_idle_interval * self.__ping_interval_mult:
                    tasks.append(asyncio.create_task(self._iter_iter_func(row)))
                else:  # worker state is error or off
                    if time_delta > self.__ping_off_interval * self.__ping_interval_mult:
                        tasks.append(asyncio.create_task(self._iter_iter_func(row)))

            # now clean the list
            tasks = [x for x in tasks if not x.done()]
            self.__pinger_logger.debug('    :: remaining ping tasks: %d', len(tasks))

            # now wait
            if wakeup_task is not None:
                sleeping_tasks = (stop_task, wakeup_task)
            else:
                wakeup_task = asyncio.create_task(self._poke_event.wait())
                sleeping_tasks = (stop_task, wakeup_task)

            done, _ = await asyncio.wait(sleeping_tasks, timeout=self.__ping_interval * self.__ping_interval_mult, return_when=asyncio.FIRST_COMPLETED)
            if wakeup_task in done:
                self._reset_poke_event()
                wakeup_task = None

            # end when stop is set
            if stop_task in done:
                break

        # FINALIZING PINGER
        self.__pinger_logger.info('finishing worker pinger...')
        if not wakeup_task.done():
            wakeup_task.cancel()
        if not stop_task.done():
            stop_task.cancel()
        if len(tasks) > 0:
            self.__pinger_logger.debug(f'waiting for {len(tasks)} pinger tasks...')
            _, pending = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED, timeout=5)
            self.__pinger_logger.debug(f'waiting enough, cancelling {len(pending)} tasks')
            for task in pending:
                task.cancel()
        self.__pinger_logger.info('worker pinger finished')
