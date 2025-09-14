import aiosqlite
import time
from datetime import datetime
from .. import logging
from ..enums import WorkerState, WorkerPingState, WorkerPingReply
from .ping_producer_base import PingEntity, PingProducerBase, PingEntityIdleness, PingReply
from ..net_messages.address import AddressChain
from ..net_messages.exceptions import MessageTransferError, MessageTransferTimeoutError
from ..timestamp import global_timestamp_int
from .data_access import DataAccess
from .scheduler_core import SchedulerCore

from typing import Optional, Iterable


class WorkerPingEntity(PingEntity):
    def __init__(self, address: AddressChain, wid: int, state: WorkerState, last_checked: datetime):
        super().__init__()
        self.__id = wid
        self.__address = address
        self.__state_seen_by_scheduler = state
        self.__last_checked = last_checked

        self.__idleness = PingEntityIdleness.SLEEPING_IDLE
        if state == WorkerState.BUSY:
            self.__idleness = PingEntityIdleness.ACTIVE
        elif state == WorkerState.IDLE:
            self.__idleness = PingEntityIdleness.WORKING_IDLE

    def worker_id(self) -> int:
        return self.__id

    def address(self) -> AddressChain:
        return self.__address

    def idleness(self) -> PingEntityIdleness:
        return self.__idleness

    def last_checked(self) -> datetime:
        return self.__last_checked

    def ping_data(self) -> dict:
        return {
            'seen_as': self.__state_seen_by_scheduler.value
        }


class WorkerPingProducer(PingProducerBase):
    def __init__(self, scheduler: SchedulerCore, data_access: DataAccess):
        super().__init__()
        self.__data_access = data_access
        self.__scheduler = scheduler  # TODO: get rid of this legacy dep

        self.__pinger_logger = logging.get_logger('scheduler.worker_pinger.processor')

    async def __check_lastseen_and_drop_invocations(self, wid: int, last_seen: int, *, switch_state_on_reset: WorkerState):
        if last_seen is not None and global_timestamp_int() - last_seen < 64:  # TODO: make this time a configurable parameter
            return False

        self.__pinger_logger.info(f'    :: Resetting worker state to {switch_state_on_reset}')
        async with self.__data_access.data_connection() as con:
            con.row_factory = aiosqlite.Row
            await self._set_worker_state(wid, switch_state_on_reset, con=con, nocommit=True)
            self.__pinger_logger.info(f'    :: Resetting worker resources')
            await self.__scheduler.reset_invocations_for_worker(wid, con, also_update_resources=True)
            await con.commit()

    async def select_entities(self) -> Iterable[PingEntity]:
        async with self.__data_access.data_connection() as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT '
                                   '"id", last_address, worker_type, hwid, state '
                                   'FROM workers '
                                   'WHERE state != ? AND state != ?', (WorkerState.UNKNOWN.value, WorkerState.OFF.value)  # so we don't bother to ping UNKNOWN ones, until they hail us and stop being UNKNOWN
                                   # 'WHERE tmp_workers_states.ping_state != ?', (WorkerPingState.CHECKING.value,)
                                   ) as cur:
                all_rows = await cur.fetchall()
        entities = []
        for row in all_rows:
            row = dict(row)
            for cached_field in ('last_seen', 'last_checked', 'ping_state'):
                row[cached_field] = self.__data_access.mem_cache_workers_state[row['id']][cached_field]
            if row['last_address'] is None:
                continue

            try:
                addr = AddressChain(row['last_address'])
            except ValueError:
                self.__pinger_logger.debug(f'    :: malformed address "{row["last_address"]}"')
                self.__data_access.mem_cache_workers_state[row['id']]['ping_state'] = WorkerPingState.ERROR.value
                await self.__check_lastseen_and_drop_invocations(row['id'], row['last_seen'], switch_state_on_reset=WorkerState.ERROR)
                continue

            entities.append(WorkerPingEntity(addr, row['id'], WorkerState(row['state']), datetime.fromtimestamp(row['last_checked'])))

        return entities

    async def entity_accepted(self, entity: PingEntity):
        assert isinstance(entity, WorkerPingEntity)
        self.__data_access.mem_cache_workers_state[entity.worker_id()]['ping_state'] = WorkerPingState.CHECKING.value
        self.__data_access.mem_cache_workers_state[entity.worker_id()]['last_checked'] = global_timestamp_int()

    async def entity_discarded(self, entity: PingEntity):
        assert isinstance(entity, WorkerPingEntity)

    async def entity_reply_received(self, reply: PingReply):
        entity = reply.entity()
        assert isinstance(entity, WorkerPingEntity)

        if reply.is_success():
            self.__pinger_logger.debug('    :: %s is "%s"', entity.address(), reply.reply_data())
        else:
            exc = reply.exception()
            if isinstance(exc, MessageTransferTimeoutError):
                self.__data_access.mem_cache_workers_state[entity.worker_id()]['ping_state'] = WorkerPingState.ERROR.value
                switch_worker_to = WorkerState.ERROR
            elif isinstance(exc, MessageTransferError):
                self.__data_access.mem_cache_workers_state[entity.worker_id()]['ping_state'] = WorkerPingState.OFF.value
                switch_worker_to = WorkerState.OFF
            else:  # any other kind of exception
                self.__data_access.mem_cache_workers_state[entity.worker_id()]['ping_state'] = WorkerPingState.ERROR.value
                switch_worker_to = WorkerState.OFF
            await self.__check_lastseen_and_drop_invocations(
                entity.worker_id(),
                self.__data_access.mem_cache_workers_state[entity.worker_id()]['last_seen'],
                switch_state_on_reset=switch_worker_to,
            )
            return

        # at this point we sure to have received a reply
        # fixing possibly inconsistent worker states
        # this inconsistencies should only occur shortly after scheduler restart
        # due to desync of still working workers and scheduler
        reply_data = reply.reply_data()
        if 'status' in reply_data:
            ping_code = WorkerPingReply(reply_data['status'])
        else:  # unexpected data returned
            self.__pinger_logger.error(
                f'worker {entity.worker_id()} at {entity.address()} replied with unexpected data. '
                f'may be version mismatch? worker consistency cannot be established! data: {reply_data}'
            )
            await self.__check_lastseen_and_drop_invocations(
                entity.worker_id(),
                self.__data_access.mem_cache_workers_state[entity.worker_id()]['last_seen'],
                switch_state_on_reset=WorkerState.ERROR,
            )
            return

        workerstate = await self.__scheduler.get_worker_state(entity.worker_id())
        # NOTE: this worker state MAY differ from worker state at the moment of ping creation!
        #  therefore strict actions MUST NOT be taken here without another strict check
        if workerstate == WorkerState.OFF:
            # there can be race conditions (theoretically) if worker saz goodbye right after getting the ping, so we get OFF state from db. or all vice-versa
            # so there is nothing but warnings here. inconsistencies should be reliably resolved by worker
            if ping_code == WorkerPingReply.IDLE:
                self.__pinger_logger.warning(f'worker {entity.worker_id()} is marked off, but pinged as IDLE... have scheduler been restarted recently? waiting for worker to ping me and resolve this inconsistency...')
            elif ping_code == WorkerPingReply.BUSY:
                self.__pinger_logger.warning(f'worker {entity.worker_id()} is marked off, but pinged as BUSY... have scheduler been restarted recently? waiting for worker to ping me and resolve this inconsistency...')

        if ping_code == WorkerPingReply.IDLE:
            if workerstate not in (WorkerState.IDLE, WorkerState.INVOKING):
                self.__pinger_logger.debug('pinger edge case: code is IDLE, but current worker state is not IDLE/INVOKING. '
                                           'this must only happen when pinged exactly at worker state change, as race with pinger is allowed there')
        elif ping_code == WorkerPingReply.BUSY:
            if workerstate != WorkerState.BUSY:
                self.__pinger_logger.debug('pinger edge case: code is BUSY, but current worker state is not BUSY. '
                                           'this must only happen when pinged exactly at worker state change, as race with pinger is allowed there')
        else:
            raise NotImplementedError(f'not a known ping_code {ping_code}')

        self.__data_access.mem_cache_workers_state[entity.worker_id()]['ping_state'] = WorkerPingState.WORKING.value
        self.__data_access.mem_cache_workers_state[entity.worker_id()]['last_seen'] = global_timestamp_int()
        # if worker was in ERROR state - it's up to worker to reintroduce itself to reset all possible errors, we don't do it here

        self.__pinger_logger.debug('    :: %s', ping_code)

    async def _set_worker_state(self, wid: int, state: WorkerState, con: Optional[aiosqlite.Connection] = None, nocommit: bool = False) -> None:
        if con is None:
            async with self.__data_access.data_connection() as con:
                con.row_factory = aiosqlite.Row
                await con.execute("UPDATE workers SET state = ? WHERE id = ?", (state.value, wid))
                if not nocommit:
                    await con.commit()
        else:
            await con.execute("UPDATE workers SET state = ? WHERE id = ?", (state.value, wid))
            if not nocommit:
                await con.commit()
