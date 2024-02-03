import asyncio

import aiosqlite
import sqlite3
import random
import struct
from dataclasses import dataclass
from ..db_misc import sql_init_script
from ..enums import TaskState, InvocationState
from ..worker_metadata import WorkerMetadata
from ..logging import get_logger
from ..scheduler_event_log import SchedulerEventLog
from ..shared_lazy_sqlite_connection import SharedLazyAiosqliteConnection
from .. import aiosqlite_overlay

from typing import Dict, Optional, Tuple

SCHEDULER_DB_FORMAT_VERSION = 2


@dataclass
class InvocationStatistics:
    invoking: int
    in_progress: int
    finished_good: int
    finished_bad: int
    total: int


class DataAccess:
    def __init__(self, db_path, db_connection_timeout):
        self.__logger = get_logger('scheduler.data_access')
        self.db_path: str = db_path
        self.db_timeout: int = db_connection_timeout

        # "public" members
        self.mem_cache_workers_resources: dict = {}
        self.mem_cache_workers_state: dict = {}
        self.mem_cache_invocations: dict = {}
        #

        self.__task_blocking_values: Dict[int, int] = {}

        self.__workers_metadata: Dict[int, WorkerMetadata] = {}
        #
        # ensure database is initialized
        with sqlite3.connect(db_path) as con:
            con.executescript(sql_init_script)
        with sqlite3.connect(db_path) as con:
            con.row_factory = sqlite3.Row
            cur = con.execute('SELECT * FROM lifeblood_metadata')
            metadata = cur.fetchone()  # there should be exactly one single row.
            cur.close()
            if metadata is None:  # if there's no - the DB has not been initialized yet
                # we need 64bit signed id to save into db
                db_uid = random.getrandbits(64)  # this is actual db_uid
                db_uid_signed = struct.unpack('>q', struct.pack('>Q', db_uid))[0]  # this one goes to db
                con.execute('INSERT INTO lifeblood_metadata ("version", "component", "unique_db_id")'
                            'VALUES (?, ?, ?)', (SCHEDULER_DB_FORMAT_VERSION, 'scheduler', db_uid_signed))
                con.commit()
                # reget metadata
                cur = con.execute('SELECT * FROM lifeblood_metadata')
                metadata = cur.fetchone()  # there should be exactly one single row.
                cur.close()
            elif metadata['version'] != SCHEDULER_DB_FORMAT_VERSION:
                self.__database_schema_upgrade(con, metadata['version'], SCHEDULER_DB_FORMAT_VERSION)  # returns true if commit needed, but we do update next line anyway
                con.execute('UPDATE lifeblood_metadata SET "version" = ?', (SCHEDULER_DB_FORMAT_VERSION,))
                con.commit()
                # reget metadata
                cur = con.execute('SELECT * FROM lifeblood_metadata')
                metadata = cur.fetchone()  # there should be exactly one single row.
                cur.close()
            self.__db_uid = struct.unpack('>Q', struct.pack('>q', metadata['unique_db_id']))[0]  # reinterpret signed as unsigned

    async def hint_task_needs_blocking(self, task_id: int, *, inc_amount: int = 1, con: Optional[aiosqlite.Connection] = None) -> bool:
        """
        Indicate "intent" that given task needs to be blocked for time being.

        counter is limited by max 1, but not min limited
        this is to avoid certain race conditions but allowing for some extra processing, which does not hurt
        and is something we can afford.
        TODO: add link to proposal
        """
        if con is None:
            async with self.data_connection() as con:
                ret = await self.hint_task_needs_blocking(task_id, inc_amount=inc_amount, con=con)
                await con.commit()
            return ret

        if not con.in_transaction:
            await con.execute('BEGIN IMMEDIATE')
        self.__task_blocking_values[task_id] = min(1, self.__task_blocking_values.get(task_id, 0) + inc_amount)
        blocking_counter = self.__task_blocking_values[task_id]
        is_blocked = blocking_counter > 0
        if is_blocked:  # if blocking - do blocking instead of simple abort
            await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ? AND ("state" = ? OR "state" = ?)',
                              (TaskState.WAITING_BLOCKED.value, task_id, TaskState.WAITING.value, TaskState.GENERATING.value))
            await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ? AND ("state" = ? OR "state" = ?)',
                              (TaskState.POST_WAITING_BLOCKED.value, task_id, TaskState.POST_WAITING.value, TaskState.POST_GENERATING.value))
        return is_blocked

    async def hint_task_needs_unblocking(self, task_id: int, *, dec_amount: int = 1, con: Optional[aiosqlite.Connection] = None) -> bool:
        """
        unblock blocked task
        """
        if con is None:
            async with self.data_connection() as con:
                ret = await self.hint_task_needs_unblocking(task_id, dec_amount=dec_amount, con=con)
                await con.commit()
            return ret

        if not con.in_transaction:
            await con.execute('BEGIN IMMEDIATE')
        self.__task_blocking_values[task_id] = min(1, self.__task_blocking_values.get(task_id, 0) - dec_amount)
        blocking_counter = self.__task_blocking_values[task_id]
        is_unblocked = blocking_counter <= 0
        if is_unblocked:  # time to unblock
            await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ? AND "state" = ?',
                              (TaskState.WAITING.value, task_id, TaskState.WAITING_BLOCKED.value))
            await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ? AND "state" = ?',
                              (TaskState.POST_WAITING.value, task_id, TaskState.POST_WAITING_BLOCKED.value))
        return is_unblocked

    async def reset_task_blocking(self, task_id: int, *, con: Optional[aiosqlite.Connection] = None):
        """
        reset task's blocking counter
        blocked task will be unblocked
        """
        # if it's not there - do nothing
        if task_id not in self.__task_blocking_values:
            return

        await self.hint_task_needs_unblocking(task_id, dec_amount=self.__task_blocking_values[task_id], con=con)
        # we just remove task_id from dict, as default value is 0
        self.__task_blocking_values.pop(task_id)

    # statistics

    async def invocations_statistics(self, *, con: Optional[aiosqlite.Connection] = None) -> InvocationStatistics:
        if con is None:
            async with self.data_connection() as con:
                con.row_factory = sqlite3.Row
                ret = await self.invocations_statistics(con=con)
                await con.commit()
            return ret

        async with con.execute(
                f'SELECT '
                f'sum(CASE "state" WHEN {InvocationState.INVOKING.value} THEN 1 ELSE 0 END) AS "invoking", '
                f'sum(CASE "state" WHEN {InvocationState.IN_PROGRESS.value} THEN 1 ELSE 0 END) AS "in_progress", '
                f'sum(CASE WHEN "state" == {InvocationState.FINISHED.value} AND "return_code" IS NOT NULL THEN 1 ELSE 0 END) AS "finished_good", '
                f'sum(CASE WHEN "state" == {InvocationState.FINISHED.value} AND "return_code" IS NULL THEN 1 ELSE 0 END) AS "finished_bad", '
                f'count("id") AS "total" '
                f'FROM invocations') as cur:
            row = await cur.fetchone()

        return InvocationStatistics(
            row['invoking'],
            row['in_progress'],
            row['finished_good'],
            row['finished_bad'],
            row['total'],
        )

    #

    @property
    def db_uid(self):
        return self.__db_uid

    def get_worker_metadata(self, worker_hwid: int) -> Optional[WorkerMetadata]:
        return self.__workers_metadata.get(worker_hwid)

    def set_worker_metadata(self, worker_hwid, data: WorkerMetadata):
        self.__workers_metadata[worker_hwid] = data

    def data_connection(self) -> aiosqlite_overlay.ConnectionWithCallbacks:
        return aiosqlite_overlay.connect(self.db_path, timeout=self.db_timeout, pragmas_after_connect=('synchronous=NORMAL',))

    def lazy_data_transaction(self, key_name: str):
        return SharedLazyAiosqliteConnection(None, self.db_path, key_name, timeout=self.db_timeout)

    async def write_back_cache(self):
        self.__logger.info('pinger syncing temporary tables back...')
        async with self.data_connection() as con:
            for wid, cached_row in self.mem_cache_workers_state.items():
                await con.execute('UPDATE workers SET '
                                  'last_seen=?, '
                                  'last_checked=?, '
                                  'ping_state=? '
                                  'WHERE "id"=?',
                                  (cached_row['last_seen'], cached_row['last_checked'], cached_row['ping_state'], wid))
            await con.commit()

    #
    # db schema update logic
    #
    def __database_schema_upgrade(self, con: sqlite3.Connection, from_version: int, to_version: int) -> bool:
        if from_version == to_version:
            return False
        if from_version < 1 or to_version > 2:
            raise NotImplementedError(f"Don't know how to update db schema from v{from_version} to v{to_version}")
        if to_version < from_version:
            raise ValueError(f'to_version cannot be less than from_version ({to_version}<{from_version})')
        if from_version - to_version > 1:
            need_commit = False
            for i in range(from_version, to_version):
                need_commit = self.__database_schema_upgrade(con, from_version, from_version + 1) or need_commit
            return need_commit

        # at this point we are sure that from_version +1 = to_version
        assert from_version + 1 == to_version
        self.__logger.warning(f'updating database schema from {from_version} to {to_version}')

        # actual logic
        if to_version == 2:
            # need to ensure new node_object_state field is present
            con.execute('ALTER TABLE "nodes" ADD COLUMN "node_object_state" BLOB')
            return True
