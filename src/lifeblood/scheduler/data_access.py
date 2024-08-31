import sys
import aiosqlite
import sqlite3
import random
import struct
from dataclasses import dataclass
from ..attribute_serialization import serialize_attributes
from ..db_misc import sql_init_script
from ..expiring_collections import ExpiringValuesSetMap
from ..enums import TaskState, InvocationState
from ..worker_metadata import WorkerMetadata
from ..logging import get_logger
from ..shared_lazy_sqlite_connection import SharedLazyAiosqliteConnection
from .. import aiosqlite_overlay
from ..environment_resolver import EnvironmentResolverArguments
from ..scheduler_config_provider_base import SchedulerConfigProviderBase
from ..worker_resource_definition import WorkerResourceDataType
from ..invocationjob import InvocationResources

from typing import Any, Dict, Iterable, List, Optional, Tuple, Type, Union

SCHEDULER_DB_FORMAT_VERSION = 5


@dataclass
class InvocationStatistics:
    invoking: int
    in_progress: int
    finished_good: int
    finished_bad: int
    total: int


@dataclass
class TaskSpawnData:
    name: str
    parent_id: Optional[int]
    attributes: Dict[str, Any]
    state: TaskState
    node_id: int
    node_output_name: str
    environment_resolver_arguments: Optional[EnvironmentResolverArguments]


class DataAccess:
    def __init__(
            self,
            *,
            config_provider: SchedulerConfigProviderBase
    ):
        self.__logger = get_logger('scheduler.data_access')
        self.__db_path: str = config_provider.main_database_location()
        self.__db_timeout: float = config_provider.main_database_connection_timeout()

        # "public" members
        self.mem_cache_workers_resources: dict = {}
        self.mem_cache_workers_state: dict = {}
        self.__mem_cache_invocations: dict = {}
        #

        self.__task_blocking_values: Dict[int, int] = {}
        # on certain submission errors we might want to ban hwid for some time, as it can be assumed
        # that consecutive submission attempts will result in the same error (like package resolution error)
        self.__banned_hwids_per_task: ExpiringValuesSetMap = ExpiringValuesSetMap()
        self.__ban_time = config_provider.hardware_ban_timeout()

        self.__workers_metadata: Dict[int, WorkerMetadata] = {}
        #
        # ensure database is initialized
        with sqlite3.connect(self.__db_path) as con:
            con.executescript(sql_init_script)

        # upgrade db definitions
        with sqlite3.connect(self.__db_path) as con:
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

        # update resource table straight away
        # for now the logic is to keep existing columns
        with sqlite3.connect(self.__db_path) as con:
            con.row_factory = sqlite3.Row
            cur = con.execute('PRAGMA table_info(resources)')
            resource_rows = {x['name']: x for x in cur.fetchall() if x['name'] != 'hwid'}
            cur.close()

            need_commit = False
            # create tables for resources
            for res_def in config_provider.hardware_resource_definitions():
                col_type, col_def = resource_definition_to_sql_type_and_default(res_def)
                if res_def.name in resource_rows:  # skip existing
                    # dflt_value is string repr of the number, so we have to convert col_def to compare. not the best way for floating numbers
                    if resource_rows[res_def.name]['type'] != col_type or resource_rows[res_def.name]['dflt_value'] != str(col_def):
                        self.__logger.warning(f'existing resource definition changed for "{res_def.name}", recreating, all resource data will be lost')
                        con.execute(f'ALTER TABLE resources DROP COLUMN "{res_def.name}"')
                        con.execute(f'ALTER TABLE resources DROP COLUMN "total_{res_def.name}"')
                    else:
                        continue
                self.__logger.debug(f'adding new resource "{res_def.name}" of type {col_type} ({col_def}) into the table')
                con.execute(f'ALTER TABLE resources ADD COLUMN "{res_def.name}" {col_type} NOT NULL DEFAULT {col_def}')
                con.execute(f'ALTER TABLE resources ADD COLUMN "total_{res_def.name}" {col_type} NOT NULL DEFAULT {col_def}')
                need_commit = True
            if need_commit:
                con.commit()

            # create tables for devices
            for dev_def in config_provider.hardware_device_type_definitions():
                # delete existing definitions
                dev_type_table_name = f'hardware_device_type__{dev_def.name}'

                dev_res_sql_parts = [
                    '"dev_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT',
                    '"hw_dev_name" TEXT NOT NULL',  # unique within hwid index of the device
                    '"available" INTEGER NOT NULL DEFAULT 1',
                    '"hwid" INTEGER NOT NULL',
                ]
                for dev_res_dev in dev_def.resources:
                    col_type, col_def = resource_definition_to_sql_type_and_default(res_def)
                    dev_res_sql_parts.append(f'"res__{dev_res_dev.name}" {col_type} DEFAULT {col_def}')
                dev_res_sql_parts.append('FOREIGN KEY("hwid") REFERENCES "resources"("hwid") ON UPDATE CASCADE ON DELETE CASCADE')
                dev_res_sql_parts.append('UNIQUE("hwid","hw_dev_name")')

                con.execute(f'DROP TABLE IF EXISTS "{dev_type_table_name}"')
                con.execute(f'CREATE TABLE "{dev_type_table_name}" ({",".join(dev_res_sql_parts)})')


    async def create_node(self, node_type: str, node_name: str, *, con: Optional[aiosqlite.Connection] = None) -> int:
        # TODO: scheduler must use this instead of creating directly
        #  this should be done as part of a bigger refactoring
        if con is None:
            async with self.data_connection() as con:
                ret = await self.create_node(node_type, node_name, con=con)
                await con.commit()
            return ret

        async with con.execute('INSERT INTO "nodes" ("type", "name") VALUES (?,?)',
                               (node_type, node_name)) as cur:
            ret = cur.lastrowid
        return ret

    async def create_task(self, newtask: TaskSpawnData, *, con: Optional[aiosqlite.Connection] = None) -> int:
        # TODO: scheduler must use this instead of creating directly
        #  this should be done as part of a bigger refactoring
        # TODO: add test that ensures input validity check, including db consistency (node_id, parent_id)
        if con is None:
            async with self.data_connection() as con:
                ret = await self.create_task(newtask, con=con)
                await con.commit()
            return ret

        async with con.execute('INSERT INTO tasks ("name", "attributes", "parent_id", "state", "node_id", "node_output_name", "environment_resolver_data") VALUES (?, ?, ?, ?, ?, ?, ?)',
                               (newtask.name, await serialize_attributes(newtask.attributes), newtask.parent_id,
                                newtask.state.value,
                                newtask.node_id, newtask.node_output_name,
                                newtask.environment_resolver_arguments.serialize() if newtask.environment_resolver_arguments is not None else None)) as newcur:
            new_id = newcur.lastrowid
        return new_id

    async def housekeeping(self):
        """
        i don't like this explicit cleanup
        TODO: when all db manipulation is moved to data_access - there should be no need for this crap
        """
        await self.__prune_cached_invocation_progress()

    def debug_get_cached_data_size(self) -> int:
        """
        Returns size in bytes of in-memory cached tables
        This info is provided for profiling/debugging purposes and is somewhat of an estimation.
        """
        def _gszofdr(obj) -> int:
            sz = sys.getsizeof(obj)
            for k, v in obj.items():
                sz += sys.getsizeof(k)
                sz += sys.getsizeof(v)
                if isinstance(v, dict):
                    sz += _gszofdr(v)
            return sz

        return _gszofdr({1: self.__mem_cache_invocations, 2: self.mem_cache_workers_resources, 3: self.mem_cache_workers_state})

    #
    async def __prune_cached_invocation_progress(self):
        async with self.data_connection() as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT "id" FROM invocations WHERE state == ?',
                                   (InvocationState.IN_PROGRESS.value,)) as inv:
                filtered_invocs = set(x['id'] for x in await inv.fetchall())
        for inv in tuple(self.__mem_cache_invocations.keys()):
            if inv not in filtered_invocs:  # Note: since task finish/cancel reporting is in the same thread as this - there will not be race conditions for del, as there's no await
                self.clear_invocation_progress(inv)

    def clear_invocation_progress(self, invocation_id: int):
        if invocation_id in self.__mem_cache_invocations:
            self.__mem_cache_invocations.pop(invocation_id)

    def set_invocation_progress(self, invocation_id: int, progress: float):
        self.__mem_cache_invocations.setdefault(invocation_id, {})['progress'] = progress

    def get_invocation_progress(self, invocation_id: int) -> Optional[float]:
        """
        None is returned if there is no data for progress for given invocation_id
        """
        return self.__mem_cache_invocations.get(invocation_id, {}).get('progress', None)
    #

    async def get_invocation_resources_assigned_to(self, worker_id: int) -> Optional[InvocationResources]:
        if (res_data := self.mem_cache_workers_resources.get(worker_id)) is None:
            return None
        if 'res' not in res_data and 'dev' not in res_data:
            return None
        res = dict(res_data.get('res', {}))
        dev = {}
        async with self.data_connection() as con:
            con.row_factory = aiosqlite.Row
            for dev_type, dev_id_list in res_data.get('dev', {}).items():
                if len(dev_id_list) == 0:
                    continue
                dev_type_table_name = f'hardware_device_type__{dev_type}'
                async with con.execute(f'SELECT hw_dev_name FROM "{dev_type_table_name}" WHERE dev_id IN ({",".join(str(x) for x in dev_id_list)})') as cur:
                    dev[dev_type] = [row['hw_dev_name'] for row in await cur.fetchall()]
        return InvocationResources(res, dev)

    async def hint_task_needs_blocking(self, task_id: int, *, inc_amount: int = 1, con: Optional[aiosqlite.Connection] = None) -> bool:
        """
        Indicate "intent" that given task needs to be blocked for time being.

        counter is limited by max 1, but not min limited
        this is to avoid certain race conditions but allowing for some extra processing, which does not hurt
        and is something we can afford.
        TODO: add link to proposal

        It should be safe to call these 3 task-blocking related methods from multiple async tasks and even threads,
         as data change is protected by a db transaction
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

        It should be safe to call these 3 task-blocking related methods from multiple async tasks and even threads,
         as data change is protected by a db transaction
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

        It should be safe to call these 3 task-blocking related methods from multiple async tasks and even threads,
         as data change is protected by a db transaction
        """
        if con is None:
            async with self.data_connection() as con:
                ret = await self.reset_task_blocking(task_id, con=con)
                await con.commit()
            return ret

        # if it's not there - do nothing
        if task_id not in self.__task_blocking_values:
            return

        if not con.in_transaction:
            await con.execute('BEGIN IMMEDIATE')
        await self.hint_task_needs_unblocking(task_id, dec_amount=self.__task_blocking_values[task_id], con=con)
        # we just remove task_id from dict, as default value is 0
        # And we ensure it is done within a transaction
        self.__task_blocking_values.pop(task_id)

    #

    def suspend_hwid(self, task_id: int, hwid: int):
        self.__banned_hwids_per_task.add_expiring_value(task_id, hwid, self.__ban_time)

    # task query

    def get_suspended_hwids(self, task_id: int) -> Iterable[int]:
        return self.__banned_hwids_per_task.get_values(task_id, prune=True)

    def prune_suspended_hwids(self):
        self.__banned_hwids_per_task.prune()

    async def is_task_blocked(self, task_id: int, *, con: Optional[aiosqlite.Connection] = None) -> bool:
        """
        is task blocked
        TODO: use get_task_state when it's moved here from scheduler
        """
        if con is None:
            async with self.data_connection() as con:
                con.row_factory = aiosqlite.Row
                ret = await self.is_task_blocked(task_id, con=con)
                assert not con.in_transaction, 'expectations failure'
            return ret

        async with con.execute('SELECT "state" FROM tasks WHERE "id" == ?', (task_id,)) as cur:
            row = await cur.fetchone()
        if row is None:
            raise ValueError(f'task {task_id} does not exist')
        return TaskState(row['state']) in (TaskState.WAITING_BLOCKED, TaskState.POST_WAITING_BLOCKED)

    async def get_task_state(self, task_id: int, *, con: Optional[aiosqlite.Connection] = None) -> Tuple[TaskState, bool]:
        """
        get task state given task id

        :return: tuple of TaskState and paused
        """
        if con is None:
            async with self.data_connection() as con:
                con.row_factory = aiosqlite.Row
                ret = await self.get_task_state(task_id, con=con)
                assert not con.in_transaction, 'expectations failure'
            return ret

        async with con.execute('SELECT "state", paused FROM tasks WHERE "id" == ?', (task_id,)) as cur:
            res = await cur.fetchone()
        if res is None:
            raise ValueError('task with specified id was not found')

        return TaskState(res['state']), res['paused']

    async def get_task_node(self, task_id, *, con: Optional[aiosqlite.Connection] = None) -> int:
        """
        get node_id of the node the given task belongs to at the moment

        :return: tuple of node_id
        """
        if con is None:
            async with self.data_connection() as con:
                con.row_factory = aiosqlite.Row
                ret = await self.get_task_node(task_id, con=con)
                assert not con.in_transaction, 'expectations failure'
            return ret

        async with con.execute('SELECT "node_id" FROM tasks WHERE "id" == ?', (task_id,)) as cur:
            res = await cur.fetchone()
        if res is None:
            raise ValueError('task with specified id was not found')

        return res['node_id']


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
        # TODO: con.row_factory = aiosqlite.Row must be here, ALMOST all places use it anyway, need to prune
        return aiosqlite_overlay.connect(self.__db_path, timeout=self.__db_timeout, pragmas_after_connect=('synchronous=NORMAL',))

    def lazy_data_transaction(self, key_name: str):
        return SharedLazyAiosqliteConnection(None, self.__db_path, key_name, timeout=self.__db_timeout)

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
        if from_version < 1 or to_version > 5:
            raise NotImplementedError(f"Don't know how to update db schema from v{from_version} to v{to_version}")
        if to_version < from_version:
            raise ValueError(f'to_version cannot be less than from_version ({to_version}<{from_version})')
        if to_version - from_version > 1:
            need_commit = False
            for i in range(from_version, to_version):
                need_commit = self.__database_schema_upgrade(con, i, i + 1) or need_commit
            return need_commit

        # at this point we are sure that from_version +1 = to_version
        assert from_version + 1 == to_version
        self.__logger.warning(f'updating database schema from {from_version} to {to_version}')

        # actual logic
        if to_version == 2:
            # need to ensure new node_object_state field is present
            con.execute('ALTER TABLE "nodes" ADD COLUMN "node_object_state" BLOB')
            return True
        if to_version == 3:
            # there was a bug in prev init script, wrong column in index, now it randomly pops on rename,
            # so we need to recreate that index
            con.executescript(
                'DROP INDEX task_group_attrs_state_creator_idx;'
                'CREATE INDEX IF NOT EXISTS "task_group_attrs_state_creator_idx" ON "task_group_attributes" ('
                '    "state",'
                '    "creator"'
                ');'
            )
            # need to set default values for existing resource columns
            for col_name in ('cpu_count', 'total_cpu_count', 'cpu_mem', 'total_cpu_mem', 'gpu_count', 'total_gpu_count', 'gpu_mem', 'total_gpu_mem'):
                con.execute(f'ALTER TABLE "resources" RENAME COLUMN "{col_name}" TO "__old_{col_name}"')
                con.execute(f'ALTER TABLE "resources" ADD COLUMN "{col_name}" INTEGER NOT NULL DEFAULT 0')
                con.execute(f'UPDATE "resources" SET "{col_name}" = "__old_{col_name}"')
                con.execute(f'ALTER TABLE "resources" DROP COLUMN "__old_{col_name}"')
            return True
        if to_version == 4:
            # need to fix _invoc_requirement_clause
            # this is a very dirty fix that might break some task processing
            con.execute('UPDATE "tasks" SET "state"=?, "_invoc_requirement_clause"=NULL WHERE _invoc_requirement_clause IS NOT NULL', (TaskState.WAITING.value,))
            return True
        if to_version == 5:
            con.execute('PRAGMA legacy_alter_table=ON')
            con.execute('ALTER TABLE "resources" RENAME TO "__old_resources"')
            con.executescript('''\
CREATE TABLE IF NOT EXISTS "resources" (
    "hwid" INTEGER NOT NULL UNIQUE,
    PRIMARY KEY("hwid")
) WITHOUT ROWID;''')
            cur = con.execute('PRAGMA table_info(__old_resources)')
            rows = cur.fetchall()
            cur.close()
            for row in rows:
                if row['name'] == 'hwid':
                    continue
                con.execute(f'ALTER TABLE resources ADD COLUMN "{row["name"]}" {row["type"]} NOT NULL DEFAULT {row["dflt_value"]}')
            con.execute('INSERT INTO "resources" SELECT * FROM "__old_resources"')
            con.execute('DROP TABLE "__old_resources"')
            con.execute('PRAGMA legacy_alter_table=OFF')
            con.execute('PRAGMA integrity_check')
            return True


def resource_definition_to_sql_type_and_default(res_def) -> Tuple[str, Union[float, int, str]]:
    return {
        WorkerResourceDataType.GENERIC_FLOAT: ('INTEGER', float(res_def.default)),  # use INTEGER for floats, as it is more flexible in sqlite, see https://sqlite.org/flextypegood.html
        WorkerResourceDataType.GENERIC_INT: ('INTEGER', int(res_def.default)),
        WorkerResourceDataType.SHARABLE_COMPUTATIONAL_UNIT: ('INTEGER', int(res_def.default)),
        WorkerResourceDataType.MEMORY_BYTES: ('INTEGER', int(res_def.default)),
    }[res_def.type]
