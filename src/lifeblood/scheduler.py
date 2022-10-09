import sys
import os
from pathlib import Path
import traceback
import time
from datetime import datetime
import json
import sqlite3
import itertools
import random  # for db id generation
import struct
import asyncio
import aiosqlite
import aiofiles
from aiorwlock import RWLock
from contextlib import asynccontextmanager
import signal
import threading  # for bugfix
from concurrent.futures import ThreadPoolExecutor

from . import logging
from . import paths
from .db_misc import sql_init_script
from .worker_task_protocol import WorkerTaskClient, WorkerPingReply, TaskScheduleStatus
from .scheduler_task_protocol import SchedulerTaskProtocol, SpawnStatus
from .scheduler_ui_protocol import SchedulerUiProtocol
from .invocationjob import InvocationJob
from .environment_resolver import EnvironmentResolverArguments
from .uidata import create_uidata
from .broadcasting import create_broadcaster
from .simple_worker_pool import WorkerPool
from .nethelpers import address_to_ip_port, get_default_addr, get_default_broadcast_addr
from .net_classes import WorkerResources
from .taskspawn import TaskSpawn
from .basenode import BaseNode
from .nodethings import ProcessingResult
from .exceptions import *
from . import pluginloader
from .enums import WorkerState, WorkerPingState, TaskState, InvocationState, WorkerType, SchedulerMode, TaskGroupArchivedState
from .config import get_config, create_default_user_config_file, get_local_scratch_path
from .misc import atimeit, alocking
from .shared_lazy_sqlite_connection import SharedLazyAiosqliteConnection
from .defaults import scheduler_port as default_scheduler_port, ui_port as default_ui_port

from typing import Optional, Any, AnyStr, Tuple, List, Iterable, Union, Dict

SCHEDULER_DB_FORMAT_VERSION = 1

# import tracemalloc
# tracemalloc.start()


class Scheduler:
    def __init__(self, db_file_path, *, do_broadcasting=None, helpers_minimal_idle_to_ensure=1,
                 server_addr: Optional[Tuple[str, int]] = None, server_ui_addr: Optional[Tuple[str, int]] = None):
        """
        TODO: add a docstring

        :param db_file_path:
        :param do_broadcasting:
        :param helpers_minimal_idle_to_ensure:
        :param server_addr:
        :param server_ui_addr:
        """
        self.__logger = logging.get_logger('scheduler')
        self.__pinger_logger = logging.get_logger('scheduler.worker_pinger')
        self.__logger.info('loading core plugins')
        pluginloader.init()  # TODO: move it outside of constructor
        self.__node_objects: Dict[int, BaseNode] = {}
        self.__node_objects_locks: Dict[int, RWLock] = {}
        # self.__plugins = {}
        # core_plugins_path = os.path.join(os.path.dirname(__file__), 'core_nodes')
        # for filename in os.listdir(core_plugins_path):
        #     filebasename, fileext = os.path.splitext(filename)
        #     if fileext != '.py':
        #         continue
        #     mod_spec = importlib.util.spec_from_file_location(f'lifeblood.coreplugins.{filebasename}',
        #                                                       os.path.join(core_plugins_path, filename))
        #     mod = importlib.util.module_from_spec(mod_spec)
        #     mod_spec.loader.exec_module(mod)
        #     for requred_attr in ('create_node_object',):
        #         if not hasattr(mod, requred_attr):
        #             print(f'error loading plugin "{filebasename}". '
        #                   f'required method {requred_attr} is missing.')
        #             continue
        #     self.__plugins[filebasename] = mod
        # print('loaded plugins:\n', '\n\t'.join(self.__plugins.keys()))
        config = get_config('scheduler')

        self.__ping_interval = 1
        self.__dormant_mode_ping_interval_multiplier = 15
        self.__processing_interval = 5  # we don't need interval too small as now things may kick processor out of sleep as needed
        self.__dormant_mode_processing_interval_multiplier = 5
        self.__db_lock_timeout = 30

        # this lock will prevent tasks from being reported cancelled and done at the same exact time should that ever happen
        # this lock is overkill already, but we can make it even more overkill by using set of locks for each invoc id
        # which would be completely useless now cuz sqlite locks DB as a whole, not even a single table, especially not just parts of table
        self.__invocation_reporting_lock = asyncio.Lock()

        self.__ping_interval_mult = 1
        self.__processing_interval_mult = 1
        self.__invocation_attempts = config.get_option_noasync('invocation.default_attempts', 3)  # TODO: config should be directly used when needed to allow dynamically reconfigure running scheduler

        self.__mode = SchedulerMode.STANDARD

        self.__all_components = None
        self.__started_event = asyncio.Event()
        
        loop = asyncio.get_event_loop()

        if db_file_path is None:
            config = get_config('scheduler')
            db_file_path = config.get_option_noasync('core.database.path', str(paths.default_main_database_location()))
        if not db_file_path.startswith('file:'):  # if schema is used - we do not modify the db uri in any way
            db_file_path = os.path.realpath(os.path.expanduser(db_file_path))

        self.__logger.debug(f'starting scheduler with database: {db_file_path}')
        #
        # ensure database is initialized
        with sqlite3.connect(db_file_path) as con:
            con.executescript(sql_init_script)
        with sqlite3.connect(db_file_path) as con:
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
            self.__db_uid = struct.unpack('>Q', struct.pack('>q', metadata['unique_db_id']))[0]  # reinterpret signed as unsigned

        self.db_path = db_file_path
        ##

        self.__use_external_log = config.get_option_noasync('core.database.store_logs_externally', False)
        self.__external_log_location: Optional[Path] = config.get_option_noasync('core.database.store_logs_externally_location', None)
        if self.__use_external_log and not self.__external_log_location:
            raise SchedulerConfigurationError('if store_logs_externally is set - store_logs_externally_location must be set too')
        if self.__use_external_log:
            external_log_path = Path(self.__use_external_log)
            if external_log_path.exists() and external_log_path.is_file():
                external_log_path.unlink()
            if not external_log_path.exists():
                external_log_path.mkdir(parents=True)
            if not os.access(self.__external_log_location, os.X_OK | os.W_OK):
                raise RuntimeError('cannot write to external log location provided')

        self.__db_cache = {'workers_resources': {},
                           'workers_state': {},
                           'invocations': {}}
        self.__ui_cache = {'groups': {}, 'last_update_time': None}

        if server_addr is None:
            server_ip = config.get_option_noasync('core.server_ip', get_default_addr())
            server_port = config.get_option_noasync('core.server_port', default_scheduler_port())
        else:
            server_ip, server_port = server_addr
        if server_ui_addr is None:
            ui_ip = config.get_option_noasync('core.ui_ip', get_default_addr())
            ui_port = config.get_option_noasync('core.ui_port', default_ui_port())
        else:
            ui_ip, ui_port = server_ui_addr
        self.__stop_event = asyncio.Event()
        self.__server_closing_task = None
        self.__wakeup_event = asyncio.Event()
        self.__task_processor_kick_event = asyncio.Event()
        self.__wakeup_event.set()
        self.__server = None
        self.__server_coro = loop.create_server(self._scheduler_protocol_factory, server_ip, server_port, backlog=16)
        self.__server_address = ':'.join((server_ip, str(server_port)))
        self.__ui_server = None
        self.__ui_server_coro = loop.create_server(self._ui_protocol_factory, ui_ip, ui_port, backlog=16)
        self.__ui_address = ':'.join((ui_ip, str(ui_port)))
        if do_broadcasting is None:
            do_broadcasting = config.get_option_noasync('core.broadcast', True)
        if do_broadcasting:
            broadcast_info = json.dumps({'worker': self.__server_address, 'ui': self.__ui_address})
            self.__broadcasting_server = None
            self.__broadcasting_server_coro = create_broadcaster('lifeblood_scheduler', broadcast_info, ip=get_default_broadcast_addr())
        else:
            self.__broadcasting_server = None
            self.__broadcasting_server_coro = None

        self.__worker_pool = WorkerPool(WorkerType.SCHEDULER_HELPER, minimal_idle_to_ensure=helpers_minimal_idle_to_ensure, scheduler_address=(server_ip, server_port))

        self.__event_loop = asyncio.get_running_loop()
        assert self.__event_loop is not None, 'Scheduler MUST be created within working event loop, in the main thread'

    def get_event_loop(self):
        return self.__event_loop

    def _scheduler_protocol_factory(self):
        return SchedulerTaskProtocol(self)

    def _ui_protocol_factory(self):
        return SchedulerUiProtocol(self)

    def db_uid(self) -> int:
        """
        unique id that was generated on creation for the DB currently in use

        :return: 64 bit unsigned int
        """
        return self.__db_uid

    def wake(self):
        """
        scheduler may go into DORMANT mode when he things there's nothing to do
        in that case wake() call exits DORMANT mode immediately
        if wake is not called on some change- eventually scheduler will check it's shit and will decide to exit DORMANT mode on it's own, it will just waste some time first
        if currently not in DORMANT mode - nothing will happen

        :return:
        """
        if self.__mode == SchedulerMode.DORMANT:
            self.__logger.info('exiting DORMANT mode. mode is STANDARD now')
            self.__mode = SchedulerMode.STANDARD
            self.__processing_interval_mult = 1
            self.__ping_interval_mult = 1
            self.__wakeup_event.set()

    def poke_task_processor(self):
        """
        kick that lazy ass to stop it's waitings and immediately perform another processing iteration
        this is not connected to wake, __sleep and DORMANT mode,
        this is just one-time kick
        good to perform when task was changed somewhere async, outside of task_processor

        :return:
        """
        self.__task_processor_kick_event.set()

    def __sleep(self):
        if self.__mode == SchedulerMode.STANDARD:
            self.__logger.info('entering DORMANT mode')
            self.__mode = SchedulerMode.DORMANT
            self.__processing_interval_mult = self.__dormant_mode_processing_interval_multiplier
            self.__ping_interval_mult = self.__dormant_mode_ping_interval_multiplier
            self.__wakeup_event.clear()

    def mode(self) -> SchedulerMode:
        return self.__mode

    async def get_node_type_and_name_by_id(self, node_id: int) -> (str, str):
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT "type", "name" FROM "nodes" WHERE "id" = ?', (node_id,)) as nodecur:
                node_row = await nodecur.fetchone()
        if node_row is None:
            raise RuntimeError(f'node with given id {node_id} does not exist')
        return node_row['type'], node_row['name']

    @asynccontextmanager
    async def node_object_by_id_for_reading(self, node_id: int):
        async with self.get_node_lock_by_id(node_id).reader_lock:
            yield await self._get_node_object_by_id(node_id)

    @asynccontextmanager
    async def node_object_by_id_for_writing(self, node_id: int):
        async with self.get_node_lock_by_id(node_id).writer_lock:
            yield await self._get_node_object_by_id(node_id)

    async def _get_node_object_by_id(self, node_id: int) -> BaseNode:
        """
        When accessing node this way - be aware that you SHOULD ensure your access happens within a lock
        returned by get_node_lock_by_id.
        If you don't want to deal with that - use scheduler's wrappers to access nodes in a safe way
        (lol, wrappers are not implemented)

        :param node_id:
        :return:
        """
        if node_id in self.__node_objects:
            return self.__node_objects[node_id]
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT * FROM "nodes" WHERE "id" = ?', (node_id,)) as nodecur:
                node_row = await nodecur.fetchone()
            if node_row is None:
                raise RuntimeError('node id is invalid')

            node_type = node_row['type']
            if node_type not in pluginloader.plugins:
                raise RuntimeError('node type is unsupported')

            if node_row['node_object'] is not None:
                self.__node_objects[node_id] = await BaseNode.deserialize_async(node_row['node_object'], self, node_id)
                return self.__node_objects[node_id]

            #newnode: BaseNode = pluginloader.plugins[node_type].create_node_object(node_row['name'], self)
            newnode: BaseNode = pluginloader.create_node(node_type, node_row['name'], self, node_id)
            self.__node_objects[node_id] = newnode
            await con.execute('UPDATE "nodes" SET node_object = ? WHERE "id" = ?',
                              (await newnode.serialize_async(), node_id))
            await con.commit()

            return newnode

    def get_node_lock_by_id(self, node_id: int) -> RWLock:
        """
        All read/write operations for a node should be locked within a per node rw lock that scheduler maintains.
        Usually you do NOT have to be concerned with this.
        But in cases you get the node object with functions like get_node_object_by_id.
        it is your responsibility to ensure data is locked when accessed.
        Lock is not part of the node itself.

        :param node_id: node id to get lock to
        :return: rw lock for the node
        """
        if node_id not in self.__node_objects_locks:
            self.__node_objects_locks[node_id] = RWLock(fast=True)  # read about fast on github. the points is if we have awaits inside critical section - it's safe to use fast
        return self.__node_objects_locks[node_id]

    async def get_task_attributes(self, task_id: int) -> Tuple[Dict[str, Any], Optional[EnvironmentResolverArguments]]:
        """
        get tasks, atributes and it's enviroment resolver's attributes

        :param task_id:
        :return:
        """
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT attributes, environment_resolver_data FROM tasks WHERE "id" = ?', (task_id,)) as cur:
                res = await cur.fetchone()
            if res is None:
                raise RuntimeError('task with specified id was not found')
            env_res_args = None
            if res['environment_resolver_data'] is not None:
                env_res_args = await EnvironmentResolverArguments.deserialize_async(res['environment_resolver_data'])
            return await asyncio.get_event_loop().run_in_executor(None, json.loads, res['attributes']), env_res_args

    async def get_task_invocation_serialized(self, task_id: int) -> Optional[bytes]:
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT work_data FROM tasks WHERE "id" = ?', (task_id,)) as cur:
                res = await cur.fetchone()
            if res is None:
                raise RuntimeError('task with specified id was not found')
            return res[0]

    async def get_task_invocation(self, task_id: int):
        data = await self.get_task_invocation_serialized(task_id)
        if data is None:
            return None
        return await InvocationJob.deserialize_async(data)

    def stop(self):
        async def _server_closer():
            await self.__worker_pool.wait_till_stops()
            if self.__server is not None:
                self.__server.close()
                await self.__server.wait_closed()

        if self.__stop_event.is_set():
            self.__logger.error('cannot double stop!')
            return  # no double stopping
        if not self.__started_event.is_set():
            self.__logger.error('cannot stop what is not started!')
            return
        self.__logger.info('STOPPING SCHEDULER')
        self.__stop_event.set()  # this will stop things including task_processor
        self.__worker_pool.stop()
        self.__server_closing_task = asyncio.create_task(_server_closer())  # we ensure worker pool stops BEFORE server, so workers have chance to report back
        if self.__ui_server is not None:
            self.__ui_server.close()

    def _stop_event_wait(self):  # TODO: this is currently being used by ui proto to stop long connections, but not used in task proto, but what if it'll also get long living connections?
        return self.__stop_event.wait()

    async def start(self):
        # prepare
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            # we play it the safest for now:
            # all workers set to UNKNOWN state, all active invocations are reset, all tasks in the middle of processing are reset to closest waiting state
            con.row_factory = aiosqlite.Row
            await con.execute('UPDATE "tasks" SET "state" = ? WHERE "state" IN (?, ?)',
                              (TaskState.READY.value, TaskState.IN_PROGRESS.value, TaskState.INVOKING.value))
            await con.execute('UPDATE "tasks" SET "state" = ? WHERE "state" = ?',
                              (TaskState.WAITING.value, TaskState.GENERATING.value))
            await con.execute('UPDATE "tasks" SET "state" = ? WHERE "state" = ?',
                              (TaskState.POST_WAITING.value, TaskState.POST_GENERATING.value))
            await con.execute('UPDATE "invocations" SET "state" = ? WHERE "state" = ?', (InvocationState.FINISHED.value, InvocationState.IN_PROGRESS.value))
            await con.execute('UPDATE workers SET "ping_state" = ?', (WorkerPingState.UNKNOWN.value,))
            await con.execute('UPDATE "workers" SET "state" = ?', (WorkerState.UNKNOWN.value,))
            await con.commit()

            # update volatile mem cache:
            async with con.execute('SELECT "id", last_seen, last_checked, ping_state FROM workers') as worcur:
                async for row in worcur:
                    self.__db_cache['workers_state'][row['id']] = {k: row[k] for k in dict(row)}

        # start
        await self.__worker_pool.start()
        self.__server = await self.__server_coro
        self.__ui_server = await self.__ui_server_coro
        if self.__broadcasting_server_coro is not None:
            self.__broadcasting_server = await self.__broadcasting_server_coro
        # run
        self.__all_components = \
              asyncio.gather(self.task_processor(),
                             self.worker_pinger(),
                             self.__server.wait_closed(),  # TODO: shit being waited here below is very unnecessary
                             self.__ui_server.wait_closed(),
                             self.__worker_pool.wait_till_stops())

        self.__started_event.set()

    async def wait_till_starts(self):
        return await self.__started_event.wait()

    async def wait_till_stops(self):
        await self.__started_event.wait()
        assert self.__all_components is not None
        await self.__all_components
        await self.__server_closing_task

    def is_started(self):
        return self.__started_event.is_set()

    #
    # helper functions
    #
    # async def set_worker_ping_state(self, wid: int, state: WorkerPingState, con: Optional[aiosqlite.Connection] = None, nocommit: bool = False) -> None:
    #     await con.execute("UPDATE tmpdb.tmp_workers_states SET ping_state= ? WHERE worker_id = ?", (state.value, wid))

    async def set_worker_state(self, wid: int, state: WorkerState, con: Optional[aiosqlite.Connection] = None, nocommit: bool = False) -> None:
        await self._set_value('workers', 'state', wid, state.value, con, nocommit)

    async def worker_id_from_address(self, addr: str) -> Optional[int]:
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            async with con.execute('SELECT "id" FROM workers WHERE last_address = ?', (addr,)) as cur:
                ret = await cur.fetchone()
        if ret is None:
            return None
        return ret[0]

    async def get_worker_state(self, wid: int, con: Optional[aiosqlite.Connection] = None) -> WorkerState:
        if con is None:
            async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
                async with con.execute('SELECT "state" FROM "workers" WHERE "id" = ?', (wid,)) as cur:
                    res = await cur.fetchone()
        else:
            async with con.execute('SELECT "state" FROM "workers" WHERE "id" = ?', (wid,)) as cur:
                res = await cur.fetchone()
        if res is None:
            raise ValueError(f'worker with given wid={wid} was not found')
        return WorkerState(res[0])

    # async def update_worker_lastseen(self, wid: int, con: Optional[aiosqlite.Connection] = None, nocommit: bool = False):
    #     await con.execute("UPDATE tmpdb.tmp_workers_states SET last_seen= ? WHERE worker_id = ?", (int(time.time()), wid))

    async def reset_invocations_for_worker(self, worker_id: int, con: Optional[aiosqlite.Connection] = None) -> bool:
        """

        :param worker_id:
        :param con:
        :return: need commit?
        """
        async def _inner_(con):
            async with con.execute('SELECT * FROM invocations WHERE "worker_id" = ? AND "state" == ?',
                                   (worker_id, InvocationState.IN_PROGRESS.value)) as incur:
                all_invoc_rows = await incur.fetchall()  # we don't really want to update db while reading it
            need_commit = False
            for invoc_row in all_invoc_rows:  # mark all (probably single one) invocations
                need_commit = True
                self.__logger.debug("fixing dangling invocation %d" % (invoc_row['id'],))
                await con.execute('UPDATE invocations SET "state" = ? WHERE "id" = ?',
                                  (InvocationState.FINISHED.value, invoc_row['id']))
                await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                  (TaskState.READY.value, invoc_row['task_id']))
            return need_commit
        if con is None:
            async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
                if await _inner_(con):
                    await con.commit()
                return False
        else:
            return await _inner_(con)

    async def _set_value(self, table: str, field: str, wid: int, value: Any, con: Optional[aiosqlite.Connection] = None, nocommit: bool = False) -> None:
        if con is None:
            # TODO: safe check table and field, allow only text
            # TODO: handle db errors
            async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
                await con.execute("UPDATE %s SET %s= ? WHERE id = ?" % (table, field), (value, wid))
                if not nocommit:
                    await con.commit()
        else:
            await con.execute("UPDATE %s SET %s = ? WHERE id = ?" % (table, field), (value, wid))
            if not nocommit:
                await con.commit()

    async def _iter_iter_func(self, worker_row):
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row

            async def _check_lastseen_and_drop_invocations(switch_state_on_reset: Optional[WorkerState] = None) -> bool:
                if worker_row['last_seen'] is not None and time.time() - worker_row['last_seen'] < 64:  # TODO: make this time a configurable parameter
                    return False
                if switch_state_on_reset is not None:
                    await self.set_worker_state(worker_row['id'], switch_state_on_reset, con, nocommit=True)
                need_commit = await self.__update_worker_resouce_usage(worker_row['id'], hwid=worker_row['hwid'], connection=con)  # remove resource usage info
                need_commit = (await self.reset_invocations_for_worker(worker_row['id'], con)) or need_commit
                return need_commit or switch_state_on_reset is not None

            self.__pinger_logger.debug('    :: pinger started')
            self.__db_cache['workers_state'][worker_row['id']]['ping_state'] = WorkerPingState.CHECKING.value
            # await self.set_worker_ping_state(worker_row['id'], WorkerPingState.CHECKING, con, nocommit=True)
            self.__db_cache['workers_state'][worker_row['id']]['last_checked'] = int(time.time())
            # await con.execute("UPDATE tmpdb.tmp_workers_states SET last_checked = ? WHERE worker_id = ?", (int(time.time()), worker_row['id']))

            addr = worker_row['last_address']
            ip, port = addr.split(':')  # type: str, str
            self.__pinger_logger.debug('    :: checking %s, %s', ip, port)

            if not port.isdigit():
                self.__pinger_logger.debug('    :: malformed address')
                self.__db_cache['workers_state'][worker_row['id']]['ping_state'] = WorkerPingState.ERROR.value
                await self.set_worker_state(worker_row['id'], WorkerState.ERROR, con, nocommit=True)
                # await asyncio.gather(
                #     self.set_worker_ping_state(worker_row['id'], WorkerPingState.ERROR, con, nocommit=True),
                #     self.set_worker_state(worker_row['id'], WorkerState.ERROR, con, nocommit=True)
                # )
                await _check_lastseen_and_drop_invocations()
                await con.commit()
                return
            try:
                async with WorkerTaskClient(ip, int(port), timeout=15) as client:
                    ping_code, pvalue = await client.ping()
            except asyncio.exceptions.TimeoutError:
                self.__pinger_logger.info(f'    :: network timeout {ip}:{port}')
                self.__db_cache['workers_state'][worker_row['id']]['ping_state'] = WorkerPingState.ERROR.value
                # await self.set_worker_ping_state(worker_row['id'], WorkerPingState.ERROR, con, nocommit=True)
                if await _check_lastseen_and_drop_invocations(switch_state_on_reset=WorkerState.ERROR):  # TODO: maybe give it a couple of tries before declaring a failure?
                    await con.commit()
                return
            except ConnectionRefusedError as e:
                self.__pinger_logger.info(f'    :: host down {ip}:{port} {e}')
                self.__db_cache['workers_state'][worker_row['id']]['ping_state'] = WorkerPingState.OFF.value
                # await self.set_worker_ping_state(worker_row['id'], WorkerPingState.OFF, con, nocommit=True)
                if await _check_lastseen_and_drop_invocations(switch_state_on_reset=WorkerState.OFF):
                    await con.commit()
                return
            except Exception as e:
                self.__pinger_logger.info(f'    :: ping failed {ip}:{port} {type(e)}, {e}')
                self.__db_cache['workers_state'][worker_row['id']]['ping_state'] = WorkerPingState.ERROR.value
                # await self.set_worker_ping_state(worker_row['id'], WorkerPingState.ERROR, con, nocommit=True)
                if await _check_lastseen_and_drop_invocations(switch_state_on_reset=WorkerState.OFF):
                    await con.commit()
                return

            # at this point we sure to have received a reply
            # fixing possibly inconsistent worker states
            # this inconsistencies should only occur shortly after scheduler restart
            # due to desync of still working workers and scheduler
            workerstate = await self.get_worker_state(worker_row['id'], con=con)
            if workerstate == WorkerState.OFF:
                # there can be race conditions (theoretically) if worker saz goodbye right after getting the ping, so we get OFF state from db. or all vice-versa
                # so there is nothing but warnings here. inconsistencies should be reliably resolved by worker
                if ping_code == WorkerPingReply.IDLE:
                    self.__logger.warning(f'worker {worker_row["id"]} is marked off, but pinged as IDLE... have scheduler been restarted recently? waiting for worker to ping me and resolve this inconsistency...')
                    # await self.set_worker_state(worker_row['id'], WorkerState.IDLE, con=con, nocommit=True)
                elif ping_code == WorkerPingReply.BUSY:
                    self.__logger.warning(f'worker {worker_row["id"]} is marked off, but pinged as BUSY... have scheduler been restarted recently? waiting for worker to ping me and resolve this inconsistency...')
                    # await self.set_worker_state(worker_row['id'], WorkerState.BUSY, con=con, nocommit=True)

            if ping_code == WorkerPingReply.IDLE:  # TODO, just like above - add warnings, but leave solving to worker
                pass
                #workerstate = WorkerState.IDLE
                # TODO: commented below as it seem to cause race conditions with worker invocation done reporting. NEED CHECKING
                #if await self.reset_invocations_for_worker(worker_row['id'], con=con):
                #    await con.commit()
            elif ping_code == WorkerPingReply.BUSY:
                #workerstate = WorkerState.BUSY  # in this case received pvalue is current task's progress. u cannot rely on it's precision: some invocations may not support progress reporting
                # TODO: think, can there be race condition here so that worker is already doing something else?
                async with con.execute('SELECT "id" FROM invocations WHERE "state" = ? AND "worker_id" = ?', (InvocationState.IN_PROGRESS.value, worker_row['id'])) as invcur:
                    inv_id = await invcur.fetchone()
                    if inv_id is not None:
                        inv_id = inv_id['id']
                if inv_id is not None:
                    if inv_id not in self.__db_cache['invocations']:
                        self.__db_cache['invocations'][inv_id] = {}
                    self.__db_cache['invocations'][inv_id].update({'progress': pvalue})  # Note: this in theory AND IN PRACTICE causes racing with del on task finished/cancelled.
                    # Therefore additional cleanup needed later - still better than lock things or check too hard

                # await con.execute('UPDATE "invocations" SET "progress" = ? WHERE "state" = ? AND "worker_id" = ?', (pvalue, InvocationState.IN_PROGRESS.value, worker_row['id']))
            else:
                raise NotImplementedError(f'not a known ping_code {ping_code}')

            self.__db_cache['workers_state'][worker_row['id']]['ping_state'] = WorkerPingState.WORKING.value
            self.__db_cache['workers_state'][worker_row['id']]['last_seen'] = int(time.time())
            if worker_row['state'] == WorkerState.ERROR.value:  # so we thought worker had an network error, but now it's all fine
                await self.set_worker_state(worker_row['id'], workerstate)
            # await asyncio.gather(self.set_worker_ping_state(worker_row['id'], WorkerPingState.WORKING, con, nocommit=True),
            #                      #self.set_worker_state(worker_row['id'], workerstate, con, nocommit=True),
            #                      self.update_worker_lastseen(worker_row['id'], con, nocommit=True)
            #                      )
            # await con.commit()
            self.__pinger_logger.debug('    :: %s', ping_code)

    async def split_task(self, task_id: int, into: int, con: aiosqlite.Connection) -> List[int]:
        """
        con is expected to be a opened db connection with dict factory
        :param into:
        :param con:
        :return:
        """
        if into < 1:
            raise ValueError('cant split into less than 1 part')

        if not con.in_transaction:
            await con.execute('BEGIN IMMEDIATE')
        # even first selects are only safe to do within a transaction

        async with con.execute('SELECT * FROM tasks WHERE "id" = ?', (task_id,)) as cur:
            task_row = await cur.fetchone()
        new_split_level = task_row['split_level'] + 1

        async with con.execute('SELECT MAX("split_id") as m FROM "task_splits"') as maxsplitcur:
            next_split_id = 1 + ((await maxsplitcur.fetchone())['m'] or 0)
        await con.execute('UPDATE tasks SET state = ? WHERE "id" = ?',
                          (TaskState.SPLITTED.value, task_id))
        # await con.execute('INSERT INTO "task_splits" ("split_id", "task_id", "split_element", "split_count", "origin_task_id") VALUES (?,?,?,?,?)',
        #                   (next_split_id, task_row['id'], 0, into, task_id))
        all_split_ids = []
        for split_element in range(into):
            async with con.execute('INSERT INTO tasks (parent_id, "state", "node_id", '
                                   '"node_input_name", "node_output_name", '
                                   '"work_data", "environment_resolver_data", "name", "attributes", "split_level") '
                                   'VALUES (?,?,?,?,?,?,?,?,?,?)',
                                   (None, task_row['state'], task_row['node_id'],
                                    task_row['node_input_name'], task_row['node_output_name'],
                                    task_row['work_data'], task_row['environment_resolver_data'], task_row['name'], task_row['attributes'], new_split_level)) \
                    as insert_cur:
                new_task_id = insert_cur.lastrowid

            # copy groups  # TODO:SQL OPTIMIZE
            async with con.execute('SELECT "group" FROM task_groups WHERE "task_id" = ?', (task_id,)) as gcur:
                groups = [x['group'] for x in await gcur.fetchall()]
            if len(groups) > 0:
                await con.executemany('INSERT INTO task_groups ("task_id", "group") VALUES (?, ?)',
                                      zip(itertools.repeat(new_task_id, len(groups)), groups))

            all_split_ids.append(new_task_id)
            await con.execute('INSERT INTO "task_splits" ("split_id", "task_id", "split_element", "split_count", "origin_task_id") VALUES (?,?,?,?,?)',
                              (next_split_id, new_task_id, split_element, into, task_id))
        # now increase number of children to the parent of the task being splitted

        assert into == len(all_split_ids)
        self.wake()
        return all_split_ids

    #
    # pinger "thread"
    async def worker_pinger(self):
        """
        one of main constantly running coroutines
        responsible for pinging all the workers once in a while in separate tasks each
        TODO: test how well this approach works for 1000+ workers
        :return: NEVER !!
        """

        tasks = []
        stop_task = asyncio.create_task(self.__stop_event.wait())
        wakeup_task = None
        while not self.__stop_event.is_set():
            nowtime = time.time()

            self.__pinger_logger.debug('    ::selecting workers...')
            async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
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
                    row[cached_field] = self.__db_cache['workers_state'][row['id']][cached_field]
                if row['ping_state'] == WorkerPingState.CHECKING.value:  # TODO: this check could happen in the very beginning of this loop... too sleepy now to blindly move it
                    continue

                time_delta = nowtime - (row['last_checked'] or 0)
                if row['state'] == WorkerState.BUSY.value:
                    tasks.append(asyncio.create_task(self._iter_iter_func(row)))
                elif row['state'] == WorkerState.IDLE.value and time_delta > 4 * self.__ping_interval * self.__ping_interval_mult:
                    tasks.append(asyncio.create_task(self._iter_iter_func(row)))
                else:  # worker state is error or off
                    if time_delta > 15 * self.__ping_interval * self.__ping_interval_mult:
                        tasks.append(asyncio.create_task(self._iter_iter_func(row)))

            # now clean the list
            tasks = [x for x in tasks if not x.done()]
            self.__pinger_logger.debug('    :: remaining ping tasks: %d', len(tasks))

            # now wait
            if wakeup_task is not None:
                sleeping_tasks = (stop_task, wakeup_task)
            else:
                if self.__mode == SchedulerMode.DORMANT:
                    wakeup_task = asyncio.create_task(self.__wakeup_event.wait())
                    sleeping_tasks = (stop_task, wakeup_task)
                else:
                    sleeping_tasks = (stop_task,)

            done, _ = await asyncio.wait(sleeping_tasks, timeout=self.__ping_interval * self.__ping_interval_mult, return_when=asyncio.FIRST_COMPLETED)
            if wakeup_task is not None and wakeup_task in done:
                wakeup_task = None
            if stop_task in done:
                break

        # FINALIZING PINGER
        self.__logger.info('finishing worker pinger...')
        if len(tasks) > 0:
            self.__logger.debug(f'waiting for {len(tasks)} pinger tasks...')
            _, pending = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED, timeout=5)
            self.__logger.debug(f'waiting enough, cancelling {len(pending)} tasks')
            for task in pending:
                task.cancel()
        self.__logger.info('pinger syncing temporary tables back...')
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            for wid, cached_row in self.__db_cache['workers_state'].items():
                await con.execute('UPDATE workers SET '
                                  'last_seen=?, '
                                  'last_checked=?, '
                                  'ping_state=? '
                                  'WHERE "id"=?',
                                  (cached_row['last_seen'], cached_row['last_checked'], cached_row['ping_state'], wid))
            await con.commit()
        self.__logger.info('worker pinger finished')

    #
    # task processing thread
    async def task_processor(self):

        awaiter_lock = asyncio.Lock()
        # task processing coroutimes
        @atimeit()
        async def _awaiter(processor_to_run, task_row, abort_state: TaskState, skip_state: TaskState):  # TODO: process task generation errors
            #_blo = time.perf_counter()
            task_id = task_row['id']
            loop = asyncio.get_event_loop()
            try:
                async with self.get_node_lock_by_id(task_row['node_id']).reader_lock:
                    process_result: ProcessingResult = await loop.run_in_executor(awaiter_executor, processor_to_run, task_row)  # TODO: this should have task and node attributes!
            except NodeNotReadyToProcess:
                async with awaiter_lock, SharedLazyAiosqliteConnection(None, self.db_path, 'awaiter_con', timeout=self.__db_lock_timeout) as con:
                                        #aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
                    await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                      (abort_state.value, task_id))
                    await con.commit(self.poke_task_processor)
                return
            except Exception as e:
                async with awaiter_lock, SharedLazyAiosqliteConnection(None, self.db_path, 'awaiter_con', timeout=self.__db_lock_timeout) as con:
                                        #aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
                    await con.execute('UPDATE tasks SET "state" = ?, "state_details" = ? WHERE "id" = ?',
                                      (TaskState.ERROR.value,
                                       json.dumps({'message': traceback.format_exc(),
                                                   'happened_at': task_row['state'],
                                                   'type': 'exception',
                                                   'exception_str': str(e),
                                                   'exception_type': str(type(e))})
                                       , task_id))
                    await con.commit(self.poke_task_processor)
                    self.__logger.exception('error happened %s %s', type(e), e)
                return

            # why is there lock? it looks locking manually is waaaay more efficient than relying on transaction locking
            async with awaiter_lock, SharedLazyAiosqliteConnection(None, self.db_path, 'awaiter_con', timeout=self.__db_lock_timeout) as con:
                                    #aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
                #con.row_factory = aiosqlite.Row
                # This implicitly starts transaction
                #print(f'up till block: {time.perf_counter() - _blo}')

                if not con.in_transaction:
                    await con.execute('BEGIN IMMEDIATE')
                    assert con.in_transaction
                if process_result.output_name:
                    await con.execute('UPDATE tasks SET "node_output_name" = ? WHERE "id" = ?',
                                      (process_result.output_name, task_id))
                #_blo = time.perf_counter()
                #_bla1 = time.perf_counter()

                # note: this may be not obvious, but ALL branches of the next if result in implicit transaction start
                if process_result.do_kill_task:
                    await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                      (TaskState.DEAD.value, task_id))
                else:
                    if process_result.invocation_job is None:  # if no job to do
                        await con.execute('UPDATE tasks SET "work_data" = ?, "work_data_invocation_attempt" = 0, "state" = ?, "_invoc_requirement_clause" = ? '
                                          'WHERE "id" = ?',
                                          (None, skip_state.value, None,
                                           task_id))
                    else:
                        # if there is an invocation - we force environment wrapper arguments from task onto it
                        if task_row['environment_resolver_data'] is not None:
                            process_result.invocation_job._set_envresolver_arguments(await EnvironmentResolverArguments.deserialize_async(task_row['environment_resolver_data']))

                        taskdada_serialized = await process_result.invocation_job.serialize_async()
                        invoc_requirements_sql = process_result.invocation_job.requirements().final_where_clause()
                        invoc_requirements_dict_str = json.dumps(process_result.invocation_job.requirements().to_dict(resources_only=True))
                        job_priority = process_result.invocation_job.priority()
                        async with con.execute('SELECT MAX(task_group_attributes.priority) AS priority FROM task_group_attributes '
                                               'INNER JOIN task_groups ON task_group_attributes."group"==task_groups."group" '
                                               'WHERE task_groups.task_id==? AND task_group_attributes.state==?', (task_id, TaskGroupArchivedState.NOT_ARCHIVED.value)) as cur:
                            group_priority = await cur.fetchone()
                            if group_priority is None:
                                group_priority = 50.0  # "or" should only work in case there were no unarchived groups at all for the task
                            else:
                                group_priority = group_priority[0]
                        await con.execute('UPDATE tasks SET "work_data" = ?, "work_data_invocation_attempt" = 0, "state" = ?, "_invoc_requirement_clause" = ?, '
                                          'priority = ? '
                                          'WHERE "id" = ?',
                                          (taskdada_serialized, TaskState.READY.value, ':::'.join((invoc_requirements_sql, invoc_requirements_dict_str)),
                                           group_priority + job_priority,
                                           task_id))
                #print(f'kill/invoc: {time.perf_counter() - _bla1}')
                #_bla1 = time.perf_counter()
                if process_result.do_split_remove:
                    async with con.execute('SELECT split_sealed FROM task_splits WHERE split_id = ?', (task_row['split_id'],)) as sealcur:
                        res = await sealcur.fetchone()
                    if res is not None and res['split_sealed'] == 0:  # sealing split does actually exist and not sealed
                        # async with con.execute('SELECT task_id FROM task_splits WHERE split_id = ?', (task_row['split_id'])) as tcur:
                        #     task_ids_to_update = [x['task_id'] for x in await tcur.fetchall()]
                        # await con.executemany('UPDATE tasks set "state" = ? WHERE "id" = ?', ((TaskState.DEAD.value, x) for x in task_ids_to_update))
                        await con.execute('UPDATE task_splits SET "split_sealed" = 1 '
                                          'WHERE  "split_id" = ?',
                                          (task_row['split_id'],))
                        # teleport original task to us
                        await con.execute('UPDATE tasks SET "node_id" = ?, "state" = ? WHERE "id" = ?',
                                          (task_row['node_id'], TaskState.DONE.value, task_row['split_origin_task_id']))
                        if process_result.output_name:
                            await con.execute('UPDATE tasks SET "node_output_name" = ? WHERE "id" = ?',
                                              (process_result.output_name, task_row['split_origin_task_id']))
                                          # so sealed split task will get the same output_name as the task that is sealing the split
                        # and update it's attributes if provided
                        if len(process_result.split_attributes_to_set) > 0:
                            async with con.execute('SELECT attributes FROM tasks WHERE "id" = ?', (task_row['split_origin_task_id'],)) as attcur:
                                attributes = await asyncio.get_event_loop().run_in_executor(None, json.loads, (await attcur.fetchone())['attributes'])
                                attributes.update(process_result.split_attributes_to_set)
                                for k, v in process_result.split_attributes_to_set.items():
                                    if v is None:
                                        del attributes[k]
                                result_serialized = await asyncio.get_event_loop().run_in_executor(None, json.dumps, attributes)
                                await con.execute('UPDATE tasks SET "attributes" = ? WHERE "id" = ?',
                                                  (result_serialized, task_row['split_origin_task_id']))

                #print(f'splitrem: {time.perf_counter() - _bla1}')
                #_bla1 = time.perf_counter()
                if process_result.attributes_to_set:  # not None or {}
                    attributes = await asyncio.get_event_loop().run_in_executor(None, json.loads, task_row['attributes'])
                    attributes.update(process_result.attributes_to_set)
                    for k, v in process_result.attributes_to_set.items():
                        if v is None:
                            del attributes[k]
                    result_serialized = await asyncio.get_event_loop().run_in_executor(None, json.dumps, attributes)
                    await con.execute('UPDATE tasks SET "attributes" = ? WHERE "id" = ?',
                                      (result_serialized, task_id))

                # process environment resolver arguments if provided
                if (envargs := process_result._environment_resolver_arguments) is not None:
                    await con.execute('UPDATE tasks SET environment_resolver_data = ? WHERE "id" = ?',
                                      (await envargs.serialize_async(), task_id))

                #print(f'attset: {time.perf_counter() - _bla1}')
                #_bla1 = time.perf_counter()
                # spawning new tasks after all attributes were set, so children inherit
                if process_result.spawn_list is not None:
                    await self.spawn_tasks(process_result.spawn_list, con=con)

                #print(f'spawn: {time.perf_counter() - _bla1}')
                #_bla1 = time.perf_counter()
                if process_result._split_attribs is not None:
                    split_count = len(process_result._split_attribs)
                    for attr_dict, split_task_id in zip(process_result._split_attribs, await self.split_task(task_id, split_count, con)):
                        async with con.execute('SELECT attributes FROM "tasks" WHERE "id" = ?', (split_task_id,)) as cur:
                            split_task_dict = await cur.fetchone()
                        assert split_task_dict is not None
                        split_task_attrs = json.loads(split_task_dict['attributes'])  # TODO: run in executor
                        split_task_attrs.update(attr_dict)
                        await con.execute('UPDATE "tasks" SET attributes = ? WHERE "id" = ?', (json.dumps(split_task_attrs), split_task_id))  # TODO: run dumps in executor
                #print(f'split: {time.perf_counter()-_bla1}')

                #_precum = time.perf_counter()-_blo
                await con.commit(self.poke_task_processor)
                #print(f'_awaiter trans: {_precum} - {time.perf_counter()-_blo}')

        # submitter
        @atimeit()
        async def _submitter(task_row, worker_row):
            self.__logger.debug(f'submitter started')
            addr = worker_row['last_address']
            try:
                ip, port = addr.split(':')
                port = int(port)
            except:
                self.__logger.error('error addres converting during unexpected here. ping should have cought it')
                ip, port = None, None  # set to invalid values to exit in error-checking if a bit below

            work_data = task_row['work_data']
            assert work_data is not None
            task: InvocationJob = await asyncio.get_event_loop().run_in_executor(None, InvocationJob.deserialize, work_data)
            if not task.args() or ip is None:  #
                async with awaiter_lock, aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as skipwork_transaction:
                    await skipwork_transaction.execute('UPDATE tasks SET state = ? WHERE "id" = ?',
                                                       (TaskState.POST_WAITING.value, task_row['id']))
                    await skipwork_transaction.execute('UPDATE workers SET state = ? WHERE "id" = ?',
                                                       (WorkerState.IDLE.value, worker_row['id']))
                    # unset resource usage
                    await self.__update_worker_resouce_usage(worker_row['id'], hwid=worker_row['hwid'], connection=con)
                    await skipwork_transaction.commit()
                    return

            # so task.args() is not None
            async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as submit_transaction:
                submit_transaction.row_factory = aiosqlite.Row
                async with awaiter_lock:
                    async with submit_transaction.execute(
                            'INSERT INTO invocations ("task_id", "worker_id", "state", "node_id") VALUES (?, ?, ?, ?)',
                            (task_row['id'], worker_row['id'], InvocationState.INVOKING.value, task_row['node_id'])) as incur:
                        invocation_id = incur.lastrowid  # rowid should be an alias to id, acc to sqlite manual
                    await submit_transaction.commit()

                task._set_invocation_id(invocation_id)
                task._set_task_id(task_row['id'])
                task._set_task_attributes(json.loads(task_row['attributes']))  # TODO: run in executor
                self.__logger.debug(f'submitting task to {addr}')
                try:
                    # this is potentially a long operation - db must NOT be locked during it
                    async with WorkerTaskClient(ip, port) as client:
                        # import random
                        # await asyncio.sleep(random.uniform(0, 8))  # DEBUG! IMITATE HIGH LOAD
                        reply = await client.give_task(task, self.__server_address)
                    self.__logger.debug(f'got reply {reply}')
                except Exception as e:
                    self.__logger.error('some unexpected error %s %s' % (str(type(e)), str(e)))
                    reply = TaskScheduleStatus.FAILED

                async with awaiter_lock:
                    await submit_transaction.execute('BEGIN IMMEDIATE')
                    async with submit_transaction.execute('SELECT "state" FROM workers WHERE "id" == ?', (worker_row['id'],)) as incur:
                        worker_state = WorkerState((await incur.fetchone())[0])
                    # IF worker state is NOT invoking - then either worker_hello, or worker_bye happened between starting _submitter and here
                    if worker_state == WorkerState.OFF:
                        self.__logger.debug('submitter: worker state changed to OFF during submitter work')
                        if reply == TaskScheduleStatus.SUCCESS:
                            self.__logger.warning('submitter succeeded, yet worker state changed to OFF in the middle of submission. forcing reply to FAIL')
                            reply = TaskScheduleStatus.FAILED
                    assert worker_state != WorkerState.IDLE  # this should never happen as hello preserves INVOKING state
                    if reply == TaskScheduleStatus.SUCCESS:
                        await submit_transaction.execute('UPDATE tasks SET state = ?, '
                                                         '"work_data_invocation_attempt" = "work_data_invocation_attempt" + 1 '
                                                         'WHERE "id" = ?',
                                                         (TaskState.IN_PROGRESS.value, task_row['id']))
                        await submit_transaction.execute('UPDATE workers SET state = ? WHERE "id" = ?',
                                                         (WorkerState.BUSY.value, worker_row['id']))
                        await submit_transaction.execute('UPDATE invocations SET state = ? WHERE "id" = ?',
                                                         (InvocationState.IN_PROGRESS.value, invocation_id))
                    else:  # on anything but success - cancel transaction
                        self.__logger.debug(f'submitter failed, rolling back for wid {worker_row["id"]}')
                        await submit_transaction.execute('UPDATE tasks SET state = ? WHERE "id" = ?',
                                                         (TaskState.READY.value,
                                                          task_row['id']))
                        await submit_transaction.execute('UPDATE workers SET state = ? WHERE "id" = ?',
                                                         (WorkerState.IDLE.value if worker_state != WorkerState.OFF else WorkerState.OFF.value,
                                                          worker_row['id']))
                        await submit_transaction.execute('DELETE FROM invocations WHERE "id" = ?',
                                                         (invocation_id,))
                        # update resource usage to none
                        await self.__update_worker_resouce_usage(worker_row['id'], hwid=worker_row['hwid'], connection=submit_transaction)
                    await submit_transaction.commit()

        awaiter_executor = ThreadPoolExecutor()  # TODO: max_workers= set from config
        # this will hold references to tasks created with asyncio.create_task
        tasks_to_wait = set()
        stop_task = asyncio.create_task(self.__stop_event.wait())
        wakeup_task = None
        kick_wait_task = asyncio.create_task(self.__task_processor_kick_event.wait())
        gc_counter = 0
        # tm_counter = 0
        while not self.__stop_event.is_set():
            gc_counter += 1
            # tm_counter += 1
            if gc_counter >= 120:  # TODO: to config this timing
                gc_counter = 0
                self.__logger.debug('========')
                self.__logger.debug('================================================================')
                with threading._shutdown_locks_lock:
                    self.__logger.debug(f'loose threads: {len(threading._shutdown_locks)}')
                    threading._shutdown_locks.difference_update([lock for lock in threading._shutdown_locks if not lock.locked()])
                    self.__logger.debug(f'loose threads after cleanup: {len(threading._shutdown_locks)}')
                self.__logger.debug(f'total tasks: {len(asyncio.all_tasks())}')

                def _gszofdr(obj):
                    sz = sys.getsizeof(obj)
                    for k, v in obj.items():
                        sz += sys.getsizeof(k)
                        sz += sys.getsizeof(v)
                        if isinstance(v, dict):
                            sz += _gszofdr(v)
                    return sz

                # pruning db_cache
                async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
                    con.row_factory = aiosqlite.Row
                    async with con.execute('SELECT "id" FROM invocations WHERE state == ?',
                                           (InvocationState.IN_PROGRESS.value,)) as inv:
                        filtered_invocs = set(x['id'] for x in await inv.fetchall())
                for inv in tuple(self.__db_cache['invocations'].keys()):
                    if inv not in filtered_invocs:  # Note: since task finish/cancel reporting is in the same thread as this - there will not be race conditions for del, as there's no await
                        del self.__db_cache['invocations'][inv]
                filtered_invocs.clear()
                # prune done

                self.__logger.debug(f'size of temp db cache: {_gszofdr(self.__db_cache)}')
                self.__logger.debug('================================================================')
                self.__logger.debug('========')

                # self.__logger.debug(f'\n\n {mem_top(verbose_types=[set], limit=16)} \n\n')
                #  seems that memtop's gc calls cause some random exceptions on db's fetch all
                #  https://bugs.python.org/issue37788
                #  https://bugs.python.org/issue15108
                #  also https://gist.github.com/ulope/db811b6cf853ff267f27e4295bc4739e
                # import gc
                # objs = gc.get_objects()
                # objs = sorted(objs, key=lambda obj: len(gc.get_referents(obj)), reverse=True)
                # print(repr(gc.get_referrers(objs[0]))[:200])
                # print('\n')
                # print(repr(gc.get_referrers(objs[1]))[:200])
            # if tm_counter >= 10*60*2:
            #     tm_counter = 0
            #     snapshot = tracemalloc.take_snapshot()
            #     top_stats = snapshot.statistics('lineno')
            #     self.__logger.warning('\n\n[ Top 10 MEM USERS]\n{}\n\n'.format("\n".join(str(stat) for stat in top_stats[:10])))

            # first prune awaited tasks
            to_remove = set()
            for task_to_wait in tasks_to_wait:
                if task_to_wait.done():
                    to_remove.add(task_to_wait)
                    try:
                        await task_to_wait
                    except Exception as e:
                        self.__logger.exception('awaited task raised some problems')
            tasks_to_wait -= to_remove

            # now proceed with processing
            _debug_con = time.perf_counter()
            total_processed = 0
            total_state_changes = 0  # note that total_state_changes may be greater than total_processed, as total_processed refers to existing tasks only, but total_state_changes counts new splits as well
            async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
                con.row_factory = aiosqlite.Row

                for task_state in (TaskState.WAITING, TaskState.READY, TaskState.DONE, TaskState.POST_WAITING, TaskState.SPAWNED):
                    _debug_sel = time.perf_counter()
                    async with con.execute('SELECT tasks.id, tasks.parent_id, tasks.children_count, tasks.active_children_count, tasks.state, tasks.state_details, '
                                           'tasks.node_id, tasks.node_input_name, tasks.node_output_name, tasks.name, tasks.attributes, tasks.split_level, '
                                           'tasks.work_data, tasks.work_data_invocation_attempt, tasks._invoc_requirement_clause, tasks.environment_resolver_data, '
                                           'nodes.type as node_type, nodes.name as node_name, nodes.id as node_id, '
                                           'task_splits.split_id as split_id, task_splits.split_element as split_element, task_splits.split_count as split_count, task_splits.origin_task_id as split_origin_task_id '
                                           'FROM tasks INNER JOIN nodes ON tasks.node_id=nodes.id '
                                           'LEFT JOIN task_splits ON tasks.id=task_splits.task_id '
                                           'WHERE (state = ?) '
                                           'AND paused = 0 '
                                           'AND dead = 0 '
                                           'ORDER BY {} RANDOM()'.format('tasks.priority DESC, ' if task_state == TaskState.READY else ''),
                                           (task_state.value,)) as cur:
                        all_task_rows = await cur.fetchall()  # we dont want to iterate reading over changing rows - easy to deadlock yourself (as already happened)
                        # if too much tasks here - consider adding LIMIT to execute and work on portions only

                    _debug_pstart = time.perf_counter()
                    if _debug_pstart - _debug_sel > 0.05:  # we shouldn't even worry if it's less
                        self.__logger.debug(f'SELECT took {_debug_pstart - _debug_sel}')

                    if len(all_task_rows) == 0:
                        continue
                    total_processed += len(all_task_rows)

                    self.__logger.debug(f'total {task_state.name}: {len(all_task_rows)}')
                    # TODO: the problem might occur below when there are thousands of processing tasks - it may take some time before implicit transaction lock is given to task_processor
                    #
                    # waiting to be processed
                    if task_state == TaskState.WAITING:
                        awaiters = []
                        set_to_stuff = []
                        for task_row in all_task_rows:
                            if task_row['node_type'] not in pluginloader.plugins:
                                self.__logger.error(f'plugin to process "{task_row["node_type"]}" not found!')
                                # await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                #                   (TaskState.ERROR.value, task_row['id']))
                                set_to_stuff.append((TaskState.ERROR.value, task_row['id']))
                                total_state_changes += 1
                            else:
                                # note that ready_to_process_task is ran not inside the read lock
                                # as it's expected that:
                                #  - running the function is even faster than locking
                                #  - function misfire (being highly unlikely) does not have side effects, so will not cause any damage
                                if not (await self._get_node_object_by_id(task_row['node_id'])).ready_to_process_task(task_row):
                                    continue

                                # await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                #                   (TaskState.GENERATING.value, task_row['id']))
                                set_to_stuff.append((TaskState.GENERATING.value, task_row['id']))
                                total_state_changes += 1

                                awaiters.append(_awaiter((await self._get_node_object_by_id(task_row['node_id']))._process_task_wrapper, dict(task_row),
                                                         abort_state=TaskState.WAITING, skip_state=TaskState.POST_WAITING))
                        if set_to_stuff:
                            await con.executemany('UPDATE tasks SET "state" = ? WHERE "id" = ?', set_to_stuff)
                            await con.commit()
                        self.__logger.debug('loop done, creating tasks')
                        for coro in awaiters:
                            tasks_to_wait.add(asyncio.create_task(coro))
                    #
                    # waiting to be post processed
                    elif task_state == TaskState.POST_WAITING:
                        awaiters = []
                        set_to_stuff = []
                        for task_row in all_task_rows:
                            if task_row['node_type'] not in pluginloader.plugins:
                                self.__logger.error(f'plugin to process "{task_row["node_type"]}" not found!')
                                # await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                #                   (TaskState.ERROR.value, task_row['id']))
                                set_to_stuff.append((TaskState.ERROR.value, task_row['id']))
                                total_state_changes += 1
                            else:
                                if not (await self._get_node_object_by_id(task_row['node_id'])).ready_to_postprocess_task(task_row):
                                    continue

                                # await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                #                   (TaskState.POST_GENERATING.value, task_row['id']))
                                set_to_stuff.append((TaskState.POST_GENERATING.value, task_row['id']))
                                total_state_changes += 1

                                awaiters.append(_awaiter((await self._get_node_object_by_id(task_row['node_id']))._postprocess_task_wrapper, dict(task_row),
                                                         abort_state=TaskState.POST_WAITING, skip_state=TaskState.DONE))
                        if set_to_stuff:
                            await con.executemany('UPDATE tasks SET "state" = ? WHERE "id" = ?', set_to_stuff)
                            await con.commit()
                        for coro in awaiters:
                            tasks_to_wait.add(asyncio.create_task(coro))  # note - dont change to run in executors in threads - there are things here like asyncio locks that RELY ON BEING IN SAME THREAD
                    #
                    # real scheduling should happen here
                    elif task_state == TaskState.READY:
                        submitters = []
                        # there may be a lot of similar queries, and if there's nothing available at some point - we may just leave it for next submission iteration
                        # and anyway - if transaction has already started - there wont be any new idle worker, since sqlite block everything
                        where_empty_cache = set()
                        for task_row in all_task_rows:
                            # check max attempts first
                            if task_row['work_data_invocation_attempt'] >= self.__invocation_attempts:
                                await con.execute('UPDATE tasks SET "state" = ?, "state_details" = ? WHERE "id" = ?',
                                                  (TaskState.ERROR.value,
                                                   json.dumps({'message': 'maximum invocation attempts reached',
                                                               'happened_at': task_row['state'],
                                                               'type': 'limit',
                                                               'limit_threshold': self.__invocation_attempts,
                                                               'limit_value': task_row['work_data_invocation_attempt']}),
                                                   task_row['id']))
                                total_state_changes += 1
                                self.__logger.warning(f'{task_row["id"]} reached maximum invocation attempts, setting it to error state')
                                continue
                            #
                            requirements_clause_sql: str = task_row["_invoc_requirement_clause"]
                            requirements_clause_dict = None
                            if (splitpos := requirements_clause_sql.rfind(':::')) > -1:
                                requirements_clause_dict = json.loads(requirements_clause_sql[splitpos+3:])
                                requirements_clause_sql = requirements_clause_sql[:splitpos]
                            if requirements_clause_sql in where_empty_cache:
                                continue
                            try:
                                self.__logger.debug('submitter selecting worker')
                                async with con.execute(f'SELECT workers.id, workers.hwid, last_address from workers '
                                                       f'INNER JOIN resources ON workers.hwid=resources.hwid '
                                                       f'WHERE state == ? AND ( {requirements_clause_sql} ) ORDER BY RANDOM() LIMIT 1', (WorkerState.IDLE.value,)) as worcur:
                                    worker = await worcur.fetchone()
                            except aiosqlite.Error as e:
                                await con.execute('UPDATE tasks SET "state" = ?, "state_details" = ? WHERE "id" = ?',
                                                  (TaskState.ERROR.value,
                                                   json.dumps({'message': traceback.format_exc(),
                                                               'happened_at': task_row['state'],
                                                               'type': 'exception',
                                                               'exception_str': str(e),
                                                               'exception_type': str(type(e))}),
                                                   task_row['id']))
                                total_state_changes += 1
                                self.__logger.exception(f'error matching workers for the task {task_row["id"]}')
                                continue
                            if worker is None:  # nothing available
                                where_empty_cache.add(requirements_clause_sql)
                                continue
                            # note that there might be no implicit transaction here yet, so previously selected
                            # worker might have changed states between that select and this update
                            # so we doublecheck in a transaction
                            if not con.in_transaction:
                                await con.execute('BEGIN IMMEDIATE')
                                async with con.execute('SELECT "state" FROM workers WHERE "id" == ?', (worker['id'],)) as worcur:
                                    if (await worcur.fetchone())['state'] != WorkerState.IDLE.value:
                                        self.__logger.debug('submitter: worker changed state while trying to submit, skipping')
                                        continue
                            await con.execute('UPDATE tasks SET state = ? WHERE "id" = ?',
                                                                (TaskState.INVOKING.value, task_row['id']))
                            total_state_changes += 1
                            await con.execute('UPDATE workers SET state = ? WHERE "id" = ?',
                                                                (WorkerState.INVOKING.value, worker['id']))
                            # set resource usage straight away
                            try:
                                await self.__update_worker_resouce_usage(worker['id'], resources=requirements_clause_dict, hwid=worker['hwid'], connection=con)
                            except NotEnoughResources:
                                self.__logger.warning(f'inconsistence in worker resource tracking! could not submit to worker {worker["id"]}')
                                continue

                            submitters.append(_submitter(dict(task_row), dict(worker)))
                            self.__logger.debug('submitter scheduled')
                        await con.commit()
                        for coro in submitters:
                            tasks_to_wait.add(asyncio.create_task(coro))
                    #
                    # means task is done being processed by current node,
                    # now it should be passed to the next node
                    elif task_state == TaskState.DONE or task_state == TaskState.SPAWNED:
                        for task_row in all_task_rows:
                            if task_row['state'] == TaskState.DONE.value:
                                out_plug_name = task_row['node_output_name'] or 'main'
                            else:
                                out_plug_name = task_row['node_output_name'] or 'spawned'
                            async with con.execute('SELECT * FROM node_connections WHERE node_id_out = ? AND out_name = ?',
                                                   (task_row['node_id'], out_plug_name)) as wire_cur:
                                all_wires = await wire_cur.fetchall()
                            wire_count = len(all_wires)
                            if wire_count > 0:
                                if wire_count == 1:
                                    wire = all_wires[0]
                                    await con.execute('UPDATE tasks SET node_id = ?, node_input_name = ?, state = ?, work_data = ? '
                                                      'WHERE "id" = ?',
                                                      (wire['node_id_in'], wire['in_name'], TaskState.WAITING.value, None, task_row['id']))
                                    total_state_changes += 1
                                else:
                                    for i, splited_task_id in enumerate(await self.split_task(task_row['id'], wire_count, con)):
                                        await con.execute('UPDATE tasks SET node_id = ?, node_input_name = ?, state = ?, work_data = ?'
                                                          'WHERE "id" = ?',
                                                          (all_wires[i]['node_id_in'], all_wires[i]['in_name'], TaskState.WAITING.value, None,
                                                           splited_task_id))
                                        total_state_changes += 1
                                    total_state_changes += 1  # this is for original (for split) task changing state to SPLITTED

                            else:
                                # the problem is that there are tasks that done, but have no wires to go anywhere
                                # and that is the point, they are done done. But processing thousands of them every time is painful
                                # so we need to somehow prevent them from being amilessly processed
                                # this is a testing desicion, TODO: test and see if thes is a good way to deal with the problem
                                await con.execute('UPDATE "tasks" SET "paused" = 1 WHERE "id" = ?', (task_row['id'],))

                        await con.commit()

                    self.__logger.debug(f'{task_state.name} took: {time.perf_counter() - _debug_pstart}')

                # out of processing loop, but still in db connection
                if total_processed == 0:
                    # check maybe it's time to sleep
                    if len(tasks_to_wait) == 0:
                        # instead of NOT IN  here using explicit IN cuz this way db index works # async with con.execute('SELECT COUNT(id) AS total FROM tasks WHERE paused = 0 AND state NOT IN (?, ?)', (TaskState.ERROR.value, TaskState.DEAD.value)) as cur:
                        async with con.execute('SELECT COUNT(id) AS total FROM tasks WHERE paused = 0 AND state IN ({}) AND dead = 0'.format(','.join(str(state.value) for state in TaskState if state not in (TaskState.ERROR, TaskState.DEAD)))) as cur:
                            total = await cur.fetchone()
                        if total is None or total['total'] == 0:
                            self.__logger.debug('no useful tasks seem to be available')
                            self.__sleep()
                else:
                    self.wake()

            processing_time = time.perf_counter() - _debug_con
            if processing_time > 1.0:
                self.__logger.info(f'processing run in {processing_time}')
            else:
                self.__logger.debug(f'processing run in {processing_time}')

            # and wait for a bit
            if wakeup_task is not None:
                sleeping_tasks = (stop_task, kick_wait_task, wakeup_task)
            else:
                if self.__mode == SchedulerMode.DORMANT:
                    wakeup_task = asyncio.create_task(self.__wakeup_event.wait())
                    sleeping_tasks = (stop_task, kick_wait_task, wakeup_task)
                else:
                    sleeping_tasks = (stop_task, kick_wait_task)

            wdone, _ = await asyncio.wait(sleeping_tasks, timeout=0 if total_state_changes > 0 else self.__processing_interval * self.__processing_interval_mult,
                                          return_when=asyncio.FIRST_COMPLETED)
            if wakeup_task is not None and wakeup_task in wdone:
                wakeup_task = None
            if kick_wait_task in wdone:
                self.__task_processor_kick_event.clear()
                kick_wait_task = asyncio.create_task(self.__task_processor_kick_event.wait())
            if stop_task in wdone:
                break


        #
        # Out of while - means we are stopping. time to save all the nodes
        self.__logger.info('finishing task processor...')
        if len(tasks_to_wait) > 0:
            await asyncio.wait(tasks_to_wait, return_when=asyncio.ALL_COMPLETED)
        self.__logger.info('saving nodes to db')
        for node_id in self.__node_objects:
            await self.save_node_to_database(node_id)
            self.__logger.debug(f'node {node_id} saved to db')
        self.__logger.info('task processor finished')

    #
    # invocation consistency checker
    async def invocation_consistency_checker(self):
        """
        both scheduler and woker might crash at any time. so we need to check that
        worker may crash working on a task (
        :return:
        """
        pass

    #
    # callbacks

    #
    # worker reports done task
    async def task_done_reported(self, task: InvocationJob, stdout: str, stderr: str):
        async with self.__invocation_reporting_lock, \
                   aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            self.__logger.debug('task finished reported %s code %s', repr(task), task.exit_code())
            # sanity check
            async with con.execute('SELECT "state" FROM invocations WHERE "id" = ?', (task.invocation_id(),)) as cur:
                invoc = await cur.fetchone()
                if invoc is None:
                    self.__logger.error('reported task has non existing invocation id %d' % task.invocation_id())
                    return
                if invoc['state'] != InvocationState.IN_PROGRESS.value:
                    self.__logger.warning('reported task for a finished invocation. assuming that worker failed to cancel task previously and ignoring invocation results.')
                    return
            await con.execute('UPDATE invocations SET "state" = ?, "return_code" = ?, "runtime" = ? WHERE "id" = ?',
                              (InvocationState.FINISHED.value, task.exit_code(), task.running_time(), task.invocation_id()))
            async with con.execute('SELECT * FROM invocations WHERE "id" = ?', (task.invocation_id(),)) as incur:
                invocation = await incur.fetchone()
            assert invocation is not None

            await con.execute('UPDATE workers SET "state" = ? WHERE "id" = ?',
                              (WorkerState.IDLE.value, invocation['worker_id']))
            await self.__update_worker_resouce_usage(invocation['worker_id'], connection=con)  # remove resource usage info
            tasks_to_wait = []
            if not self.__use_external_log:
                await con.execute('UPDATE invocations SET "stdout" = ?, "stderr" = ? WHERE "id" = ?',
                                  (stdout, stderr, task.invocation_id()))
            else:
                await con.execute('UPDATE invocations SET "log_external" = 1 WHERE "id" = ?',
                                  (task.invocation_id(),))
                tasks_to_wait.append(asyncio.create_task(self._save_external_logs(task.invocation_id(), stdout, stderr)))

            if task.invocation_id() in self.__db_cache['invocations']:
                del self.__db_cache['invocations'][task.invocation_id()]

            if task.finished_needs_retry():  # max retry count will be checked by task processor
                await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                  (TaskState.READY.value, invocation['task_id']))
            elif task.finished_with_error():
                await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                  (TaskState.ERROR.value, invocation['task_id']))
                await con.execute('UPDATE tasks SET "state_details" = ? WHERE "id" = ?',
                                  (json.dumps({'message': f'see invocation #{invocation["id"]} log for details',
                                               'happened_at': TaskState.IN_PROGRESS.value,
                                               'type': 'invocation'})
                                   , invocation['task_id']))
            else:
                await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                  (TaskState.POST_WAITING.value, invocation['task_id']))

            await con.commit()
            if len(tasks_to_wait) > 0:
                await asyncio.wait(tasks_to_wait)
        self.wake()
        self.poke_task_processor()

    async def _save_external_logs(self, invocation_id, stdout, stderr):
        logbasedir = self.__external_log_location / 'invocations' / f'{invocation_id}'
        try:
            if not logbasedir.exists():
                logbasedir.mkdir(exist_ok=True)
            async with aiofiles.open(logbasedir / 'stdout.log', 'w') as fstdout, \
                    aiofiles.open(logbasedir / 'stderr.log', 'w') as fstderr:
                await asyncio.gather(fstdout.write(stdout),
                                     fstderr.write(stderr))
        except OSError:
            self.__logger.exception('error happened saving external logs! Ignoring this error')

    #
    # worker reports canceled task
    async def task_cancel_reported(self, task: InvocationJob, stdout: str, stderr: str):
        async with self.__invocation_reporting_lock,\
                   aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            self.__logger.debug('task cancelled reported %s', repr(task))
            # sanity check
            async with con.execute('SELECT "state" FROM invocations WHERE "id" = ?', (task.invocation_id(),)) as cur:
                invoc = await cur.fetchone()
                if invoc is None:
                    self.__logger.error('reported task has non existing invocation id %d' % task.invocation_id())
                    return
                if invoc['state'] != InvocationState.IN_PROGRESS.value:
                    self.__logger.warning('reported task for a finished invocation. assuming that worker failed to cancel task previously and ignoring invocation results.')
                    return
            await con.execute('UPDATE invocations SET "state" = ?, "runtime" = ? WHERE "id" = ?',
                              (InvocationState.FINISHED.value, task.running_time(), task.invocation_id()))
            async with con.execute('SELECT * FROM invocations WHERE "id" = ?', (task.invocation_id(),)) as incur:
                invocation = await incur.fetchone()
            assert invocation is not None
            if task.invocation_id() in self.__db_cache['invocations']:
                del self.__db_cache['invocations'][task.invocation_id()]

            await con.execute('UPDATE workers SET "state" = ? WHERE "id" = ?',
                              (WorkerState.IDLE.value, invocation['worker_id']))
            await self.__update_worker_resouce_usage(invocation['worker_id'], connection=con)  # remove resource usage info
            tasks_to_wait = []
            if not self.__use_external_log:
                await con.execute('UPDATE invocations SET "stdout" = ?, "stderr" = ? WHERE "id" = ?',
                                  (stdout, stderr, task.invocation_id()))
            else:
                await con.execute('UPDATE invocations SET "log_external" = 1, "stdout" = null, "stderr" = null WHERE "id" = ?',
                                  (task.invocation_id(),))
                tasks_to_wait.append(asyncio.create_task(self._save_external_logs(task.invocation_id(), stdout, stderr)))
            await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                              (TaskState.WAITING.value, invocation['task_id']))
            await con.commit()
            if len(tasks_to_wait) > 0:
                await asyncio.wait(tasks_to_wait)
        self.__logger.debug(f'cancelling task done {repr(task)}')
        self.wake()
        self.poke_task_processor()

    #
    # add new worker to db
    async def add_worker(self, addr: str, worker_type: WorkerType, worker_resources: WorkerResources, assume_active=True):  # TODO: all resource should also go here
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            await con.execute('BEGIN IMMEDIATE')  # important to have locked DB during all this state change
            # logic for now:
            #  - search for same last_address, same hwid
            #  - if no - search for first entry (OFF or UNKNOWN) with same hwid, ignore address
            #    - in this case also delete addr from DB if exists
            async with con.execute('SELECT "id", state FROM "workers" WHERE "last_address" == ? AND hwid == ?', (addr, worker_resources.hwid)) as worcur:
                worker_row = await worcur.fetchone()
            if worker_row is None:
                # first ensure that there is no entry with the same address
                await con.execute('UPDATE "workers" SET "last_address" = ? WHERE "last_address" == ?', (None, addr))
                async with con.execute('SELECT "id", state FROM "workers" WHERE hwid == ? AND '
                                       '(state == ? OR state == ?)', (worker_resources.hwid,
                                                                      WorkerState.OFF.value, WorkerState.UNKNOWN.value)) as worcur:
                    worker_row = await worcur.fetchone()
            if assume_active:
                ping_state = WorkerPingState.WORKING.value
                state = WorkerState.IDLE.value
            else:
                ping_state = WorkerPingState.OFF.value
                state = WorkerState.OFF.value

            tstamp = int(time.time())
            if worker_row is not None:
                if worker_row['state'] == WorkerState.INVOKING.value:  # so we are in the middle of sumbission
                    state = WorkerState.INVOKING.value  # then we preserve INVOKING state
                await self.reset_invocations_for_worker(worker_row['id'], con=con)
                await con.execute('UPDATE "workers" SET '
                                  'hwid=?, '
                                  'last_seen=?, ping_state=?, state=?, worker_type=?, '
                                  'last_address=?  '
                                  'WHERE "id"=?',
                                  (worker_resources.hwid,
                                   tstamp, ping_state, state, worker_type.value,
                                   addr,
                                   worker_row['id']))
                # async with con.execute('SELECT "id" FROM "workers" WHERE last_address=?', (addr,)) as worcur:
                #     worker_id = (await worcur.fetchone())['id']
                worker_id = worker_row['id']
                self.__db_cache['workers_state'][worker_id].update({'last_seen': tstamp,
                                                                        'last_checked': tstamp,
                                                                        'ping_state': ping_state,
                                                                        'worker_id': worker_id})
                # await con.execute('UPDATE tmpdb.tmp_workers_states SET '
                #                   'last_seen=?, ping_state=? '
                #                   'WHERE worker_id=?',
                #                   (tstamp, ping_state, worker_id))
            else:
                async with con.execute('INSERT INTO "workers" '
                                       '(hwid, '
                                       'last_address, last_seen, ping_state, state, worker_type) '
                                       'VALUES '
                                       '(?, ?, ?, ?, ?, ?)',
                                       (worker_resources.hwid, addr, tstamp, ping_state, state, worker_type.value)) as insworcur:
                    worker_id = insworcur.lastrowid
                self.__db_cache['workers_state'][worker_id] = {'last_seen': tstamp,
                                                               'last_checked': tstamp,
                                                               'ping_state': ping_state,
                                                               'worker_id': worker_id}
                # await con.execute('INSERT INTO tmpdb.tmp_workers_states '
                #                   '(worker_id, last_seen, ping_state) '
                #                   'VALUES '
                #                   '(?, ?, ?)',
                #                   (worker_id, tstamp, ping_state))

            await con.execute('INSERT INTO resources '
                              '(hwid, cpu_count, total_cpu_count, '
                              'cpu_mem, total_cpu_mem, '
                              'gpu_count, total_gpu_count, '
                              'gpu_mem, total_gpu_mem) '
                              'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) '
                              'ON CONFLICT(hwid) DO UPDATE SET '
                              'cpu_count=excluded.cpu_count, total_cpu_count=excluded.total_cpu_count, '
                              'cpu_mem=excluded.cpu_mem, total_cpu_mem=excluded.total_cpu_mem, '
                              'gpu_count=excluded.gpu_count, total_gpu_count=excluded.total_gpu_count, '
                              'gpu_mem=excluded.gpu_mem, total_gpu_mem=excluded.total_gpu_mem',
                              (worker_resources.hwid,
                               worker_resources.cpu_count,
                               worker_resources.total_cpu_count,
                               worker_resources.cpu_mem,
                               worker_resources.total_cpu_mem,
                               worker_resources.gpu_count,
                               worker_resources.total_gpu_count,
                               worker_resources.gpu_mem,
                               worker_resources.total_gpu_mem)
                               )
            await self.__update_worker_resouce_usage(worker_id, hwid=worker_resources.hwid, connection=con)  # used resources are inited to none
            await con.commit()
        self.poke_task_processor()

    # TODO: add decorator that locks method from reentry or smth
    #  potentially a worker may report done while this works,
    #  or when scheduler picked worker and about to run this, which will lead to inconsistency warning
    #  NOTE!: so far it's always called from a STARTED transaction, so there should not be reentry possible
    #  But that is not enforced right now, easy to make mistake
    async def __update_worker_resouce_usage(self, worker_id: int, resources: Optional[dict] = None, *, hwid=None, connection: aiosqlite.Connection) -> bool:
        """
        updates resource information based on new worker resources usage
        as part of ongoing transaction
        Note: con SHOULD HAVE STARTED TRANSACTION, otherwise it might be not safe to call this

        :param worker_id:
        :param hwid: if hwid of worker_id is already known - provide it here to skip extra db query. but be SURE it's correct!
        :param connection: opened db connection. expected to have Row as row factory
        :return: if commit is needed on connection (if db set operation happened)
        """
        resource_fields = ('cpu_count', 'cpu_mem', 'gpu_count', 'gpu_mem')

        workers_resources = self.__db_cache['workers_resources']
        if hwid is None:
            async with connection.execute('SELECT "hwid" FROM "workers" WHERE "id" == ?', (worker_id,)) as worcur:
                hwid = (await worcur.fetchone())['hwid']

        # calculate available resources NOT counting current worker_id
        async with connection.execute(f'SELECT '
                                      f'{", ".join(resource_fields)}, '
                                      f'{", ".join("total_"+x for x in resource_fields)} '
                                      f'FROM resources WHERE hwid == ?', (hwid,)) as rescur:
            available_res = dict(await rescur.fetchone())
        current_available = {k: v for k, v in available_res.items() if not k.startswith('total_')}
        available_res = {k[len('total_'):]: v for k, v in available_res.items() if k.startswith('total_')}

        for wid, res in workers_resources.items():
            if wid == worker_id:
                continue  # SKIP worker_id currently being set
            if res.get('hwid') != hwid:
                continue
            for field in resource_fields:
                if field not in res:
                    continue
                available_res[field] -= res[field]
        ##

        # now choose proper amount of resources to pick
        if resources is None:
            workers_resources[worker_id] = {'hwid': hwid}  # remove resource usage info
        else:
            workers_resources[worker_id] = {}
            for field in resource_fields:
                if field not in resources:
                    continue
                if available_res[field] < resources[field]:
                    raise NotEnoughResources(f'{field}: {resources[field]} out of {available_res[field]}')
                # so we take preferred amount of resources (or minimum if pref not set), but no more than available
                # if preferred is lower than min - it's ignored
                workers_resources[worker_id][field] = min(available_res[field],
                                                          max(resources.get(f'pref_{field}', resources[field]),
                                                              resources[field]))
                available_res[field] -= workers_resources[worker_id][field]

            workers_resources[worker_id]['hwid'] = hwid  # just to ensure it was not overriden

        self.__logger.debug(f'updating resources {hwid} with {available_res} against {current_available}')
        self.__logger.debug(workers_resources)

        if available_res == current_available:  # nothing needs to be updated
            return False

        await connection.execute(f'UPDATE resources SET {", ".join(f"{k}={v}" for k, v in available_res.items())} WHERE hwid == ?', (hwid,))
        return True

    #
    # worker reports it being stopped
    async def worker_stopped(self, addr: str):
        """

        :param addr:
        :return:
        """
        self.__logger.debug(f'worker reported stopped: {addr}')
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            await con.execute('BEGIN IMMEDIATE')
            async with con.execute('SELECT id, hwid from "workers" WHERE "last_address" = ?', (addr,)) as worcur:
                worker_row = await worcur.fetchone()
            if worker_row is None:
                self.__logger.warning(f'unregistered worker reported "stopped": {addr}, ignoring')
                await con.rollback()
                return
            wid = worker_row['id']
            hwid = worker_row['hwid']
            # print(wid)

            # we ensure there are no invocations running with this worker
            async with con.execute('SELECT "id", task_id FROM invocations WHERE worker_id = ? AND "state" = ?', (wid, InvocationState.IN_PROGRESS.value)) as invcur:
                invocations = await invcur.fetchall()

            await con.execute('UPDATE workers SET "state" = ? WHERE "id" = ?', (WorkerState.OFF.value, wid))
            await con.executemany('UPDATE invocations SET state = ? WHERE "id" = ?', ((InvocationState.FINISHED.value, x["id"]) for x in invocations))
            await con.executemany('UPDATE tasks SET state = ? WHERE "id" = ?', ((TaskState.WAITING.value, x["task_id"]) for x in invocations))
            await self.__update_worker_resouce_usage(wid, hwid=hwid, connection=con)
            del self.__db_cache['workers_resources'][wid]  # remove from cache
            await con.commit()
        self.__logger.debug(f'finished worker reported stopped: {addr}')

    #
    # protocol related commands
    #
    #
    # cancel invocation
    async def cancel_invocation(self, invocation_id: str):
        self.__logger.debug(f'canceling invocation {invocation_id}')
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT * FROM "invocations" WHERE "id" = ?', (invocation_id,)) as cur:
                invoc = await cur.fetchone()
            if invoc is None or invoc['state'] != InvocationState.IN_PROGRESS.value:
                return
            async with con.execute('SELECT "last_address" FROM "workers" WHERE "id" = ?', (invoc['worker_id'],)) as cur:
                worker = await cur.fetchone()
        if worker is None:
            self.__logger.error('inconsistent worker ids? how?')
            return
        ip, port = worker['last_address'].rsplit(':', 1)

        # the logic is:
        # - we send the worker a signal to cancel invocation
        # - later worker sends task_cancel_reported, and we are happy
        # - but worker might be overloaded, broken or whatever and may never send it. and it can even finish task and send task_done_reported, witch we need to treat
        async with WorkerTaskClient(ip, int(port)) as client:
            await client.cancel_task()

        # oh no, we don't do that, we wait for worker to report task canceled.  await con.execute('UPDATE invocations SET "state" = ? WHERE "id" = ?', (InvocationState.FINISHED.value, invocation_id))

    #
    #
    async def cancel_invocation_for_task(self, task_id: int):
        self.__logger.debug(f'canceling invocation for task {task_id}')
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT "id" FROM "invocations" WHERE "task_id" = ? AND state = ?', (task_id, InvocationState.IN_PROGRESS.value)) as cur:
                invoc = await cur.fetchone()
        if invoc is None:
            return
        return await self.cancel_invocation(invoc['id'])

    #
    #
    async def cancel_invocation_for_worker(self, worker_id: int):
        self.__logger.debug(f'canceling invocation for worker {worker_id}')
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT "id" FROM "invocations" WHERE "worker_id" == ? AND state == ?', (worker_id, InvocationState.IN_PROGRESS.value)) as cur:
                invoc = await cur.fetchone()
        if invoc is None:
            return
        return await self.cancel_invocation(invoc['id'])

    #
    #
    async def force_set_node_task(self, task_id: int, node_id: int):
        self.__logger.debug(f'forcing task {task_id} to node {node_id}')
        try:
            async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
                con.row_factory = aiosqlite.Row
                await con.execute('PRAGMA FOREIGN_KEYS = on')
                await con.execute('UPDATE "tasks" SET "node_id" = ? WHERE "id" = ?', (node_id, task_id))
                await con.commit()
        except aiosqlite.IntegrityError:
            self.__logger.error('could not remove node connection because of database integrity check')
        else:
            self.wake()
            self.poke_task_processor()

    #
    # force change task state
    async def force_change_task_state(self, task_ids: Union[int, Iterable[int]], state: TaskState):
        """
        forces task into given state.
        obviously a task cannot be forced into certain states, like IN_PROGRESS, GENERATING, POST_GENERATING
        :param task_ids:
        :param state:
        :return:
        """
        if state in (TaskState.IN_PROGRESS, TaskState.GENERATING, TaskState.POST_GENERATING):
            self.__logger.error(f'cannot force task {task_ids} into state {state}')
            return
        if isinstance(task_ids, int):
            task_ids = [task_ids]
        query = 'UPDATE "tasks" SET "state" = %d WHERE "id" = ?' % state.value
        #print('beep')
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            for task_id in task_ids:
                await con.execute('BEGIN IMMEDIATE')
                async with con.execute('SELECT "state" FROM tasks WHERE "id" = ?', (task_id,)) as cur:
                    state = await cur.fetchone()
                    if state is None:
                        await con.rollback()
                        continue
                    state = TaskState(state[0])
                if state in (TaskState.IN_PROGRESS, TaskState.GENERATING, TaskState.POST_GENERATING):
                    self.__logger.warning(f'forcing task out of state {state} is not currently implemented')
                    await con.rollback()
                    continue

                await con.execute(query, (task_id,))
                #await con.executemany(query, ((x,) for x in task_ids))
                await con.commit()
        #print('boop')
        self.wake()
        self.poke_task_processor()

    #
    # change task's paused state
    async def set_task_paused(self, task_ids_or_group: Union[int, Iterable[int], str], paused: bool):
        if isinstance(task_ids_or_group, str):
            async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
                await con.execute('UPDATE "tasks" SET "paused" = ? WHERE "id" IN (SELECT "task_id" FROM task_groups WHERE "group" = ?)',
                                  (int(paused), task_ids_or_group))
                await con.commit()
            self.wake()
            self.poke_task_processor()
            return
        if isinstance(task_ids_or_group, int):
            task_ids_or_group = [task_ids_or_group]
        query = 'UPDATE "tasks" SET "paused" = %d WHERE "id" = ?' % int(paused)
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            await con.executemany(query, ((x,) for x in task_ids_or_group))
            await con.commit()
        self.wake()
        self.poke_task_processor()

    #
    # change task group archived state
    async def set_task_group_archived(self, task_group_name: str, state: TaskGroupArchivedState = TaskGroupArchivedState.ARCHIVED) -> None:
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            await con.execute('UPDATE task_group_attributes SET state=? WHERE "group"==?', (state.value, task_group_name))  # this triggers all task deadness | 2, so potentially it can be long, beware
            await con.commit()
            if state == TaskGroupArchivedState.NOT_ARCHIVED:
                self.poke_task_processor()  # unarchived, so kick task processor, just in case
                return
            # otherwise - it's archived
            # now all tasks belonging to that group should be set to dead|2
            # we need to make sure to cancel all running invocations for those tasks
            # at this point tasks are archived and won't be processed,
            # so we only expect concurrent changes due to already running _submitters and _awaiters,
            # like INVOKING->IN_PROGRESS
            async with con.execute('SELECT "id" FROM invocations '
                                   'INNER JOIN task_groups ON task_groups.task_id == invocations.task_id '
                                   'WHERE task_groups."group" == ? AND invocations.state == ?',
                                   (task_group_name, InvocationState.INVOKING.value)) as cur:
                invoking_invoc_ids = set(x['id'] for x in await cur.fetchall())
            async with con.execute('SELECT "id" FROM invocations '
                                   'INNER JOIN task_groups ON task_groups.task_id == invocations.task_id '
                                   'WHERE task_groups."group" == ? AND invocations.state == ?',
                                   (task_group_name, InvocationState.IN_PROGRESS.value)) as cur:
                active_invoc_ids = tuple(x['id'] for x in await cur.fetchall())
                # i sure use a lot of fetchall where it's much more natural to iterate cursor
                # that is because of a fear of db locking i got BEFORE switching to WAL, when iterating connection was randomly crashing other connections not taking timeout into account at all.

        # note at this point we might have some invoking_invocs_id, but at this point some of them
        # might already have been set to in-progress and even got into active_invoc_ids list

        # first - cancel all in-progress invocations
        for inv_id in active_invoc_ids:
            await self.cancel_invocation(inv_id)

        # now since we dont have the ability to safely cancel running _submitter task - we will just wait till
        # invoking invocations change state
        # sure it's a bit bruteforce
        # but a working solution for now
        if len(invoking_invoc_ids) == 0:
            return
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            while len(invoking_invoc_ids) > 0:
                # TODO: this forever while doesn't seem right
                #  in average case it should basically never happen at all
                #  only in case of really bad buggy network connections an invocation can get stuck on INVOKING
                #  but there are natural timeouts in _submitter that will switch it from INVOKING eventually
                #  the only question is - do we want to just stay in this function until it's resolved? UI's client is single thread, so it will get stuck waiting
                con.row_factory = aiosqlite.Row
                async with con.execute('SELECT "id",state FROM invocations WHERE state!={} AND "id" IN ({})'.format(
                                            InvocationState.IN_PROGRESS.value,
                                            ','.join(str(x) for x in invoking_invoc_ids))) as cur:
                    changed_state_ones = await cur.fetchall()

                for oid, ostate in ((x['id'], x['state']) for x in changed_state_ones):
                    if ostate == InvocationState.IN_PROGRESS.value:
                        await self.cancel_invocation(oid)
                    assert oid in invoking_invoc_ids
                    invoking_invoc_ids.remove(oid)
                await asyncio.sleep(0.5)

    #
    # set task name
    async def set_task_name(self, task_id: int, new_name: str):
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            await con.execute('UPDATE "tasks" SET "name" = ? WHERE "id" = ?', (new_name, task_id))
            await con.commit()

    #
    # set task groups
    async def set_task_groups(self, task_id: int, group_names: Iterable[str]):
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            await con.execute('BEGIN IMMEDIATE')
            async with con.execute('SELECT "group" FROM task_groups WHERE "task_id" = ?', (task_id,)) as cur:
                all_groups = set(x['group'] for x in await cur.fetchall())
            group_names = set(group_names)
            groups_to_set = group_names - all_groups
            groups_to_del = all_groups - group_names
            print(task_id, groups_to_set, groups_to_del, all_groups, group_names)

            for group_name in groups_to_set:
                await con.execute('INSERT INTO task_groups (task_id, "group") VALUES (?, ?)', (task_id, group_name))
                await con.execute('INSERT OR ABORT INTO task_group_attributes ("group", "ctime") VALUES (?, ?)', (group_name, int(datetime.utcnow().timestamp())))
            for group_name in groups_to_del:
                await con.execute('DELETE FROM task_groups WHERE task_id = ? AND "group" = ?', (task_id, group_name))
            await con.commit()

    #
    # update task attributes
    async def update_task_attributes(self, task_id: int, attributes_to_update: dict, attributes_to_delete: set):
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            await con.execute('BEGIN IMMEDIATE')
            async with con.execute('SELECT "attributes" FROM tasks WHERE "id" = ?', (task_id,)) as cur:
                row = await cur.fetchone()
            if row is None:
                self.__logger.warning(f'update task attributes for {task_id} failed. task id not found.')
                await con.commit()
                return
            attributes = await asyncio.get_event_loop().run_in_executor(None, json.loads, row['attributes'])
            attributes.update(attributes_to_update)
            for name in attributes_to_delete:
                if name in attributes:
                    del attributes[name]
            await con.execute('UPDATE tasks SET "attributes" = ? WHERE "id" = ?', (await asyncio.get_event_loop().run_in_executor(None, json.dumps, attributes),
                                                                                        task_id))
            await con.commit()

    #
    # set environment resolver
    async def set_task_environment_resolver_arguments(self, task_id: int, env_res: Optional[EnvironmentResolverArguments]):
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            await con.execute('UPDATE tasks SET "environment_resolver_data" = ? WHERE "id" = ?',
                              (await env_res.serialize_async() if env_res is not None else None,
                               task_id))
            await con.commit()

    #
    # node stuff
    async def set_node_name(self, node_id: int, node_name: str) -> str:
        """
        rename node. node_name may undergo validation and change. final node name that was set is returned
        :param node_id: node id
        :param node_name: proposed node name
        :return: actual node name set
        """
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            await con.execute('UPDATE "nodes" SET "name" = ? WHERE "id" = ?', (node_name, node_id))
            if node_id in self.__node_objects:
                self.__node_objects[node_id].set_name(node_name)
            await con.commit()
        return node_name

    #
    # reset node's stored state
    async def wipe_node_state(self, node_id):
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            await con.execute('UPDATE "nodes" SET node_object = NULL WHERE "id" = ?', (node_id,))
            if node_id in self.__node_objects:
                # TODO: this below may be not safe (at least not proven to be safe yet, but maybe). check
                del self.__node_objects[node_id]  # it's here to "protect" operation within db transaction. TODO: but a proper __node_object lock should be in place instead
            await con.commit()
        self.wake()

    #
    # copy nodes
    async def duplicate_nodes(self, node_ids: Iterable[int]) -> Dict[int, int]:
        """
        copies given nodes, including connections between given nodes,
        and returns mapping from given node_ids to respective new copies

        :param node_ids:
        :return:
        """
        old_to_new = {}
        for nid in node_ids:
            node_obj = await self._get_node_object_by_id(nid)
            node_type, node_name = await self.get_node_type_and_name_by_id(nid)
            new_id = await self.add_node(node_type, f'{node_name} copy')
            new_node_obj = await self._get_node_object_by_id(new_id)
            node_obj.copy_ui_to(new_node_obj)
            old_to_new[nid] = new_id

        # now copy connections
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            node_ids_str = f'({",".join(str(x) for x in node_ids)})'
            async with con.execute(f'SELECT * FROM node_connections WHERE node_id_in IN {node_ids_str} AND node_id_out IN {node_ids_str}') as cur:
                all_cons = await cur.fetchall()
        for nodecon in all_cons:
            assert nodecon['node_id_in'] in old_to_new
            assert nodecon['node_id_out'] in old_to_new
            await self.add_node_connection(old_to_new[nodecon['node_id_out']], nodecon['out_name'], old_to_new[nodecon['node_id_in']], nodecon['in_name'])
        return old_to_new
        # TODO: NotImplementedError("recheck and needs testing")

    #
    #
    # node reports it's interface was changed. not sure why it exists
    async def node_reports_changes_needs_saving(self, node_id):
        assert node_id in self.__node_objects, 'this may be caused by race condition with node deletion'
        # TODO: introduce __node_objects lock? or otherwise secure access
        await self.save_node_to_database(node_id)

    #
    # save node to database.
    async def save_node_to_database(self, node_id):
        """
        save node with given node_id to database
        if node is not in our list of nodes - we assume it was not touched, not changed, so no saving needed

        :param node_id:
        :return:
        """
        # TODO: introduce __node_objects lock? or otherwise secure access
        #  why? this happens on ui_update, which can happen cuz of request from viewer.
        #  while node processing happens in a different thread, so this CAN happen at the same time with this
        #  AND THIS IS BAD! (potentially) if a node has changing internal state - this can save some inconsistent snapshot of node state!
        node_object = self.__node_objects[node_id]
        if node_object is None:
            self.__logger.error('node_object is None while')
            return
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            await con.execute('UPDATE "nodes" SET node_object = ? WHERE "id" = ?',
                              (await node_object.serialize_async(), node_id))
            await con.commit()

    #
    # set worker groups
    async def set_worker_groups(self, worker_hwid: int, groups: List[str]):
        groups = set(groups)
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            await con.execute('BEGIN IMMEDIATE')  # start transaction straight away
            async with con.execute('SELECT "group" FROM worker_groups WHERE worker_hwid == ?', (worker_hwid,)) as cur:
                existing_groups = set(x[0] for x in await cur.fetchall())
            to_delete = existing_groups - groups
            to_add = groups - existing_groups
            if len(to_delete):
                await con.execute(f'DELETE FROM worker_groups WHERE worker_hwid == ? AND "group" IN ({",".join(("?",)*len(to_delete))})', (worker_hwid, *to_delete))
            if len(to_add):
                await con.executemany(f'INSERT INTO worker_groups (worker_hwid, "group") VALUES (?, ?)',
                                      ((worker_hwid, x) for x in to_add))
            await con.commit()

    #
    # stuff
    @atimeit(0.005)
    async def get_full_ui_state(self, task_groups: Optional[Iterable[str]] = None, skip_dead=True, skip_archived_groups=True):
        self.__logger.debug('full update for %s', task_groups)
        now = datetime.now()
        group_totals_update_interval = 5
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT "id", "type", "name" FROM "nodes"') as cur:
                all_nodes = {x['id']: dict(x) for x in await cur.fetchall()}
            async with con.execute('SELECT * FROM "node_connections"') as cur:
                all_conns = {x['id']: dict(x) for x in await cur.fetchall()}
            if not task_groups:  # None or []
                all_tasks = dict()
                # async with con.execute('SELECT tasks.*, task_splits.origin_task_id, task_splits.split_id, GROUP_CONCAT(task_groups."group") as groups, invocations.progress '
                #                        'FROM "tasks" '
                #                        'LEFT JOIN "task_splits" ON tasks.id=task_splits.task_id AND tasks.split_level=task_splits.split_level '
                #                        'LEFT JOIN "task_groups" ON tasks.id=task_groups.task_id '
                #                        'LEFT JOIN "invocations" ON tasks.id=invocations.task_id AND invocations.state = %d '
                #                        'GROUP BY tasks."id"' % InvocationState.IN_PROGRESS.value) as cur:
                #     all_tasks_rows = await cur.fetchall()
                # for task_row in all_tasks_rows:
                #     task = dict(task_row)
                #     if task['groups'] is None:
                #         task['groups'] = set()
                #     else:
                #         task['groups'] = set(task['groups'].split(','))  # TODO: enforce no commas (,) in group names
                #     all_tasks[task['id']] = task
            else:
                all_tasks = dict()
                for group in task_groups:
                    # _dbg = time.perf_counter()
                    async with con.execute('SELECT tasks.id, tasks.parent_id, tasks.children_count, tasks.active_children_count, tasks.state, tasks.state_details, tasks.paused, tasks.node_id, '
                                           'tasks.node_input_name, tasks.node_output_name, tasks.name, tasks.split_level, tasks.work_data_invocation_attempt, '
                                           'task_splits.origin_task_id, task_splits.split_id, invocations."id" as invoc_id, GROUP_CONCAT(task_groups."group") as groups '
                                           'FROM "tasks" '
                                           'LEFT JOIN "task_groups" ON tasks.id=task_groups.task_id AND task_groups."group" == ?'
                                           'LEFT JOIN "task_splits" ON tasks.id=task_splits.task_id '
                                           'LEFT JOIN "invocations" ON tasks.id=invocations.task_id AND invocations.state = ? '
                                           'WHERE task_groups."group" == ? AND tasks.dead {dodead} '
                                           'GROUP BY tasks."id"'.format(dodead=f'== 0' if skip_dead else 'IN (0,1)'),
                                           (group, InvocationState.IN_PROGRESS.value, group)) as cur:  # NOTE: if you change = to LIKE - make sure to GROUP_CONCAT groups too
                        grp_tasks = await cur.fetchall()
                    # print(f'fetch groups: {time.perf_counter() - _dbg}')
                    for task_row in grp_tasks:
                        task = dict(task_row)
                        task['progress'] = self.__db_cache['invocations'].get(task['invoc_id'], {}).get('progress', None)
                        task['groups'] = set(task['groups'].split(','))
                        if task['id'] in all_tasks:
                            all_tasks[task['id']]['groups'].update(task['groups'])
                        else:
                            all_tasks[task['id']] = task
            # _dbg = time.perf_counter()
            #async with con.execute('SELECT DISTINCT task_groups."group", task_group_attributes.ctime FROM task_groups LEFT JOIN task_group_attributes ON task_groups."group" = task_group_attributes."group"') as cur:

            # some things are updated onlt once in a while, not on every update
            need_group_totals_update = (now - (self.__ui_cache.get('last_update_time', None) or datetime.fromtimestamp(0))).total_seconds() > group_totals_update_interval
            #async with con.execute('SELECT "group", "ctime", "state", "priority" FROM task_group_attributes' + (f' WHERE state == {TaskGroupArchivedState.NOT_ARCHIVED.value}' if skip_archived_groups else '')) as cur:
            if need_group_totals_update:
                sqlexpr =  'SELECT "group", "ctime", "state", "priority", tdone, tprog, terr, tall FROM task_group_attributes ' \
                           'LEFT JOIN ' \
                          f'(SELECT SUM(state=={TaskState.DONE.value}) as tdone, ' \
                           f'       SUM(state=={TaskState.IN_PROGRESS.value}) as tprog, ' \
                           f'       SUM(state=={TaskState.ERROR.value}) as terr, ' \
                           f'       COUNT() as tall, "group" as grp FROM tasks JOIN task_groups ON tasks."id"==task_groups.task_id WHERE tasks.dead==0 GROUP BY "group") ' \
                           'ON "grp"==task_group_attributes."group" ' \
                           + (f' WHERE state == {TaskGroupArchivedState.NOT_ARCHIVED.value}' if skip_archived_groups else '')
            else:
                sqlexpr = 'SELECT "group", "ctime", "state", "priority" FROM task_group_attributes' + (f' WHERE state == {TaskGroupArchivedState.NOT_ARCHIVED.value}' if skip_archived_groups else '')
            async with con.execute(sqlexpr) as cur:
                all_task_groups = {x['group']: dict(x) for x in await cur.fetchall()}
            if need_group_totals_update:
                self.__ui_cache['last_update_time'] = now
                self.__ui_cache['groups'] = {group: {k: attrs[k] for k in ('tdone', 'tprog', 'terr', 'tall')} for group, attrs in all_task_groups.items()}
            else:
                for group in all_task_groups:
                    all_task_groups[group].update(self.__ui_cache['groups'].get(group, {}))

            # print(f'distinct groups: {time.perf_counter() - _dbg}')
            # _dbg = time.perf_counter()
            async with con.execute('SELECT workers."id", '
                                   'cpu_count, '
                                   'total_cpu_count, '
                                   'cpu_mem, '
                                   'total_cpu_mem, '
                                   'gpu_count, '
                                   'total_gpu_count, '
                                   'gpu_mem, '
                                   'total_gpu_mem, '
                                   'workers."hwid", '
                                   'last_address, workers."state", worker_type, invocations.node_id, invocations.task_id, invocations."id" as invoc_id, '
                                   'GROUP_CONCAT(worker_groups."group") as groups '
                                   'FROM workers '
                                   'LEFT JOIN invocations ON workers."id" == invocations.worker_id AND invocations."state" == 0 '
                                   'LEFT JOIN worker_groups ON workers."hwid" == worker_groups.worker_hwid '
                                   'LEFT JOIN resources ON workers.hwid == resources.hwid '
                                   'GROUP BY workers."id"') as cur:
                all_workers = tuple({**dict(x),
                                     'last_seen': self.__db_cache['workers_state'][x['id']]['last_seen'],
                                     'progress': self.__db_cache['invocations'].get(x['invoc_id'], {}).get('progress', None)
                                     } for x in await cur.fetchall())

            # print(f'workers: {time.perf_counter() - _dbg}')
            data = await create_uidata(self.db_uid(), all_nodes, all_conns, all_tasks, all_workers, all_task_groups)
        return data

    #
    # change node connection callback
    async def change_node_connection(self, node_connection_id: int, new_out_node_id: Optional[int], new_out_name: Optional[str],
                                     new_in_node_id: Optional[int], new_in_name: Optional[str]):
        parts = []
        vals = []
        if new_out_node_id is not None:
            parts.append('node_id_out = ?')
            vals.append(new_out_node_id)
        if new_out_name is not None:
            parts.append('out_name = ?')
            vals.append(new_out_name)
        if new_in_node_id is not None:
            parts.append('node_id_in = ?')
            vals.append(new_in_node_id)
        if new_in_name is not None:
            parts.append('in_name = ?')
            vals.append(new_in_name)
        if len(vals) == 0:  # nothing to do
            return
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            vals.append(node_connection_id)
            await con.execute(f'UPDATE node_connections SET {", ".join(parts)} WHERE "id" = ?', vals)
            await con.commit()
        self.wake()

    #
    # add node connection callback
    async def add_node_connection(self, out_node_id: int, out_name: str, in_node_id: int, in_name: str) -> int:
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('INSERT INTO node_connections (node_id_out, out_name, node_id_in, in_name) VALUES (?,?,?,?)',
                                   (out_node_id, out_name, in_node_id, in_name)) as cur:
                ret = cur.lastrowid
            await con.commit()
            self.wake()
            return ret

    #
    # remove node connection callback
    async def remove_node_connection(self, node_connection_id: int):
        try:
            async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
                con.row_factory = aiosqlite.Row
                await con.execute('PRAGMA FOREIGN_KEYS = on')
                await con.execute('DELETE FROM node_connections WHERE "id" = ?', (node_connection_id,))
                await con.commit()
        except aiosqlite.IntegrityError as e:
            self.__logger.error('could not remove node connection because of database integrity check')

    #
    # add node
    async def add_node(self, node_type: str, node_name: str) -> int:
        if node_type not in pluginloader.plugins:
            raise RuntimeError('unknown node type')
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('INSERT INTO "nodes" ("type", "name") VALUES (?,?)',
                                   (node_type, node_name)) as cur:
                ret = cur.lastrowid
            await con.commit()
            return ret

    async def remove_node(self, node_id: int):
        try:
            async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
                con.row_factory = aiosqlite.Row
                await con.execute('PRAGMA FOREIGN_KEYS = on')
                await con.execute('DELETE FROM "nodes" WHERE "id" = ?', (node_id,))
                await con.commit()
        except aiosqlite.IntegrityError as e:
            self.__logger.error('could not remove node connection because of database integrity check')

    #
    # query connections
    async def get_node_input_connections(self, node_id: int, input_name: Optional[str] = None):
        return await self.get_node_connections(node_id, True, input_name)

    async def get_node_output_connections(self, node_id: int, output_name: Optional[str] = None):
        return await self.get_node_connections(node_id, False, output_name)

    async def get_node_connections(self, node_id: int, query_input: bool = True, name: Optional[str] = None):
        if query_input:
            nodecol = 'node_id_in'
            namecol = 'in_name'
        else:
            nodecol = 'node_id_out'
            namecol = 'out_name'
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            if name is None:
                async with con.execute('SELECT * FROM node_connections WHERE "%s" = ?' % (nodecol,),
                                       (node_id,)) as cur:
                    return [dict(x) for x in await cur.fetchall()]
            else:
                async with con.execute('SELECT * FROM node_connections WHERE "%s" = ? AND "%s" = ?' % (nodecol, namecol),
                                       (node_id, name)) as cur:
                    return [dict(x) for x in await cur.fetchall()]

    #
    # spawning new task callback
    @alocking()
    async def spawn_tasks(self, newtasks: Union[Iterable[TaskSpawn], TaskSpawn], con: Optional[aiosqlite.Connection] = None) -> SpawnStatus:
        """

        :param newtasks:
        :param con:
        :return:
        """

        async def _inner_shit():
            current_timestamp = int(datetime.utcnow().timestamp())
            for newtask in newtasks:
                if newtask.source_invocation_id() is not None:
                    async with con.execute('SELECT node_id, task_id FROM invocations WHERE "id" = ?',
                                           (newtask.source_invocation_id(),)) as incur:
                        invocrow = await incur.fetchone()
                        assert invocrow is not None
                        node_id: int = invocrow['node_id']
                        parent_task_id: int = invocrow['task_id']
                elif newtask.forced_node_task_id() is not None:
                    node_id, parent_task_id = newtask.forced_node_task_id()
                else:
                    self.__logger.error('ERROR CREATING SPAWN TASK: Malformed source')
                    continue

                async with con.execute('INSERT INTO tasks ("name", "attributes", "parent_id", "state", "node_id", "node_output_name", "environment_resolver_data") VALUES (?, ?, ?, ?, ?, ?, ?)',
                                       (newtask.name(), json.dumps(newtask._attributes()), parent_task_id,  # TODO: run dumps in executor
                                        TaskState.SPAWNED.value if newtask.create_as_spawned() else TaskState.WAITING.value,
                                        node_id, newtask.node_output_name(),
                                        newtask.environment_arguments().serialize() if newtask.environment_arguments() is not None else None)) as newcur:
                    new_id = newcur.lastrowid

                if parent_task_id is not None:  # inherit all parent's groups
                    # check and inherit parent's environment wrapper arguments
                    if newtask.environment_arguments() is None:
                        await con.execute('UPDATE tasks SET environment_resolver_data = (SELECT environment_resolver_data FROM tasks WHERE "id" == ?) WHERE "id" == ?',
                                          (parent_task_id, new_id))

                    # inc children count happens in db trigger
                    # inherit groups
                    async with con.execute('SELECT "group" FROM task_groups WHERE "task_id" = ?', (parent_task_id,)) as gcur:
                        groups = [x['group'] for x in await gcur.fetchall()]
                    if len(groups) > 0:
                        await con.executemany('INSERT INTO task_groups ("task_id", "group") VALUES (?, ?)',
                                              zip(itertools.repeat(new_id, len(groups)), groups))
                else:  # parent_task_id is None
                    # in this case we create a default group for the task.
                    # task should not be left without groups at all - otherwise it will be impossible to find in UI
                    new_group = '{name}#{id:d}'.format(name=newtask.name(), id=new_id)
                    await con.execute('INSERT INTO task_groups ("task_id", "group") VALUES (?, ?)',
                                      (new_id, new_group))
                    await con.execute('INSERT OR REPLACE INTO task_group_attributes ("group", "ctime") VALUES (?, ?)',
                                      (new_group, current_timestamp))
                    if newtask.default_priority() is not None:
                        await con.execute('UPDATE task_group_attributes SET "priority" = ? WHERE "group" = ?',
                                          (newtask.default_priority(), new_group))
                    #
                if newtask.extra_group_names():
                    groups = newtask.extra_group_names()
                    await con.executemany('INSERT INTO task_groups ("task_id", "group") VALUES (?, ?)',
                                          zip(itertools.repeat(new_id, len(groups)), groups))
                    for group in groups:
                        async with con.execute('SELECT "group" FROM task_group_attributes WHERE "group" == ?', (group,)) as gcur:
                            need_create = await gcur.fetchone() is None
                        if not need_create:
                            continue
                        await con.execute('INSERT INTO task_group_attributes ("group", "ctime") VALUES (?, ?)',
                                          (group, current_timestamp))

        if isinstance(newtasks, TaskSpawn):
            newtasks = (newtasks,)
        if con is not None:
            await _inner_shit()
        else:
            async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
                con.row_factory = aiosqlite.Row
                await _inner_shit()
                await con.commit()
        self.wake()
        self.poke_task_processor()
        return SpawnStatus.SUCCEEDED

    #
    async def node_name_to_id(self, name: str) -> List[int]:
        """
        get the list of node ids that have specified name
        :param name:
        :return:
        """
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            async with con.execute('SELECT "id" FROM "nodes" WHERE "name" = ?', (name,)) as cur:
                return list(x[0] for x in await cur.fetchall())

    #
    async def get_invocation_metadata(self, task_id: int):
        """
        get task's log metadata - meaning which nodes it ran on and how
        :param task_id:
        :return: dict[node_id -> dict[invocation_id: None]]
        """
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            logs = {}
            self.__logger.debug(f'fetching log metadata for {task_id}')
            async with con.execute('SELECT "id", node_id, runtime, worker_id from "invocations" WHERE "task_id" = ?',
                                   (task_id, )) as cur:
                async for entry in cur:
                    node_id = entry['node_id']
                    if node_id not in logs:
                        logs[node_id] = {}
                    logs[node_id][entry['id']] = {'runtime': entry['runtime'],
                                                  'worker_id': entry['worker_id'],
                                                  '__incompletemeta__': True}
            return logs

    async def get_logs(self, task_id: int, node_id: int, invocation_id: Optional[int] = None):
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            logs = {}
            self.__logger.debug(f"fetching for {task_id}, {node_id} {'' if invocation_id is None else invocation_id}")
            if invocation_id is None:  # TODO: disable this option
                async with con.execute('SELECT * from "invocations" WHERE "task_id" = ? AND "node_id" = ?',
                                       (task_id, node_id)) as cur:
                    async for entry in cur:
                        logs[entry['id']] = dict(entry)
            else:
                async with con.execute('SELECT * from "invocations" WHERE "task_id" = ? AND "node_id" = ? AND "id" = ?',
                                       (task_id, node_id, invocation_id)) as cur:
                    all_entries = await cur.fetchall()  # should be exactly 1 or 0
                for entry in all_entries:
                    entry = dict(entry)
                    if entry['state'] == InvocationState.IN_PROGRESS.value:
                        async with con.execute('SELECT last_address FROM workers WHERE "id" = ?', (entry['worker_id'],)) as worcur:
                            workrow = await worcur.fetchone()
                        if workrow is None:
                            self.__logger.error('Worker not found during log fetch! this is not supposed to happen! Database inconsistent?')
                        else:
                            try:
                                async with WorkerTaskClient(*address_to_ip_port(workrow['last_address'])) as client:
                                    stdout, stderr = await client.get_log(invocation_id)
                                if not self.__use_external_log:
                                    await con.execute('UPDATE "invocations" SET stdout = ?, stderr = ? WHERE "id" = ?',
                                                      (stdout, stderr, invocation_id))
                                    await con.commit()
                                # TODO: maybe add else case? save partial log to file?
                            except ConnectionError:
                                self.__logger.warning('could not connect to worker to get freshest logs')
                            else:
                                entry['stdout'] = stdout
                                entry['stderr'] = stderr

                    elif entry['state'] == InvocationState.FINISHED.value and entry['log_external'] == 1:
                        logbasedir = self.__external_log_location / 'invocations' / f'{invocation_id}'
                        stdout_path = logbasedir / 'stdout.log'
                        stderr_path = logbasedir / 'stderr.log'
                        try:
                            if stdout_path.exists():
                                async with aiofiles.open(stdout_path, 'r') as fstdout:
                                    entry['stdout'] = await fstdout.read()
                        except IOError:
                            self.__logger.exception(f'could not read external stdout log for {invocation_id}')
                        try:
                            if stderr_path.exists():
                                async with aiofiles.open(stderr_path, 'r') as fstderr:
                                    entry['stderr'] = await fstderr.read()
                        except IOError:
                            self.__logger.exception(f'could not read external stdout log for {invocation_id}')

                    logs[entry['id']] = entry
        return {node_id: logs}

    def server_address(self) -> str:
        return self.__server_address


default_config = f'''
[core]
## you can uncomment stuff below to specify some static values
## 
# server_ip = "192.168.0.2"
# server_port = {default_scheduler_port()}
# ui_ip = "192.168.0.2"
# ui_port = {default_ui_port()}

## you can turn off scheduler broadcasting if you want to manually configure viewer and workers to connect
## to a specific address
# broadcast = false

[scheduler]

[scheduler.globals]
## entries from this section will be available to any node from config[key] 
##
## if you use more than 1 machine - you must change this to a network location shared among all workers
## by default it's set to scheduler's machine local temp path, and will only work for 1 machine setup 
global_scratch_path = "{get_local_scratch_path()}"

[scheduler.database]
## you can specify default database path, 
##  but this can be overriden with command line argument --db-path
# path = "/path/to/database.db"

## uncomment line below to store task logs outside of the database
##  it works in a way that all NEW logs will be saved according to settings below
##  existing logs will be kept where they are
##  external logs will ALWAYS be looked for in location specified by store_logs_externally_location
##  so if you have ANY logs saved externally - you must keep store_logs_externally_location defined in the config, 
##    or those logs will be inaccessible
##  but you can safely move logs and change location in config accordingly, but be sure scheduler is not accessing them at that time
# store_logs_externally = true
# store_logs_externally_location = /path/to/dir/where/to/store/logs
'''


async def main_async(db_path=None):
    def graceful_closer(*args):
        scheduler.stop()

    noasync_do_close = False
    def noasync_windows_graceful_closer_event(*args):
        nonlocal noasync_do_close
        noasync_do_close = True

    async def windows_graceful_closer():
        while not noasync_do_close:
            await asyncio.sleep(1)
        graceful_closer()

    scheduler = Scheduler(db_path)
    win_signal_waiting_task = None
    try:
        asyncio.get_event_loop().add_signal_handler(signal.SIGINT, graceful_closer)
        asyncio.get_event_loop().add_signal_handler(signal.SIGTERM, graceful_closer)
    except NotImplementedError:  # solution for windows
        signal.signal(signal.SIGINT, noasync_windows_graceful_closer_event)
        signal.signal(signal.SIGBREAK, noasync_windows_graceful_closer_event)
        win_signal_waiting_task = asyncio.create_task(windows_graceful_closer())

    await scheduler.start()
    await scheduler.wait_till_stops()
    if win_signal_waiting_task is not None:
        if not win_signal_waiting_task.done():
            win_signal_waiting_task.cancel()
    logging.get_logger('scheduler').info('SCHEDULER STOPPED')


def main(argv):
    import argparse
    import tempfile

    parser = argparse.ArgumentParser('lifeblood scheduler')
    parser.add_argument('--db-path', help='path to sqlite database to use')
    parser.add_argument('--ephemeral', action='store_true', help='start with an empty one time use database, that is placed into shared memory IF POSSIBLE')
    parser.add_argument('--verbosity-pinger', help='set individual verbosity for worker pinger')
    opts = parser.parse_args(argv)

    # check and create default config if none
    create_default_user_config_file('scheduler', default_config)

    config = get_config('scheduler')
    if opts.db_path is not None:
        db_path = opts.db_path
    else:
        db_path = config.get_option_noasync('scheduler.database.path', str(paths.default_main_database_location()))

    global_logger = logging.get_logger('scheduler')

    fd = None
    if opts.ephemeral:
        if opts.db_path is not None:
            parser.error('only one of --db-path or --ephemeral must be provided, not both')
        # 'file:memorydb?mode=memory&cache=shared'
        # this does not work ^ cuz shared cache means that all runs on the *same connection*
        # and when there is a transaction conflict on the same connection - we get instalocked (SQLITE_LOCKED)
        # and there is no way to emulate normal DB in memory but with shared cache

        # look for shm (UNIX only)
        shm_path = Path('/dev/shm')
        lb_shm_path = None
        if shm_path.exists():
            lb_shm_path = shm_path/f'u{os.getuid()}-lifeblood'
            try:
                lb_shm_path.mkdir(exist_ok=True)
            except Exception as e:
                global_logger.warning('/dev/shm is not accessible (permission issues?), creating ephemeral database in temp dir')
                lb_shm_path = None
        else:
            global_logger.warning('/dev/shm is not supported by OS, creating ephemeral database in temp dir')

        fd, db_path = tempfile.mkstemp(dir=lb_shm_path, prefix='shedb-')

    if opts.verbosity_pinger:
        logging.get_logger('scheduler.worker_pinger').setLevel(opts.verbosity_pinger)
    try:
        asyncio.run(main_async(db_path))
    except KeyboardInterrupt:
        global_logger.warning('SIGINT caught')
        global_logger.info('SIGINT caught. Scheduler is stopped now.')
    finally:
        if opts.ephemeral:
            assert fd is not None
            os.close(fd)
            os.unlink(db_path)

if __name__ == '__main__':
    main(sys.argv[1:])
