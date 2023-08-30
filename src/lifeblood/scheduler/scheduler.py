import sys
import os
from pathlib import Path
import time
from datetime import datetime
import json
import itertools
import asyncio
import aiosqlite
import aiofiles
from aiorwlock import RWLock
from contextlib import asynccontextmanager

from .. import logging
from .. import paths
#from ..worker_task_protocol import WorkerTaskClient
from ..worker_messsage_processor import WorkerControlClient
from ..scheduler_task_protocol import SchedulerTaskProtocol, SpawnStatus
from ..scheduler_ui_protocol import SchedulerUiProtocol
from ..invocationjob import InvocationJob
from ..environment_resolver import EnvironmentResolverArguments
from ..broadcasting import create_broadcaster
from ..simple_worker_pool import WorkerPool
from ..nethelpers import address_to_ip_port, get_default_addr, get_default_broadcast_addr
from ..net_classes import WorkerResources
from ..taskspawn import TaskSpawn
from ..basenode import BaseNode
from ..exceptions import *
from .. import pluginloader
from ..enums import WorkerState, WorkerPingState, TaskState, InvocationState, WorkerType, \
    SchedulerMode, TaskGroupArchivedState
from ..config import get_config
from ..misc import atimeit, alocking
from ..defaults import scheduler_port as default_scheduler_port, ui_port as default_ui_port, scheduler_message_port as default_scheduler_message_port
from .. import aiosqlite_overlay
from ..ui_protocol_data import TaskData, TaskDelta, IncompleteInvocationLogData, InvocationLogData

from ..net_messages.address import DirectAddress, AddressChain
from ..scheduler_message_processor import SchedulerMessageProcessor

from .data_access import DataAccess
from .scheduler_component_base import SchedulerComponentBase
from .pinger import Pinger
from .task_processor import TaskProcessor
from .ui_state_accessor import UIStateAccessor

from typing import Optional, Any, Tuple, List, Iterable, Union, Dict


class Scheduler:
    def __init__(self, db_file_path, *, do_broadcasting: Optional[bool] = None, broadcast_interval: Optional[int] = None,
                 helpers_minimal_idle_to_ensure=1,
                 server_addr: Optional[Tuple[str, int, int]] = None,
                 server_ui_addr: Optional[Tuple[str, int]] = None):
        """
        TODO: add a docstring

        :param db_file_path:
        :param do_broadcasting:
        :param helpers_minimal_idle_to_ensure:
        :param server_addr:
        :param server_ui_addr:
        """
        self.__logger = logging.get_logger('scheduler')
        self.__logger.info('loading core plugins')
        pluginloader.init()  # TODO: move it outside of constructor
        self.__node_objects: Dict[int, BaseNode] = {}
        self.__node_objects_locks: Dict[int, RWLock] = {}
        config = get_config('scheduler')

        # this lock will prevent tasks from being reported cancelled and done at the same exact time should that ever happen
        # this lock is overkill already, but we can make it even more overkill by using set of locks for each invoc id
        # which would be completely useless now cuz sqlite locks DB as a whole, not even a single table, especially not just parts of table
        self.__invocation_reporting_lock = asyncio.Lock()

        self.__all_components = None
        self.__started_event = asyncio.Event()
        
        loop = asyncio.get_event_loop()

        if db_file_path is None:
            config = get_config('scheduler')
            db_file_path = config.get_option_noasync('core.database.path', str(paths.default_main_database_location()))
        if not db_file_path.startswith('file:'):  # if schema is used - we do not modify the db uri in any way
            db_file_path = os.path.realpath(os.path.expanduser(db_file_path))

        self.__logger.debug(f'starting scheduler with database: {db_file_path}')

        self.db_path = db_file_path
        self.data_access = DataAccess(db_file_path, 30)
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

        self.__pinger: Pinger = Pinger(self)
        self.__task_processor: TaskProcessor = TaskProcessor(self)
        self.ui_state_access: UIStateAccessor = UIStateAccessor(self)

        server_ip = None
        if server_addr is None:
            server_ip = config.get_option_noasync('core.server_ip', get_default_addr())
            server_port = config.get_option_noasync('core.server_port', default_scheduler_port())
            message_server_port = config.get_option_noasync('core.server_message_port', default_scheduler_message_port())
        else:
            server_ip, server_port, message_server_port = server_addr
        if server_ui_addr is None:
            ui_ip = config.get_option_noasync('core.ui_ip', server_ip or get_default_addr())
            ui_port = config.get_option_noasync('core.ui_port', default_ui_port())
        else:
            ui_ip, ui_port = server_ui_addr
        self.__stop_event = asyncio.Event()
        self.__server_closing_task = None
        self.__cleanup_tasks = None

        self.__server = None
        self.__server_coro = loop.create_server(self._scheduler_protocol_factory, server_ip, server_port, backlog=16)
        self.__server_address = ':'.join((server_ip, str(server_port)))
        self.__message_processor: Optional[SchedulerMessageProcessor] = None
        self.__message_address: Tuple[str, int] = (server_ip, message_server_port)
        self.__ui_server = None
        self.__ui_server_coro = loop.create_server(self._ui_protocol_factory, ui_ip, ui_port, backlog=16)
        self.__ui_address = ':'.join((ui_ip, str(ui_port)))
        if do_broadcasting is None:
            do_broadcasting = config.get_option_noasync('core.broadcast', True)
        if do_broadcasting:
            if broadcast_interval is None or broadcast_interval <= 0:
                broadcast_interval = config.get_option_noasync('core.broadcast_interval', 10)
            broadcast_info = json.dumps({
                'message_address': str(DirectAddress.from_host_port(*self.__message_address)),
                'worker': self.__server_address,
                'ui': self.__ui_address
            })
            self.__broadcasting_server = None
            self.__broadcasting_server_coro = create_broadcaster('lifeblood_scheduler', broadcast_info, ip=get_default_broadcast_addr(), broadcast_interval=broadcast_interval)
        else:
            self.__broadcasting_server = None
            self.__broadcasting_server_coro = None

        self.__worker_pool = None
        self.__worker_pool_helpers_minimal_idle_to_ensure = helpers_minimal_idle_to_ensure

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
        return self.data_access.db_uid

    def wake(self):
        """
        scheduler may go into DORMANT mode when he things there's nothing to do
        in that case wake() call exits DORMANT mode immediately
        if wake is not called on some change- eventually scheduler will check it's shit and will decide to exit DORMANT mode on it's own, it will just waste some time first
        if currently not in DORMANT mode - nothing will happen

        :return:
        """
        self.__task_processor.wake()
        self.__pinger.wake()

    def poke_task_processor(self):
        """
        kick that lazy ass to stop it's waitings and immediately perform another processing iteration
        this is not connected to wake, __sleep and DORMANT mode,
        this is just one-time kick
        good to perform when task was changed somewhere async, outside of task_processor

        :return:
        """
        self.__task_processor.poke()

    def _component_changed_mode(self, component: SchedulerComponentBase, mode: SchedulerMode):
        if component == self.__task_processor and mode == SchedulerMode.DORMANT:
            self.__logger.info('task processor switched to DORMANT mode')
            self.__pinger.sleep()

    def message_processor(self) -> SchedulerMessageProcessor:
        """
        get scheduler's main message processor
        """
        return self.__message_processor

    async def get_node_type_and_name_by_id(self, node_id: int) -> (str, str):
        async with self.data_access.data_connection() as con:
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
        async with self.data_access.data_connection() as con:
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

            # newnode: BaseNode = pluginloader.plugins[node_type].create_node_object(node_row['name'], self)
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
        async with self.data_access.data_connection() as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT attributes, environment_resolver_data FROM tasks WHERE "id" = ?', (task_id,)) as cur:
                res = await cur.fetchone()
            if res is None:
                raise RuntimeError('task with specified id was not found')
            env_res_args = None
            if res['environment_resolver_data'] is not None:
                env_res_args = await EnvironmentResolverArguments.deserialize_async(res['environment_resolver_data'])
            return await asyncio.get_event_loop().run_in_executor(None, json.loads, res['attributes']), env_res_args

    async def get_task_fields(self, task_id: int) -> Dict[str, Any]:
        """
        returns information about the given task, excluding thicc fields like attributes or env resolver
        for those - use get_task_attributes

        :param task_id:
        :return:
        """
        async with self.data_access.data_connection() as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT "id", "name", parent_id, children_count, active_children_count, "state", paused, '
                                   '"node_id", split_level, priority, "dead" FROM tasks WHERE "id" == ?', (task_id,)) as cur:
                res = await cur.fetchone()
            if res is None:
                raise RuntimeError('task with specified id was not found')
            return dict(res)

    async def task_name_to_id(self, name: str) -> List[int]:
        """
        get the list of task ids that have specified name

        :param name:
        :return:
        """
        async with self.data_access.data_connection() as con:
            async with con.execute('SELECT "id" FROM "tasks" WHERE "name" = ?', (name,)) as cur:
                return list(x[0] for x in await cur.fetchall())

    async def get_task_invocation_serialized(self, task_id: int) -> Optional[bytes]:
        async with self.data_access.data_connection() as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT work_data FROM tasks WHERE "id" = ?', (task_id,)) as cur:
                res = await cur.fetchone()
            if res is None:
                raise RuntimeError('task with specified id was not found')
            return res[0]

    async def worker_id_from_address(self, addr: str) -> Optional[int]:
        async with self.data_access.data_connection() as con:
            async with con.execute('SELECT "id" FROM workers WHERE last_address = ?', (addr,)) as cur:
                ret = await cur.fetchone()
        if ret is None:
            return None
        return ret[0]

    async def get_worker_state(self, wid: int, con: Optional[aiosqlite.Connection] = None) -> WorkerState:
        if con is None:
            async with self.data_access.data_connection() as con:
                async with con.execute('SELECT "state" FROM "workers" WHERE "id" = ?', (wid,)) as cur:
                    res = await cur.fetchone()
        else:
            async with con.execute('SELECT "state" FROM "workers" WHERE "id" = ?', (wid,)) as cur:
                res = await cur.fetchone()
        if res is None:
            raise ValueError(f'worker with given wid={wid} was not found')
        return WorkerState(res[0])

    async def get_task_invocation(self, task_id: int):
        data = await self.get_task_invocation_serialized(task_id)
        if data is None:
            return None
        return await InvocationJob.deserialize_async(data)

    async def get_invocation_worker(self, invocation_id: int) -> Optional[AddressChain]:
        async with self.data_access.data_connection() as con:
            async with con.execute(
                    'SELECT workers.last_address '
                    'FROM invocations LEFT JOIN workers '
                    'ON invocations.worker_id == workers.id '
                    'WHERE invocations.id == ?', (invocation_id,)) as cur:
                res = await cur.fetchone()
        if res is None:
            return None
        return AddressChain(res[0])

    async def get_invocation_state(self, invocation_id: int) -> Optional[InvocationState]:
        async with self.data_access.data_connection() as con:
            async with con.execute(
                    'SELECT state FROM invocations WHERE id == ?', (invocation_id,)) as cur:
                res = await cur.fetchone()
        if res is None:
            return None
        return InvocationState(res[0])

    def stop(self):
        async def _server_closer():
            # ensure all components stop first
            await self.__pinger.wait_till_stops()
            await self.__task_processor.wait_till_stops()
            await self.__worker_pool.wait_till_stops()
            await self.__ui_server.wait_closed()
            if self.__server is not None:
                self.__server.close()
                await self.__server.wait_closed()
            self.__logger.debug('stopping message processor...')
            self.__message_processor.stop()
            await self.__message_processor.wait_till_stops()
            self.__logger.debug('message processor stopped')

        async def _db_cache_writeback():
            await self.__pinger.wait_till_stops()
            await self.__task_processor.wait_till_stops()
            await self.__server_closing_task
            await self._save_all_cached_nodes_to_db()
            await self.data_access.write_back_cache()

        if self.__stop_event.is_set():
            self.__logger.error('cannot double stop!')
            return  # no double stopping
        if not self.__started_event.is_set():
            self.__logger.error('cannot stop what is not started!')
            return
        self.__logger.info('STOPPING SCHEDULER')
        self.__stop_event.set()  # this will stop things including task_processor
        self.__pinger.stop()
        self.__task_processor.stop()
        self.ui_state_access.stop()
        self.__worker_pool.stop()
        self.__server_closing_task = asyncio.create_task(_server_closer())  # we ensure worker pool stops BEFORE server, so workers have chance to report back
        self.__cleanup_tasks = [asyncio.create_task(_db_cache_writeback())]
        if self.__ui_server is not None:
            self.__ui_server.close()

    def _stop_event_wait(self):  # TODO: this is currently being used by ui proto to stop long connections, but not used in task proto, but what if it'll also get long living connections?
        return self.__stop_event.wait()

    async def start(self):
        # prepare
        async with self.data_access.data_connection() as con:
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
                    self.data_access.mem_cache_workers_state[row['id']] = {k: row[k] for k in dict(row)}

        # start
        self.__server = await self.__server_coro
        self.__ui_server = await self.__ui_server_coro
        # start message processor
        self.__message_processor = SchedulerMessageProcessor(self, self.__message_address)
        await self.__message_processor.start()
        self.__worker_pool = WorkerPool(WorkerType.SCHEDULER_HELPER,
                                        minimal_idle_to_ensure=self.__worker_pool_helpers_minimal_idle_to_ensure,
                                        scheduler_address=self.server_message_address())
        await self.__worker_pool.start()
        #
        if self.__broadcasting_server_coro is not None:
            self.__broadcasting_server = await self.__broadcasting_server_coro
        await self.__task_processor.start()
        await self.__pinger.start()
        await self.ui_state_access.start()
        # run
        self.__all_components = \
              asyncio.gather(self.__task_processor.wait_till_stops(),
                             self.__pinger.wait_till_stops(),
                             self.ui_state_access.wait_till_stops(),
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
        for task in self.__cleanup_tasks:
            await task

    async def _save_all_cached_nodes_to_db(self):
        self.__logger.info('saving nodes to db')
        for node_id in self.__node_objects:
            await self.save_node_to_database(node_id)
            self.__logger.debug(f'node {node_id} saved to db')

    def is_started(self):
        return self.__started_event.is_set()

    def is_stopping(self) -> bool:
        """
        True if stopped or in process of stopping
        """
        return self.__stop_event.is_set()

    #
    # helper functions
    #

    async def reset_invocations_for_worker(self, worker_id: int, con: aiosqlite_overlay.ConnectionWithCallbacks, also_update_resources=True) -> bool:
        """

        :param worker_id:
        :param con:
        :param also_update_resources:
        :return: need commit?
        """
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
            con.add_after_commit_callback(self.ui_state_access.scheduler_reports_task_updated, invoc_row['task_id'])  # ui event
        if also_update_resources:
            need_commit = need_commit or await self._update_worker_resouce_usage(worker_id, connection=con)
        return need_commit

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
                   self.data_access.data_connection() as con:
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
            await self._update_worker_resouce_usage(invocation['worker_id'], connection=con)  # remove resource usage info
            tasks_to_wait = []
            if not self.__use_external_log:
                await con.execute('UPDATE invocations SET "stdout" = ?, "stderr" = ? WHERE "id" = ?',
                                  (stdout, stderr, task.invocation_id()))
            else:
                await con.execute('UPDATE invocations SET "log_external" = 1 WHERE "id" = ?',
                                  (task.invocation_id(),))
                tasks_to_wait.append(asyncio.create_task(self._save_external_logs(task.invocation_id(), stdout, stderr)))

            if task.invocation_id() in self.data_access.mem_cache_invocations:
                del self.data_access.mem_cache_invocations[task.invocation_id()]

            ui_task_delta = TaskDelta(invocation['task_id'])  # for ui event
            if task.finished_needs_retry():  # max retry count will be checked by task processor
                await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                  (TaskState.READY.value, invocation['task_id']))
                ui_task_delta.state = TaskState.READY  # for ui event
            elif task.finished_with_error():
                state_details = json.dumps({'message': f'see invocation #{invocation["id"]} log for details',
                                            'happened_at': TaskState.IN_PROGRESS.value,
                                            'type': 'invocation'})
                await con.execute('UPDATE tasks SET "state" = ?, "state_details" = ? WHERE "id" = ?',
                                  (TaskState.ERROR.value,
                                   state_details,
                                   invocation['task_id']))
                ui_task_delta.state = TaskState.ERROR  # for ui event
                ui_task_delta.state_details = state_details  # for ui event
            else:
                await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                  (TaskState.POST_WAITING.value, invocation['task_id']))
                ui_task_delta.state = TaskState.POST_WAITING  # for ui event

            con.add_after_commit_callback(self.ui_state_access.scheduler_reports_task_updated, ui_task_delta)  # ui event
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
                   self.data_access.data_connection() as con:
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
            if task.invocation_id() in self.data_access.mem_cache_invocations:
                del self.data_access.mem_cache_invocations[task.invocation_id()]

            await con.execute('UPDATE workers SET "state" = ? WHERE "id" = ?',
                              (WorkerState.IDLE.value, invocation['worker_id']))
            await self._update_worker_resouce_usage(invocation['worker_id'], connection=con)  # remove resource usage info
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
            con.add_after_commit_callback(self.ui_state_access.scheduler_reports_task_updated, TaskDelta(invocation['task_id'], state=TaskState.WAITING))  # ui event
            await con.commit()
            if len(tasks_to_wait) > 0:
                await asyncio.wait(tasks_to_wait)
        self.__logger.debug(f'cancelling task done {repr(task)}')
        self.wake()
        self.poke_task_processor()

    #
    # add new worker to db
    async def add_worker(self, addr: str, worker_type: WorkerType, worker_resources: WorkerResources, assume_active=True):  # TODO: all resource should also go here
        self.__logger.debug(f'worker reported added: {addr}')
        async with self.data_access.data_connection() as con:
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
                await self.reset_invocations_for_worker(worker_row['id'], con=con, also_update_resources=False)  # we update later
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
                self.data_access.mem_cache_workers_state[worker_id].update({'last_seen': tstamp,
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
                self.data_access.mem_cache_workers_state[worker_id] = {'last_seen': tstamp,
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
            await self._update_worker_resouce_usage(worker_id, hwid=worker_resources.hwid, connection=con)  # used resources are inited to none
            await con.commit()
        self.__logger.debug(f'finished worker reported added: {addr}')
        self.poke_task_processor()

    # TODO: add decorator that locks method from reentry or smth
    #  potentially a worker may report done while this works,
    #  or when scheduler picked worker and about to run this, which will lead to inconsistency warning
    #  NOTE!: so far it's always called from a STARTED transaction, so there should not be reentry possible
    #  But that is not enforced right now, easy to make mistake
    async def _update_worker_resouce_usage(self, worker_id: int, resources: Optional[dict] = None, *, hwid=None, connection: aiosqlite.Connection) -> bool:
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

        workers_resources = self.data_access.mem_cache_workers_resources
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
        async with self.data_access.data_connection() as con:
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
            async with con.execute('SELECT "id", task_id FROM invocations WHERE worker_id = ? AND ("state" = ? OR "state" = ?)',
                                   (wid, InvocationState.IN_PROGRESS.value, InvocationState.INVOKING.value)) as invcur:
                invocations = await invcur.fetchall()

            await con.execute('UPDATE workers SET "state" = ? WHERE "id" = ?', (WorkerState.OFF.value, wid))
            await con.executemany('UPDATE invocations SET state = ? WHERE "id" = ?', ((InvocationState.FINISHED.value, x["id"]) for x in invocations))
            await con.executemany('UPDATE tasks SET state = ? WHERE "id" = ?', ((TaskState.WAITING.value, x["task_id"]) for x in invocations))
            await self._update_worker_resouce_usage(wid, hwid=hwid, connection=con)
            del self.data_access.mem_cache_workers_resources[wid]  # remove from cache
            if len(invocations) > 0:
                con.add_after_commit_callback(self.ui_state_access.scheduler_reports_tasks_updated, [TaskDelta(x["task_id"], state=TaskState.WAITING) for x in invocations])  # ui event
            await con.commit()
        self.__logger.debug(f'finished worker reported stopped: {addr}')

    #
    # protocol related commands
    #
    #
    # cancel invocation
    async def cancel_invocation(self, invocation_id: str):
        self.__logger.debug(f'canceling invocation {invocation_id}')
        async with self.data_access.data_connection() as con:
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
        addr = AddressChain(worker['last_address'])

        # the logic is:
        # - we send the worker a signal to cancel invocation
        # - later worker sends task_cancel_reported, and we are happy
        # - but worker might be overloaded, broken or whatever and may never send it. and it can even finish task and send task_done_reported, witch we need to treat
        with WorkerControlClient.get_worker_control_client(addr, self.message_processor()) as client:  # type: WorkerControlClient
            await client.cancel_task()

        # oh no, we don't do that, we wait for worker to report task canceled.  await con.execute('UPDATE invocations SET "state" = ? WHERE "id" = ?', (InvocationState.FINISHED.value, invocation_id))

    #
    #
    async def cancel_invocation_for_task(self, task_id: int):
        self.__logger.debug(f'canceling invocation for task {task_id}')
        async with self.data_access.data_connection() as con:
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
        async with self.data_access.data_connection() as con:
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
            async with self.data_access.data_connection() as con:
                con.row_factory = aiosqlite.Row
                await con.execute('PRAGMA FOREIGN_KEYS = on')
                await con.execute('UPDATE tasks SET "node_id" = ? WHERE "id" = ?', (node_id, task_id))
                con.add_after_commit_callback(self.ui_state_access.scheduler_reports_task_updated, TaskDelta(task_id, node_id=node_id))  # ui event
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
        query = 'UPDATE tasks SET "state" = %d WHERE "id" = ?' % state.value
        #print('beep')
        async with self.data_access.data_connection() as con:
            for task_id in task_ids:
                await con.execute('BEGIN IMMEDIATE')
                async with con.execute('SELECT "state" FROM tasks WHERE "id" = ?', (task_id,)) as cur:
                    cur_state = await cur.fetchone()
                    if cur_state is None:
                        await con.rollback()
                        continue
                    cur_state = TaskState(cur_state[0])
                if cur_state in (TaskState.IN_PROGRESS, TaskState.GENERATING, TaskState.POST_GENERATING):
                    self.__logger.warning(f'forcing task out of state {cur_state} is not currently implemented')
                    await con.rollback()
                    continue

                await con.execute(query, (task_id,))
                #await con.executemany(query, ((x,) for x in task_ids))
                con.add_after_commit_callback(self.ui_state_access.scheduler_reports_task_updated, TaskDelta(task_id, state=state))  # ui event
                await con.commit()  # TODO: this can be optimized into a single transaction
        #print('boop')
        self.wake()
        self.poke_task_processor()

    #
    # change task's paused state
    async def set_task_paused(self, task_ids_or_group: Union[int, Iterable[int], str], paused: bool):
        if isinstance(task_ids_or_group, str):
            async with self.data_access.data_connection() as con:
                await con.execute('UPDATE tasks SET "paused" = ? WHERE "id" IN (SELECT "task_id" FROM task_groups WHERE "group" = ?)',
                                  (int(paused), task_ids_or_group))
                ui_task_ids = await self.ui_state_access._get_group_tasks(task_ids_or_group)  # ui event
                con.add_after_commit_callback(self.ui_state_access.scheduler_reports_tasks_updated, [TaskDelta(ui_task_id, paused=paused) for ui_task_id in ui_task_ids])  # ui event
                await con.commit()
            self.wake()
            self.poke_task_processor()
            return
        if isinstance(task_ids_or_group, int):
            task_ids_or_group = [task_ids_or_group]
        query = 'UPDATE tasks SET "paused" = %d WHERE "id" = ?' % int(paused)
        async with self.data_access.data_connection() as con:
            await con.executemany(query, ((x,) for x in task_ids_or_group))
            con.add_after_commit_callback(self.ui_state_access.scheduler_reports_tasks_updated, [TaskDelta(ui_task_id, paused=paused) for ui_task_id in task_ids_or_group])  # ui event
            await con.commit()
        self.wake()
        self.poke_task_processor()

    #
    # change task group archived state
    async def set_task_group_archived(self, task_group_name: str, state: TaskGroupArchivedState = TaskGroupArchivedState.ARCHIVED) -> None:
        async with self.data_access.data_connection() as con:
            con.row_factory = aiosqlite.Row
            await con.execute('UPDATE task_group_attributes SET state=? WHERE "group"==?', (state.value, task_group_name))  # this triggers all task deadness | 2, so potentially it can be long, beware
            # task's dead field's 2nd bit is set, but we currently do not track it
            # so no event needed
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
        async with self.data_access.data_connection() as con:
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
        async with self.data_access.data_connection() as con:
            await con.execute('UPDATE tasks SET "name" = ? WHERE "id" = ?', (new_name, task_id))
            con.add_after_commit_callback(self.ui_state_access.scheduler_reports_task_updated, TaskDelta(task_id, name=new_name))  # ui event
            await con.commit()

    #
    # set task groups
    async def set_task_groups(self, task_id: int, group_names: Iterable[str]):
        async with self.data_access.data_connection() as con:
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
                await con.execute('INSERT OR IGNORE INTO task_group_attributes ("group", "ctime") VALUES (?, ?)', (group_name, int(datetime.utcnow().timestamp())))
            for group_name in groups_to_del:
                await con.execute('DELETE FROM task_groups WHERE task_id = ? AND "group" = ?', (task_id, group_name))
            con.add_after_commit_callback(self.ui_state_access.scheduler_reports_tasks_removed_from_group, [task_id], groups_to_del)  # ui event
            con.add_after_commit_callback(self.ui_state_access.scheduler_reports_task_groups_changed, groups_to_set)  # ui event
            #
            # ui event
            if len(groups_to_set) > 0:
                async with con.execute(
                        'SELECT tasks.id, tasks.parent_id, tasks.children_count, tasks.active_children_count, tasks.state, tasks.state_details, tasks.paused, tasks.node_id, '
                        'tasks.node_input_name, tasks.node_output_name, tasks.name, tasks.split_level, tasks.work_data_invocation_attempt, '
                        'task_splits.origin_task_id, task_splits.split_id, invocations."id" as invoc_id '
                        'FROM "tasks" '
                        'LEFT JOIN "task_splits" ON tasks.id=task_splits.task_id '
                        'LEFT JOIN "invocations" ON tasks.id=invocations.task_id AND invocations.state = ? '
                        'WHERE tasks."id" == ?',
                        (InvocationState.IN_PROGRESS.value, task_id)) as cur:
                    task_row = await cur.fetchone()
                if task_row is not None:
                    progress = self.data_access.mem_cache_invocations.get(task_row['invoc_id'], {}).get('progress', None)
                    con.add_after_commit_callback(
                        self.ui_state_access.scheduler_reports_task_added,
                        TaskData(task_id, task_row['parent_id'], task_row['children_count'], task_row['active_children_count'], TaskState(task_row['state']),
                                 task_row['state_details'], bool(task_row['paused']), task_row['node_id'], task_row['node_input_name'], task_row['node_output_name'],
                                 task_row['name'], task_row['split_level'], task_row['work_data_invocation_attempt'], progress,
                                 task_row['origin_task_id'], task_row['split_id'], task_row['invoc_id'], group_names),
                        groups_to_set
                    )  # ui event
            con.add_after_commit_callback(self.ui_state_access.scheduler_reports_task_updated, TaskDelta(task_id, groups=group_names))  # ui event
            #
            #
            await con.commit()

    #
    # update task attributes
    async def update_task_attributes(self, task_id: int, attributes_to_update: dict, attributes_to_delete: set):
        async with self.data_access.data_connection() as con:
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
        async with self.data_access.data_connection() as con:
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
        async with self.data_access.data_connection() as con:
            await con.execute('UPDATE "nodes" SET "name" = ? WHERE "id" = ?', (node_name, node_id))
            if node_id in self.__node_objects:
                self.__node_objects[node_id].set_name(node_name)
            await con.commit()
            self.ui_state_access.bump_graph_update_id()
        return node_name

    #
    # reset node's stored state
    async def wipe_node_state(self, node_id):
        async with self.data_access.data_connection() as con:
            await con.execute('UPDATE "nodes" SET node_object = NULL WHERE "id" = ?', (node_id,))
            if node_id in self.__node_objects:
                # TODO: this below may be not safe (at least not proven to be safe yet, but maybe). check
                del self.__node_objects[node_id]  # it's here to "protect" operation within db transaction. TODO: but a proper __node_object lock should be in place instead
            await con.commit()
            self.ui_state_access.bump_graph_update_id()  # not sure if needed - even number of inputs/outputs is not part of graph description
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
        async with self.data_access.data_connection() as con:
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
        async with self.data_access.data_connection() as con:
            await con.execute('UPDATE "nodes" SET node_object = ? WHERE "id" = ?',
                              (await node_object.serialize_async(), node_id))
            await con.commit()

    #
    # set worker groups
    async def set_worker_groups(self, worker_hwid: int, groups: List[str]):
        groups = set(groups)
        async with self.data_access.data_connection() as con:
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
        async with self.data_access.data_connection() as con:
            con.row_factory = aiosqlite.Row
            vals.append(node_connection_id)
            await con.execute(f'UPDATE node_connections SET {", ".join(parts)} WHERE "id" = ?', vals)
            await con.commit()
        self.wake()
        self.ui_state_access.bump_graph_update_id()

    #
    # add node connection callback
    async def add_node_connection(self, out_node_id: int, out_name: str, in_node_id: int, in_name: str) -> int:
        async with self.data_access.data_connection() as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('INSERT OR REPLACE INTO node_connections (node_id_out, out_name, node_id_in, in_name) VALUES (?,?,?,?)',  # INSERT OR REPLACE here (and not OR ABORT or smth) to ensure lastrowid is set
                                   (out_node_id, out_name, in_node_id, in_name)) as cur:
                ret = cur.lastrowid
            await con.commit()
            self.wake()
            self.ui_state_access.bump_graph_update_id()
            return ret

    #
    # remove node connection callback
    async def remove_node_connection(self, node_connection_id: int):
        try:
            async with self.data_access.data_connection() as con:
                con.row_factory = aiosqlite.Row
                await con.execute('PRAGMA FOREIGN_KEYS = on')
                await con.execute('DELETE FROM node_connections WHERE "id" = ?', (node_connection_id,))
                await con.commit()
                self.ui_state_access.bump_graph_update_id()
        except aiosqlite.IntegrityError as e:
            self.__logger.error('could not remove node connection because of database integrity check')

    #
    # add node
    async def add_node(self, node_type: str, node_name: str) -> int:
        if node_type not in pluginloader.plugins:
            raise RuntimeError('unknown node type')
        async with self.data_access.data_connection() as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('INSERT INTO "nodes" ("type", "name") VALUES (?,?)',
                                   (node_type, node_name)) as cur:
                ret = cur.lastrowid
            await con.commit()
            self.ui_state_access.bump_graph_update_id()
            return ret

    async def remove_node(self, node_id: int) -> bool:
        try:
            async with self.data_access.data_connection() as con:
                con.row_factory = aiosqlite.Row
                await con.execute('PRAGMA FOREIGN_KEYS = on')
                await con.execute('DELETE FROM "nodes" WHERE "id" = ?', (node_id,))
                await con.commit()
                self.ui_state_access.bump_graph_update_id()
        except aiosqlite.IntegrityError as e:
            self.__logger.error('could not remove node connection because of database integrity check')
            return False
        return True

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
        async with self.data_access.data_connection() as con:
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
    async def spawn_tasks(self, newtasks: Union[Iterable[TaskSpawn], TaskSpawn], con: Optional[aiosqlite_overlay.ConnectionWithCallbacks] = None) -> Union[Tuple[SpawnStatus, Optional[int]], Tuple[Tuple[SpawnStatus, Optional[int]], ...]]:
        """

        :param newtasks:
        :param con:
        :return:
        """

        async def _inner_shit() -> Tuple[Tuple[SpawnStatus, Optional[int]], ...]:
            result = []
            new_tasks = []
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
                    result.append((SpawnStatus.FAILED, None))
                    continue

                async with con.execute('INSERT INTO tasks ("name", "attributes", "parent_id", "state", "node_id", "node_output_name", "environment_resolver_data") VALUES (?, ?, ?, ?, ?, ?, ?)',
                                       (newtask.name(), json.dumps(newtask._attributes()), parent_task_id,  # TODO: run dumps in executor
                                        TaskState.SPAWNED.value if newtask.create_as_spawned() else TaskState.WAITING.value,
                                        node_id, newtask.node_output_name(),
                                        newtask.environment_arguments().serialize() if newtask.environment_arguments() is not None else None)) as newcur:
                    new_id = newcur.lastrowid

                all_groups = set()
                if parent_task_id is not None:  # inherit all parent's groups
                    # check and inherit parent's environment wrapper arguments
                    if newtask.environment_arguments() is None:
                        await con.execute('UPDATE tasks SET environment_resolver_data = (SELECT environment_resolver_data FROM tasks WHERE "id" == ?) WHERE "id" == ?',
                                          (parent_task_id, new_id))

                    # inc children count happens in db trigger
                    # inherit groups
                    async with con.execute('SELECT "group" FROM task_groups WHERE "task_id" = ?', (parent_task_id,)) as gcur:
                        groups = [x['group'] for x in await gcur.fetchall()]
                    all_groups.update(groups)
                    if len(groups) > 0:
                        con.add_after_commit_callback(self.ui_state_access.scheduler_reports_task_groups_changed, groups)  # ui event
                        await con.executemany('INSERT INTO task_groups ("task_id", "group") VALUES (?, ?)',
                                              zip(itertools.repeat(new_id, len(groups)), groups))
                else:  # parent_task_id is None
                    # in this case we create a default group for the task.
                    # task should not be left without groups at all - otherwise it will be impossible to find in UI
                    new_group = '{name}#{id:d}'.format(name=newtask.name(), id=new_id)
                    all_groups.add(new_group)
                    await con.execute('INSERT INTO task_groups ("task_id", "group") VALUES (?, ?)',
                                      (new_id, new_group))
                    await con.execute('INSERT OR REPLACE INTO task_group_attributes ("group", "ctime") VALUES (?, ?)',
                                      (new_group, current_timestamp))
                    if newtask.default_priority() is not None:
                        await con.execute('UPDATE task_group_attributes SET "priority" = ? WHERE "group" = ?',
                                          (newtask.default_priority(), new_group))
                    con.add_after_commit_callback(self.ui_state_access.scheduler_reports_task_groups_changed, (new_group,))  # ui event
                    #
                if newtask.extra_group_names():
                    groups = newtask.extra_group_names()
                    all_groups.update(groups)
                    await con.executemany('INSERT INTO task_groups ("task_id", "group") VALUES (?, ?)',
                                          zip(itertools.repeat(new_id, len(groups)), groups))
                    con.add_after_commit_callback(self.ui_state_access.scheduler_reports_task_groups_changed, groups)  # ui event
                    for group in groups:
                        async with con.execute('SELECT "group" FROM task_group_attributes WHERE "group" == ?', (group,)) as gcur:
                            need_create = await gcur.fetchone() is None
                        if not need_create:
                            continue
                        await con.execute('INSERT INTO task_group_attributes ("group", "ctime") VALUES (?, ?)',
                                          (group, current_timestamp))
                        # TODO: task_groups.group should be a foreign key to task_group_attributes.group
                        #  but then we need to insert those guys in correct order (first in attributes table, then groups)
                        #  then smth like FOREIGN KEY("group") REFERENCES "task_group_attributes"("group") ON UPDATE CASCADE ON DELETE CASCADE
                result.append((SpawnStatus.SUCCEEDED, new_id))
                new_tasks.append(TaskData(new_id, parent_task_id, 0, 0,
                                          TaskState.SPAWNED if newtask.create_as_spawned() else TaskState.WAITING, '',
                                          False, node_id, 'main', newtask.node_output_name(), newtask.name(), 0, 0, None, None, None, None,
                                          all_groups))

            # callbacks for ui events
            con.add_after_commit_callback(self.ui_state_access.scheduler_reports_tasks_added, new_tasks)
            return tuple(result)

        return_single = False
        if isinstance(newtasks, TaskSpawn):
            newtasks = (newtasks,)
            return_single = True
        if con is not None:
            stuff = await _inner_shit()
        else:
            async with self.data_access.data_connection() as con:
                con.row_factory = aiosqlite.Row
                stuff = await _inner_shit()
                await con.commit()
        self.wake()
        self.poke_task_processor()
        return stuff[0] if return_single else stuff

    #
    async def node_name_to_id(self, name: str) -> List[int]:
        """
        get the list of node ids that have specified name
        :param name:
        :return:
        """
        async with self.data_access.data_connection() as con:
            async with con.execute('SELECT "id" FROM "nodes" WHERE "name" = ?', (name,)) as cur:
                return list(x[0] for x in await cur.fetchall())

    #
    async def get_invocation_metadata(self, task_id: int) -> Dict[int, List[IncompleteInvocationLogData]]:
        """
        get task's log metadata - meaning which nodes it ran on and how
        :param task_id:
        :return: dict[node_id -> list[IncompleteInvocationLogData]]
        """
        async with self.data_access.data_connection() as con:
            con.row_factory = aiosqlite.Row
            logs = {}
            self.__logger.debug(f'fetching log metadata for {task_id}')
            async with con.execute('SELECT "id", node_id, runtime, worker_id from "invocations" WHERE "task_id" = ?',
                                   (task_id, )) as cur:
                async for entry in cur:
                    node_id = entry['node_id']
                    logs.setdefault(node_id, []).append(IncompleteInvocationLogData(
                        entry['id'],
                        entry['worker_id'],
                        entry['runtime']
                    ))
            return logs

    async def get_log(self, invocation_id: int) -> Optional[InvocationLogData]:
        """
        get logs for given task, node and invocation ids

        returns a dict of node_id

        :param invocation_id:
        :return:
        """
        async with self.data_access.data_connection() as con:
            con.row_factory = aiosqlite.Row
            self.__logger.debug(f"fetching for {invocation_id}")
            async with con.execute('SELECT "id", task_id, worker_id, node_id, state, return_code, log_external, runtime, stdout, stderr '
                                   'FROM "invocations" WHERE "id" = ?',
                                   (invocation_id,)) as cur:
                rawentry = await cur.fetchone()  # should be exactly 1 or 0
            if rawentry is None:
                return None

            entry: InvocationLogData = InvocationLogData(rawentry['id'],
                                                         rawentry['worker_id'],
                                                         rawentry['runtime'],
                                                         rawentry['task_id'],
                                                         rawentry['node_id'],
                                                         InvocationState(rawentry['state']),
                                                         rawentry['return_code'],
                                                         rawentry['stdout'] or '',
                                                         rawentry['stderr'] or '')
            if entry.invocation_state == InvocationState.IN_PROGRESS:
                async with con.execute('SELECT last_address FROM workers WHERE "id" = ?', (entry.worker_id,)) as worcur:
                    workrow = await worcur.fetchone()
                if workrow is None:
                    self.__logger.error('Worker not found during log fetch! this is not supposed to happen! Database inconsistent?')
                else:
                    try:
                        with WorkerControlClient.get_worker_control_client(AddressChain(workrow['last_address']), self.message_processor()) as client:  # type: WorkerControlClient
                            stdout, stderr = await client.get_log(invocation_id)
                        if not self.__use_external_log:
                            await con.execute('UPDATE "invocations" SET stdout = ?, stderr = ? WHERE "id" = ?',  # TODO: is this really needed? if it's never really read
                                              (stdout, stderr, invocation_id))
                            await con.commit()
                        # TODO: maybe add else case? save partial log to file?
                    except ConnectionError:
                        self.__logger.warning('could not connect to worker to get freshest logs')
                    else:
                        entry.stdout = stdout
                        entry.stderr = stderr

            elif entry.invocation_state == InvocationState.FINISHED and rawentry['log_external'] == 1:
                logbasedir = self.__external_log_location / 'invocations' / f'{invocation_id}'
                stdout_path = logbasedir / 'stdout.log'
                stderr_path = logbasedir / 'stderr.log'
                try:
                    if stdout_path.exists():
                        async with aiofiles.open(stdout_path, 'r') as fstdout:
                            entry.stdout = await fstdout.read()
                except IOError:
                    self.__logger.exception(f'could not read external stdout log for {invocation_id}')
                try:
                    if stderr_path.exists():
                        async with aiofiles.open(stderr_path, 'r') as fstderr:
                            entry.stderr = await fstderr.read()
                except IOError:
                    self.__logger.exception(f'could not read external stdout log for {invocation_id}')

        return entry

    def server_address(self) -> str:
        return self.__server_address

    def server_message_address(self) -> AddressChain:
        if self.__message_processor is None:
            raise RuntimeError('cannot get listening address of a non started server')
        return self.message_processor().listening_address()
