import sys
import os
import traceback
import time
from datetime import datetime
import json
import itertools
from enum import Enum
import asyncio
import aiosqlite
import signal

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
from .worker_pool import WorkerPool
from .nethelpers import address_to_ip_port, get_default_addr, get_default_broadcast_addr
from .net_classes import WorkerResources
from .taskspawn import TaskSpawn
from .basenode import BaseNode
from .nodethings import ProcessingResult
from .exceptions import *
from . import pluginloader
from .enums import WorkerState, WorkerPingState, TaskState, InvocationState, WorkerType, SchedulerMode
from .config import get_config, create_default_user_config_file
from .misc import atimeit

from typing import Optional, Any, AnyStr, List, Iterable, Union, Dict


class Scheduler:
    def __init__(self, db_file_path, *, do_broadcasting=True, helpers_minimal_idle_to_ensure=1):
        self.__logger = logging.get_logger('scheduler')
        self.__pinger_logger = logging.get_logger('scheduler.worker_pinger')
        self.__logger.info('loading core plugins')
        pluginloader.init()  # TODO: move it outside of constructor
        self.__node_objects: Dict[int, BaseNode] = {}
        # self.__plugins = {}
        # core_plugins_path = os.path.join(os.path.dirname(__file__), 'core_nodes')
        # for filename in os.listdir(core_plugins_path):
        #     filebasename, fileext = os.path.splitext(filename)
        #     if fileext != '.py':
        #         continue
        #     mod_spec = importlib.util.spec_from_file_location(f'taskflow.coreplugins.{filebasename}',
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
        self.__ping_interval_mult = 1
        self.__processing_interval = 0.5
        self.__processing_interval_mult = 1
        self.__db_lock_timeout = 30
        self.__invocation_attempts = config.get_option_noasync('invocation.default_attempts', 3)  # TODO: config should be directly used when needed to allow dynamically reconfigure running scheduler

        self.__mode = SchedulerMode.STANDARD

        self.__all_components = None
        self.__started_event = asyncio.Event()
        
        loop = asyncio.get_event_loop()
        self.db_path = db_file_path

        server_ip = config.get_option_noasync('core.server_ip', get_default_addr())
        server_port = config.get_option_noasync('core.server_port', 7979)
        ui_ip = config.get_option_noasync('core.ui_ip', get_default_addr())
        ui_port = config.get_option_noasync('core.ui_port', 7989)
        self.__stop_event = asyncio.Event()
        self.__wakeup_event = asyncio.Event()
        self.__wakeup_event.set()
        self.__server = None
        self.__server_coro = loop.create_server(self.scheduler_protocol_factory, server_ip, server_port, backlog=16)
        self.__server_address = ':'.join((server_ip, str(server_port)))
        self.__ui_server = None
        self.__ui_server_coro = loop.create_server(self.ui_protocol_factory, ui_ip, ui_port, backlog=16)
        self.__ui_address = ':'.join((ui_ip, str(ui_port)))
        if do_broadcasting:
            broadcast_info = json.dumps({'worker': self.__server_address, 'ui': self.__ui_address})
            self.__broadcasting_server = None
            self.__broadcasting_server_coro = create_broadcaster('taskflow_scheduler', broadcast_info, ip=get_default_broadcast_addr())
        else:
            self.__broadcasting_server = None
            self.__broadcasting_server_coro = None

        self.__worker_pool = WorkerPool(WorkerType.SCHEDULER_HELPER, minimal_idle_to_ensure=helpers_minimal_idle_to_ensure, scheduler_address=(server_ip, server_port))

        self.__event_loop = asyncio.get_running_loop()
        assert self.__event_loop is not None, 'Scheduler MUST be created within working event loop, in the main thread'

    def get_event_loop(self):
        return self.__event_loop

    def scheduler_protocol_factory(self):
        return SchedulerTaskProtocol(self)

    def ui_protocol_factory(self):
        return SchedulerUiProtocol(self)

    def wake(self):
        if self.__mode == SchedulerMode.DORMANT:
            self.__logger.info('exiting DORMANT mode. mode is STANDARD now')
            self.__mode = SchedulerMode.STANDARD
            self.__processing_interval_mult = 1
            self.__ping_interval_mult = 1
            self.__wakeup_event.set()

    def __sleep(self):
        if self.__mode == SchedulerMode.STANDARD:
            self.__logger.info('entering DORMANT mode')
            self.__mode = SchedulerMode.DORMANT
            self.__processing_interval_mult = 50
            self.__ping_interval_mult = 10
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

    async def get_node_object_by_id(self, node_id: int) -> BaseNode:
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

    async def get_task_attributes(self, task_id: int):
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT attributes FROM tasks WHERE "id" = ?', (task_id,)) as cur:
                res = await cur.fetchone()
            if res is None:
                raise RuntimeError('task with specified id was not found')
            return await asyncio.get_event_loop().run_in_executor(None, json.loads, res['attributes'])

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
        if self.__stop_event.is_set():
            self.__logger.error('cannot double stop!')
            return  # no double stopping
        if not self.__started_event.is_set():
            self.__logger.error('cannot stop what is not started!')
            return
        self.__logger.info('STOPPING SCHEDULER')
        self.__stop_event.set()
        if self.__server is not None:
            self.__server.close()
        if self.__ui_server is not None:
            self.__ui_server.close()
        self.__worker_pool.stop()

    def _stop_event_wait(self):
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

    def is_started(self):
        return self.__started_event.is_set()

    #
    # helper functions
    #
    async def set_worker_ping_state(self, wid: int, state: WorkerPingState, con: Optional[aiosqlite.Connection] = None, nocommit: bool = False) -> None:
        await self._set_value('workers', 'ping_state', wid, state.value, con, nocommit)

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

    async def update_worker_lastseen(self, wid: int, con: Optional[aiosqlite.Connection] = None, nocommit: bool = False):
        await self._set_value('workers', 'last_seen', wid, int(time.time()), con, nocommit)

    async def reset_invocations_for_worker(self, worker_id: int, con: Optional[aiosqlite.Connection] = None):
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

            async def _check_lastseen_and_drop_invocations(switch_state_on_reset: Optional[WorkerState] = None):
                if worker_row['last_seen'] is not None and time.time() - worker_row['last_seen'] < 64:  # TODO: make this time a configurable parameter
                    return False
                if switch_state_on_reset is not None:
                    await self.set_worker_state(worker_row['id'], switch_state_on_reset, con, nocommit=True)
                return await self.reset_invocations_for_worker(worker_row['id'], con)

            self.__pinger_logger.debug('    :: pinger started')
            await self.set_worker_ping_state(worker_row['id'], WorkerPingState.CHECKING, con, nocommit=True)
            await self._set_value('workers', 'last_checked', worker_row['id'], int(time.time()), con, nocommit=True)
            await con.commit()

            addr = worker_row['last_address']
            ip, port = addr.split(':')  # type: str, str
            self.__pinger_logger.debug('    :: checking %s, %s', ip, port)

            if not port.isdigit():
                self.__pinger_logger.debug('    :: malformed address')
                await asyncio.gather(
                    self.set_worker_ping_state(worker_row['id'], WorkerPingState.ERROR, con, nocommit=True),
                    self.set_worker_state(worker_row['id'], WorkerState.ERROR, con, nocommit=True)
                )
                await _check_lastseen_and_drop_invocations()
                await con.commit()
                return
            try:
                async with WorkerTaskClient(ip, int(port), timeout=15) as client:
                    ping_code, pvalue = await client.ping()
            except asyncio.exceptions.TimeoutError:
                self.__pinger_logger.debug('    :: network error')
                await self.set_worker_ping_state(worker_row['id'], WorkerPingState.ERROR, con, nocommit=True)
                await _check_lastseen_and_drop_invocations(switch_state_on_reset=WorkerState.ERROR)
                await con.commit()
                return
            except ConnectionRefusedError as e:
                self.__pinger_logger.debug('    :: host down %s', str(e))
                await self.set_worker_ping_state(worker_row['id'], WorkerPingState.OFF, con, nocommit=True)
                await _check_lastseen_and_drop_invocations(switch_state_on_reset=WorkerState.OFF)
                await con.commit()
                return
            except Exception as e:
                self.__pinger_logger.debug('    :: ping failed %s %s', type(e), e)
                await self.set_worker_ping_state(worker_row['id'], WorkerPingState.ERROR, con, nocommit=True)
                await _check_lastseen_and_drop_invocations(switch_state_on_reset=WorkerState.OFF)
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
            else:
                #workerstate = WorkerState.BUSY  # in this case received pvalue is current task's progress. u cannot rely on it's precision: some invocations may not support progress reporting
                # TODO: think, can there be race condition here so that worker is already doing something else?
                await con.execute('UPDATE "invocations" SET "progress" = ? WHERE "state" = ? AND "worker_id" = ?', (pvalue, InvocationState.IN_PROGRESS.value, worker_row['id']))

            await asyncio.gather(self.set_worker_ping_state(worker_row['id'], WorkerPingState.WORKING, con, nocommit=True),
                                 #self.set_worker_state(worker_row['id'], workerstate, con, nocommit=True),
                                 self.update_worker_lastseen(worker_row['id'], con, nocommit=True)
                                 )
            await con.commit()
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

        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:  # TODO: is it good to keep con always open?
            con.row_factory = aiosqlite.Row

            tasks = []
            stop_task = asyncio.create_task(self.__stop_event.wait())
            wakeup_task = None
            while not self.__stop_event.is_set():
                nowtime = time.time()

                self.__pinger_logger.debug('    ::selecting workers...')
                async with con.execute("SELECT * from workers WHERE ping_state != ?", (WorkerPingState.CHECKING.value,)) as cur:
                    # TODO: don't scan the errored and off ones as often?

                    async for row in cur:
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

        self.__logger.info('finishing worker pinger...')
        if len(tasks) > 0:
            self.__logger.debug(f'waiting for {len(tasks)} pinger tasks...')
            _, pending = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED, timeout=5)
            self.__logger.debug(f'waiting enough, cancelling {len(pending)} tasks')
            for task in pending:
                task.cancel()
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
                process_result: ProcessingResult = await loop.run_in_executor(None, processor_to_run, task_row)  # TODO: this should have task and node attributes!
            except NodeNotReadyToProcess:
                async with awaiter_lock, aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
                    await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                      (abort_state.value, task_id))
                    await con.commit()
                return
            except Exception as e:
                async with awaiter_lock, aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
                    await con.execute('UPDATE tasks SET "state" = ?, "state_details" = ? WHERE "id" = ?',
                                      (TaskState.ERROR.value,
                                       json.dumps({'message': traceback.format_exc(),
                                                   'happened_at': task_row['state'],
                                                   'type': 'exception',
                                                   'exception_str': str(e),
                                                   'exception_type': str(type(e))})
                                       , task_id))
                    await con.commit()
                    self.__logger.exception('error happened %s %s', type(e), e)
                return

            # why is there lock? it looks locking manually is waaaay more efficient than relying on transaction locking
            async with awaiter_lock, aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
                con.row_factory = aiosqlite.Row
                # This implicitly starts transaction
                #print(f'up till block: {time.perf_counter() - _blo}')
                if process_result.output_name:
                    await con.execute('UPDATE tasks SET "node_output_name" = ? WHERE "id" = ?',
                                      (process_result.output_name, task_id))
                #_blo = time.perf_counter()
                #_bla1 = time.perf_counter()
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
                        await con.execute('UPDATE tasks SET "work_data" = ?, "work_data_invocation_attempt" = 0, "state" = ?, "_invoc_requirement_clause" = ? '
                                          'WHERE "id" = ?',
                                          (taskdada_serialized, TaskState.READY.value, invoc_requirements_sql,
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
                #print(f'attset: {time.perf_counter() - _bla1}')
                #_bla1 = time.perf_counter()
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
                        split_task_attrs = json.loads(split_task_dict['attributes'])
                        split_task_attrs.update(attr_dict)
                        await con.execute('UPDATE "tasks" SET attributes = ? WHERE "id" = ?', (json.dumps(split_task_attrs), split_task_id))
                #print(f'split: {time.perf_counter()-_bla1}')

                #_precum = time.perf_counter()-_blo
                await con.commit()
                #print(f'_awaiter trans: {_precum} - {time.perf_counter()-_blo}')

        # submitter
        @atimeit()
        async def _submitter(task_row, worker_row):
            addr = worker_row['last_address']
            try:
                ip, port = addr.split(':')
                port = int(port)
            except:
                self.__logger.error('error addres converting during unexpected here. ping should have cought it')
                return

            work_data = task_row['work_data']
            assert work_data is not None
            task: InvocationJob = await asyncio.get_event_loop().run_in_executor(None, InvocationJob.deserialize, work_data)
            if not task.args():
                async with awaiter_lock, aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as skipwork_transaction:
                    await skipwork_transaction.execute('UPDATE tasks SET state = ? WHERE "id" = ?',
                                                       (TaskState.POST_WAITING.value, task_row['id']))
                    await skipwork_transaction.execute('UPDATE workers SET state = ? WHERE "id" = ?',
                                                       (WorkerState.IDLE.value, worker_row['id']))
                    await skipwork_transaction.commit()
                    return

            # so task.args() is not None
            async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as submit_transaction:
                async with awaiter_lock:
                    async with submit_transaction.execute(
                            'INSERT INTO invocations ("task_id", "worker_id", "state", "node_id") VALUES (?, ?, ?, ?)',
                            (task_row['id'], worker_row['id'], InvocationState.INVOKING.value, task_row['node_id'])) as incur:
                        invocation_id = incur.lastrowid  # rowid should be an alias to id, acc to sqlite manual
                    await submit_transaction.commit()

                task._set_invocation_id(invocation_id)
                task._set_task_attributes(json.loads(task_row['attributes']))
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
                    if reply == TaskScheduleStatus.SUCCESS:
                        await submit_transaction.execute('UPDATE tasks SET state = ?, '
                                                         '"work_data_invocation_attempt" = "work_data_invocation_attempt" + 1 '
                                                         'WHERE "id" = ?',
                                                         (TaskState.IN_PROGRESS.value, task_row['id']))
                        await submit_transaction.execute('UPDATE workers SET state = ? WHERE "id" = ?',
                                                         (WorkerState.BUSY.value, worker_row['id']))
                        await submit_transaction.execute('UPDATE invocations SET state = ? WHERE "id" = ?',
                                                         (InvocationState.IN_PROGRESS.value, invocation_id))
                        await submit_transaction.commit()
                    else:  # on anything but success - cancel transaction
                        await submit_transaction.execute('UPDATE tasks SET state = ? WHERE "id" = ?',
                                                         (TaskState.READY.value, task_row['id']))
                        await submit_transaction.execute('UPDATE workers SET state = ? WHERE "id" = ?',
                                                         (WorkerState.IDLE.value, worker_row['id']))
                        await submit_transaction.execute('DELETE FROM invocations WHERE "id" = ?',
                                                         (invocation_id,))
                        await submit_transaction.commit()

        # this will hold references to tasks created with asyncio.create_task
        tasks_to_wait = set()
        stop_task = asyncio.create_task(self.__stop_event.wait())
        wakeup_task = None
        while not self.__stop_event.is_set():

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
            async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
                con.row_factory = aiosqlite.Row

                for task_state in (TaskState.WAITING, TaskState.READY, TaskState.DONE, TaskState.POST_WAITING, TaskState.SPAWNED):
                    _debug_sel = time.perf_counter()
                    async with con.execute('SELECT tasks.*, nodes.type as node_type, nodes.name as node_name, nodes.id as node_id, '
                                           'task_splits.split_id as split_id, task_splits.split_element as split_element, task_splits.split_count as split_count, task_splits.origin_task_id as split_origin_task_id '
                                           'FROM tasks INNER JOIN nodes ON tasks.node_id=nodes.id '
                                           'LEFT JOIN task_splits ON tasks.id=task_splits.task_id '
                                           'WHERE (state = ?) '
                                           'AND paused = 0 '
                                           'ORDER BY RANDOM()',
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
                    #
                    # waiting to be processed
                    if task_state == TaskState.WAITING:
                        awaiters = []
                        for task_row in all_task_rows:
                            if task_row['node_type'] not in pluginloader.plugins:
                                self.__logger.error(f'plugin to process "{task_row["node_type"]}" not found!')
                                await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                                  (TaskState.ERROR.value, task_row['id']))
                            else:
                                if not (await self.get_node_object_by_id(task_row['node_id'])).ready_to_process_task(task_row):
                                    continue

                                await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                                  (TaskState.GENERATING.value, task_row['id']))

                                awaiters.append(_awaiter((await self.get_node_object_by_id(task_row['node_id']))._process_task_wrapper, dict(task_row),
                                                         abort_state=TaskState.WAITING, skip_state=TaskState.POST_WAITING))
                        await con.commit()
                        for coro in awaiters:
                            tasks_to_wait.add(asyncio.create_task(coro))
                    #
                    # waiting to be post processed
                    elif task_state == TaskState.POST_WAITING:
                        awaiters = []
                        for task_row in all_task_rows:
                            if task_row['node_type'] not in pluginloader.plugins:
                                self.__logger.error(f'plugin to process "{task_row["node_type"]}" not found!')
                                await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                                  (TaskState.ERROR.value, task_row['id']))
                            else:
                                if not (await self.get_node_object_by_id(task_row['node_id'])).ready_to_postprocess_task(task_row):
                                    continue

                                await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                                  (TaskState.POST_GENERATING.value, task_row['id']))

                                awaiters.append(_awaiter((await self.get_node_object_by_id(task_row['node_id']))._postprocess_task_wrapper, dict(task_row),
                                                         abort_state=TaskState.POST_WAITING, skip_state=TaskState.DONE))
                        await con.commit()
                        for coro in awaiters:
                            tasks_to_wait.add(asyncio.create_task(coro))
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
                                self.__logger.warning(f'{task_row["id"]} reached maximum invocation attempts, setting it to error state')
                                continue
                            #
                            if task_row["_invoc_requirement_clause"] in where_empty_cache:
                                continue
                            try:
                                async with con.execute(f'SELECT * from workers WHERE state == ? AND ( {task_row["_invoc_requirement_clause"]} )', (WorkerState.IDLE.value,)) as worcur:
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
                                self.__logger.exception(f'error matching workers for the task {task_row["id"]}')
                                continue
                            if worker is None:  # nothing available
                                where_empty_cache.add(task_row["_invoc_requirement_clause"])
                                continue

                            await con.execute('UPDATE tasks SET state = ? WHERE "id" = ?',
                                                                (TaskState.INVOKING.value, task_row['id']))
                            await con.execute('UPDATE workers SET state = ? WHERE "id" = ?',
                                                                (WorkerState.INVOKING.value, worker['id']))

                            submitters.append(_submitter(dict(task_row), dict(worker)))
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
                                else:
                                    for i, splited_task_id in enumerate(await self.split_task(task_row['id'], wire_count, con)):
                                        await con.execute('UPDATE tasks SET node_id = ?, node_input_name = ?, state = ?, work_data = ?'
                                                          'WHERE "id" = ?',
                                                          (all_wires[i]['node_id_in'], all_wires[i]['in_name'], TaskState.WAITING.value, None,
                                                           splited_task_id))

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
                        async with con.execute('SELECT COUNT(id) AS total FROM tasks WHERE paused = 0 AND state NOT IN (?, ?)', (TaskState.ERROR.value, TaskState.DEAD.value)) as cur:
                            total = await cur.fetchone()
                            if total is None or total['total'] == 0:
                                self.__logger.info('no useful tasks seem to be available')
                                self.__sleep()
                else:
                    self.wake()

            self.__logger.debug(f'processing run in {time.perf_counter() - _debug_con}')

            # and wait for a bit
            if wakeup_task is not None:
                sleeping_tasks = (stop_task, wakeup_task)
            else:
                if self.__mode == SchedulerMode.DORMANT:
                    wakeup_task = asyncio.create_task(self.__wakeup_event.wait())
                    sleeping_tasks = (stop_task, wakeup_task)
                else:
                    sleeping_tasks = (stop_task,)
            wdone, _ = await asyncio.wait(sleeping_tasks, timeout=self.__processing_interval * self.__processing_interval_mult, return_when=asyncio.FIRST_COMPLETED)
            if wakeup_task is not None and wakeup_task in wdone:
                wakeup_task = None
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
        self.__logger.debug('task finished reported %s code %s', repr(task), task.exit_code())
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            # sanity check
            async with con.execute('SELECT "state" FROM invocations WHERE "id" = ?', (task.invocation_id(),)) as cur:
                invoc = await cur.fetchone()
                if invoc is None:
                    self.__logger.error('reported task has non existing invocation id %d' % task.invocation_id())
                    return
                if invoc['state'] != InvocationState.IN_PROGRESS.value:
                    self.__logger.warning('reported task for a finished invocation. assuming that worker failed to cancel task previously and ignoring invocation results.')
                    return
            await con.execute('UPDATE invocations SET "state" = ?, "return_code" = ? WHERE "id" = ?',
                              (InvocationState.FINISHED.value, task.exit_code(), task.invocation_id()))
            async with con.execute('SELECT * FROM invocations WHERE "id" = ?', (task.invocation_id(),)) as incur:
                invocation = await incur.fetchone()
            assert invocation is not None

            await con.execute('UPDATE workers SET "state" = ? WHERE "id" = ?',
                              (WorkerState.IDLE.value, invocation['worker_id']))
            await con.execute('UPDATE invocations SET "stdout" = ?, "stderr" = ? WHERE "id" = ?',
                              (stdout, stderr, task.invocation_id()))

            if task.finished_needs_retry():  # TODO: max retry count!
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
        self.wake()

    #
    # worker reports canceled task
    async def task_cancel_reported(self, task: InvocationJob, stdout: str, stderr: str):
        self.__logger.debug('task cancelled reported %s', repr(task))
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            # sanity check
            async with con.execute('SELECT "state" FROM invocations WHERE "id" = ?', (task.invocation_id(),)) as cur:
                invoc = await cur.fetchone()
                if invoc is None:
                    self.__logger.error('reported task has non existing invocation id %d' % task.invocation_id())
                    return
                if invoc['state'] != InvocationState.IN_PROGRESS.value:
                    self.__logger.warning('reported task for a finished invocation. assuming that worker failed to cancel task previously and ignoring invocation results.')
                    return
            await con.execute('UPDATE invocations SET "state" = ? WHERE "id" = ?',
                              (InvocationState.FINISHED.value, task.invocation_id()))
            async with con.execute('SELECT * FROM invocations WHERE "id" = ?', (task.invocation_id(),)) as incur:
                invocation = await incur.fetchone()
            assert invocation is not None

            await con.execute('UPDATE workers SET "state" = ? WHERE "id" = ?',
                              (WorkerState.IDLE.value, invocation['worker_id']))
            await con.execute('UPDATE invocations SET "stdout" = ?, "stderr" = ? WHERE "id" = ?',
                              (stdout, stderr, task.invocation_id()))
            await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                              (TaskState.WAITING.value, invocation['task_id']))
            await con.commit()
        self.wake()

    #
    # add new worker to db
    async def add_worker(self, addr: str, worker_type: WorkerType, worker_resources: WorkerResources, assume_active=True):  # TODO: all resource should also go here
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            await con.execute('BEGIN IMMEDIATE')
            async with con.execute('SELECT id from "workers" WHERE "last_address" = ?', (addr,)) as worcur:
                worker_row = await worcur.fetchone()
            if assume_active:
                ping_state = WorkerPingState.WORKING.value
                state = WorkerState.IDLE.value
            else:
                ping_state = WorkerPingState.OFF.value
                state = WorkerState.OFF.value

            if worker_row is not None:
                await self.reset_invocations_for_worker(worker_row['id'], con=con)
                await con.execute('UPDATE "workers" SET '
                                  'hwid=?, '
                                  'cpu_count=?, '
                                  'mem_size=?,'
                                  'gpu_count=?,'
                                  'gmem_size=?,'
                                  'last_seen=?, ping_state=?, state=?, worker_type=? '
                                  'WHERE last_address=?',
                                  (worker_resources.hwid,
                                   worker_resources.cpu_count,
                                   worker_resources.mem_size,
                                   worker_resources.gpu_count,
                                   worker_resources.gmem_size,
                                   int(time.time()), ping_state, state, worker_type.value, addr))
            else:
                await con.execute('INSERT INTO "workers" '
                                  '(hwid, cpu_count, mem_size, gpu_count, gmem_size, last_address, last_seen, ping_state, state, worker_type) '
                                  'VALUES '
                                  '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                                  (worker_resources.hwid,
                                   worker_resources.cpu_count,
                                   worker_resources.mem_size,
                                   worker_resources.gpu_count,
                                   worker_resources.gmem_size,
                                   addr, int(time.time()), ping_state, state, worker_type.value))
            await con.commit()

    #
    # worker reports it being stopped
    async def worker_stopped(self, addr: str):
        """

        :param addr:
        :return:
        """
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            await con.execute('BEGIN IMMEDIATE')
            async with con.execute('SELECT id from "workers" WHERE "last_address" = ?', (addr,)) as worcur:
                worker_row = await worcur.fetchone()
            wid = worker_row['id']

            # we ensure there are no invocations running with this worker
            async with con.execute('SELECT "id", task_id FROM invocations WHERE worker_id = ? AND "state" = ?', (wid, InvocationState.IN_PROGRESS.value)) as invcur:
                invocations = await invcur.fetchall()

            await con.execute('UPDATE workers SET "state" = ? WHERE "id" = ?', (WorkerState.OFF.value, wid))
            await con.executemany('UPDATE invocations SET state = ? WHERE "id" = ?', ((InvocationState.FINISHED.value, x["id"]) for x in invocations))
            await con.executemany('UPDATE tasks SET state = ? WHERE "id" = ?', ((TaskState.WAITING.value, x["task_id"]) for x in invocations))
            await con.commit()

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

    #
    # change task's paused state
    async def set_task_paused(self, task_ids_or_group: Union[int, Iterable[int], str], paused: bool):
        if isinstance(task_ids_or_group, str):
            async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
                await con.execute('UPDATE "tasks" SET "paused" = ? WHERE "id" IN (SELECT "task_id" FROM task_groups WHERE "group" = ?)',
                                  (int(paused), task_ids_or_group))
                await con.commit()
            self.wake()
            return
        if isinstance(task_ids_or_group, int):
            task_ids_or_group = [task_ids_or_group]
        query = 'UPDATE "tasks" SET "paused" = %d WHERE "id" = ?' % int(paused)
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            await con.executemany(query, ((x,) for x in task_ids_or_group))
            await con.commit()
        self.wake()

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
                del self.__node_objects[node_id]  # it's here to "protect" operation within db transaction. but a proper __node_object lock should be in place instead
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
            node_obj = await self.get_node_object_by_id(nid)
            node_type, node_name = await self.get_node_type_and_name_by_id(nid)
            new_id = await self.add_node(node_type, f'{node_name} copy')
            new_node_obj = await self.get_node_object_by_id(new_id)
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
        node_object = self.__node_objects[node_id]
        if node_object is None:
            self.__logger.error('node_object is None while')
            return
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            await con.execute('UPDATE "nodes" SET node_object = ? WHERE "id" = ?',
                              (await node_object.serialize_async(), node_id))
            await con.commit()

    #
    # stuff
    @atimeit(0.001)
    async def get_full_ui_state(self, task_groups: Optional[Iterable[str]] = None):
        self.__logger.debug('full update for %s', task_groups)
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
                                           'tasks.node_input_name, tasks.node_output_name, tasks.name, tasks.split_level, '
                                           'task_splits.origin_task_id, task_splits.split_id, GROUP_CONCAT(task_groups."group") as groups, invocations.progress '
                                           'FROM "tasks" '
                                           'LEFT JOIN "task_groups" ON tasks.id=task_groups.task_id AND task_groups."group" == ?'
                                           'LEFT JOIN "task_splits" ON tasks.id=task_splits.task_id '
                                           'LEFT JOIN "invocations" ON tasks.id=invocations.task_id AND invocations.state = %d '
                                           'WHERE task_groups."group" == ? '
                                           'GROUP BY tasks."id"' % InvocationState.IN_PROGRESS.value, (group, group)) as cur:  # NOTE: if you change = to LIKE - make sure to GROUP_CONCAT groups too
                        grp_tasks = await cur.fetchall()
                    # print(f'fetch groups: {time.perf_counter() - _dbg}')
                    for task_row in grp_tasks:
                        task = dict(task_row)
                        task['groups'] = set(task['groups'].split(','))
                        if task['id'] in all_tasks:
                            all_tasks[task['id']]['groups'].update(task['groups'])
                        else:
                            all_tasks[task['id']] = task
            # _dbg = time.perf_counter()
            async with con.execute('SELECT DISTINCT task_groups."group", task_group_attributes.ctime FROM task_groups LEFT JOIN task_group_attributes ON task_groups."group" = task_group_attributes."group";') as cur:
                all_task_groups = {x['group']: dict(x) for x in await cur.fetchall()}
            # print(f'distinct groups: {time.perf_counter() - _dbg}')
            # _dbg = time.perf_counter()
            async with con.execute('SELECT workers."id", cpu_count, mem_size, gpu_count, gmem_size, last_address, last_seen, workers."state", worker_type, invocations.node_id, invocations.task_id, invocations.progress, '
                                   'GROUP_CONCAT(worker_groups."group") as groups '
                                   'FROM workers '
                                   'LEFT JOIN invocations ON workers."id" == invocations.worker_id AND invocations."state" == 0 '
                                   'LEFT JOIN worker_groups ON workers."id" == worker_groups.worker_id '
                                   'GROUP BY workers."id"') as cur:
                all_workers = tuple(dict(x) for x in await cur.fetchall())
            # print(f'workers: {time.perf_counter() - _dbg}')
            data = await create_uidata(all_nodes, all_conns, all_tasks, all_workers, all_task_groups)
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
    async def spawn_tasks(self, newtasks: Union[Iterable[TaskSpawn], TaskSpawn], con: Optional[aiosqlite.Connection] = None) -> SpawnStatus:
        """

        :param newtasks:
        :param con:
        :return:
        """

        async def _inner_shit():
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
                                       (newtask.name(), json.dumps(newtask._attributes()), parent_task_id,
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
                    new_group = '{name}#{id:d}'.format(name=newtask.name(), id=new_id)
                    await con.execute('INSERT INTO task_groups ("task_id", "group") VALUES (?, ?)',
                                      (new_id, new_group))
                    await con.execute('INSERT OR REPLACE INTO task_group_attributes ("group", "ctime") VALUES (?, ?)',
                                      (new_group, int(datetime.utcnow().timestamp())))

                if newtask.extra_group_names():
                    groups = newtask.extra_group_names()
                    await con.executemany('INSERT INTO task_groups ("task_id", "group") VALUES (?, ?)',
                                          zip(itertools.repeat(new_id, len(groups)), groups))

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
    async def get_log_metadata(self, task_id: int):
        """
        get task's log metadata - meaning which nodes it ran on and how
        :param task_id:
        :return: dict[node_id -> dict[invocation_id: None]]
        """
        async with aiosqlite.connect(self.db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            logs = {}
            self.__logger.debug(f'fetching log metadata for {task_id}')
            async with con.execute('SELECT "id", node_id from "invocations" WHERE "task_id" = ?',
                                   (task_id, )) as cur:
                async for entry in cur:
                    node_id = entry['node_id']
                    if node_id not in logs:
                        logs[node_id] = {}
                    logs[node_id][entry['id']] = None
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
                    if entry['state'] == InvocationState.IN_PROGRESS.value or entry['stdout'] is None or entry['stderr'] is None:
                        async with con.execute('SELECT last_address FROM workers WHERE "id" = ?', (entry['worker_id'],)) as worcur:
                            workrow = await worcur.fetchone()
                        if workrow is None:
                            self.__logger.error('Worker not found during log fetch! this is not supposed to happen! Database inconsistent?')
                        else:
                            try:
                                async with WorkerTaskClient(*address_to_ip_port(workrow['last_address'])) as client:
                                    stdout, stderr = await client.get_log(invocation_id)
                                await con.execute('UPDATE "invocations" SET stdout = ?, stderr = ? WHERE "id" = ?',
                                                  (stdout, stderr, invocation_id))
                                await con.commit()
                            except ConnectionError:
                                self.__logger.warning('could not connect to worker to get freshest logs')
                            else:
                                entry['stdout'] = stdout
                                entry['stderr'] = stderr
                    logs[entry['id']] = entry
        return {node_id: logs}

    def server_address(self) -> str:
        return self.__server_address


default_config = '''
[core]
## you can uncomment stuff below to specify some static values
## 
# server_ip = "192.168.0.2"
# server_port = 7979
# ui_ip = "192.168.0.2"
# ui_port = 7989

[scheduler]
## you may specify here some db to load
## ore use --db-path cmd argument to override whatever is in the config
# db_path = "~/some_special_place/main.db"
'''


async def main_async(db_path=None):
    def graceful_closer():
        scheduler.stop()

    if db_path is None:
        config = get_config('scheduler')
        db_path = await config.get_option('scheduler.db_path', str(paths.default_main_database_location()))
    db_path = os.path.realpath(os.path.expanduser(db_path))
    # ensure database is initialized
    async with aiosqlite.connect(db_path) as con:
        await con.executescript(sql_init_script)

    scheduler = Scheduler(db_path)
    asyncio.get_event_loop().add_signal_handler(signal.SIGINT, graceful_closer)
    asyncio.get_event_loop().add_signal_handler(signal.SIGTERM, graceful_closer)
    await scheduler.start()
    await scheduler.wait_till_stops()


def main(argv):
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--db-path', help='path to sqlite database to use')
    parser.add_argument('--verbosity-pinger', help='set individual verbosity for worker pinger')
    opts = parser.parse_args(argv)

    # check and create default config if none
    create_default_user_config_file('scheduler', default_config)

    config = get_config('scheduler')
    db_path = opts.db_path if opts.db_path is not None else config.get_option_noasync('scheduler.db_path', str(paths.default_main_database_location()))
    global_logger = logging.get_logger('scheduler')
    if opts.verbosity_pinger:
        logging.get_logger('scheduler.worker_pinger').setLevel(opts.verbosity_pinger)
    try:
        asyncio.run(main_async(db_path))
    except KeyboardInterrupt:
        global_logger.warning('SIGINT caught')
        global_logger.info('SIGINT caught. Scheduler is stopped now.')


if __name__ == '__main__':
    main(sys.argv[1:])
