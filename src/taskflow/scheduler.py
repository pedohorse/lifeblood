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

from . import logging
from . import paths
from .db_misc import sql_init_script
from .worker_task_protocol import WorkerTaskClient, WorkerPingReply, TaskScheduleStatus
from .scheduler_task_protocol import SchedulerTaskProtocol, SpawnStatus
from .scheduler_ui_protocol import SchedulerUiProtocol
from .invocationjob import InvocationJob
from .uidata import create_uidata
from .broadcasting import create_broadcaster
from .nethelpers import address_to_ip_port, get_default_addr, get_default_broadcast_addr
from .taskspawn import TaskSpawn
from .basenode import BaseNode
from .nodethings import ProcessingResult
from .exceptions import *
from . import pluginloader
from .enums import WorkerState, WorkerPingState, TaskState, InvocationState
from .config import get_config

from typing import Optional, Any, AnyStr, List, Iterable, Union, Dict


class Scheduler:
    def __init__(self, db_file_path, do_broadcasting=True, loop=None):
        self.__logger = logging.getLogger('scheduler')
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

        if loop is None:
            loop = asyncio.get_event_loop()
        self.db_path = db_file_path
        config = get_config('scheduler')
        server_ip = config.get_option_noasync('core.server_ip', get_default_addr())
        server_port = config.get_option_noasync('core.server_port', 7979)
        ui_ip = config.get_option_noasync('core.ui_ip', get_default_addr())
        ui_port = config.get_option_noasync('core.ui_port', 7989)
        self.__server = loop.create_server(self.scheduler_protocol_factory, server_ip, server_port, backlog=16)
        self.__server_address = ':'.join((server_ip, str(server_port)))
        self.__ui_server = loop.create_server(self.ui_protocol_factory, ui_ip, ui_port, backlog=16)
        self.__ui_address = ':'.join((ui_ip, str(ui_port)))
        if do_broadcasting:
            broadcast_info = json.dumps({'worker': self.__server_address, 'ui': self.__ui_address})
            self.__broadcasting_server_task = create_broadcaster('taskflow_scheduler', broadcast_info, ip=get_default_broadcast_addr())
        else:
            self.__broadcasting_server_task = loop.create_future()
            self.__broadcasting_server_task.set_result('noop')

        self.__ping_interval = 1
        self.__processing_interval = 2

        self.__event_loop = asyncio.get_running_loop()
        assert self.__event_loop is not None, 'Scheduler MUST be created within working event loop, in the main thread'

    def get_event_loop(self):
        return self.__event_loop

    def scheduler_protocol_factory(self):
        return SchedulerTaskProtocol(self)

    def ui_protocol_factory(self):
        return SchedulerUiProtocol(self)

    async def get_node_object_by_id(self, node_id: int) -> BaseNode:
        if node_id in self.__node_objects:
            return self.__node_objects[node_id]
        async with aiosqlite.connect(self.db_path) as con:
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
        async with aiosqlite.connect(self.db_path) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT attributes FROM tasks WHERE "id" = ?', (task_id,)) as cur:
                res = await cur.fetchone()
            if res is None:
                raise RuntimeError('task with specified id was not found')
            return await asyncio.get_event_loop().run_in_executor(None, json.loads, res['attributes'])

    async def run(self):
        # prepare
        async with aiosqlite.connect(self.db_path) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute("SELECT id from workers") as cur:
                async for row in cur:
                    await self.set_worker_ping_state(row['id'], WorkerPingState.OFF, con, nocommit=True)
            await con.commit()
            await con.execute('UPDATE "tasks" SET "state" = ? WHERE "state" = ?',
                              (TaskState.WAITING.value, TaskState.GENERATING.value))
            await con.execute('UPDATE "tasks" SET "state" = ? WHERE "state" = ?',
                              (TaskState.POST_WAITING.value, TaskState.POST_GENERATING.value))
            await con.commit()

        # run
        await asyncio.gather(self.task_processor(),
                             self.worker_pinger(),
                             self.__server,
                             self.__ui_server,
                             self.__broadcasting_server_task)

    async def set_worker_ping_state(self, wid: int, state: WorkerPingState, con: Optional[aiosqlite.Connection] = None, nocommit: bool = False) -> None:
        await self._set_value('workers', 'ping_state', wid, state.value, con, nocommit)

    async def set_worker_state(self, wid: int, state: WorkerState, con: Optional[aiosqlite.Connection] = None, nocommit: bool = False) -> None:
        await self._set_value('workers', 'state', wid, state.value, con, nocommit)

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
            async with aiosqlite.connect(self.db_path) as con:
                if await _inner_(con):
                    await con.commit()
                return False
        else:
            return await _inner_(con)


    async def _set_value(self, table: str, field: str, wid: int, value: Any, con: Optional[aiosqlite.Connection] = None, nocommit: bool = False) -> None:
        if con is None:
            # TODO: safe check table and field, allow only text
            # TODO: handle db errors
            async with aiosqlite.connect(self.db_path) as con:
                await con.execute("UPDATE %s SET %s= ? WHERE id = ?" % (table, field), (value, wid))
                if not nocommit:
                    await con.commit()
        else:
            await con.execute("UPDATE %s SET %s = ? WHERE id = ?" % (table, field), (value, wid))
            if not nocommit:
                await con.commit()

    async def _iter_iter_func(self, worker_row):
        async with aiosqlite.connect(self.db_path, timeout=30) as con:
            con.row_factory = aiosqlite.Row

            async def _check_lastseen_and_drop_invocations():
                if worker_row['last_seen'] is not None and time.time() - worker_row['last_seen'] < 32:
                    return False
                return await self.reset_invocations_for_worker(worker_row['id'], con)

            self.__logger.debug('    :: pinger started')
            await self.set_worker_ping_state(worker_row['id'], WorkerPingState.CHECKING, con, nocommit=True)
            await self._set_value('workers', 'last_checked', worker_row['id'], int(time.time()), con, nocommit=True)
            await con.commit()

            addr = worker_row['last_address']
            ip, port = addr.split(':')  # type: str, str
            self.__logger.debug('    :: checking %s, %s', ip, port)

            if not port.isdigit():
                self.__logger.debug('    :: malformed address')
                await asyncio.gather(
                    self.set_worker_ping_state(worker_row['id'], WorkerPingState.ERROR, con, nocommit=True),
                    self.set_worker_state(worker_row['id'], WorkerState.ERROR, con, nocommit=True)
                )
                await _check_lastseen_and_drop_invocations()
                await con.commit()
                return
            try:
                async with WorkerTaskClient(ip, int(port)) as client:
                    ping_code, pvalue = await client.ping()
            except asyncio.exceptions.TimeoutError:
                self.__logger.debug('    :: network error')
                await asyncio.gather(self.set_worker_ping_state(worker_row['id'], WorkerPingState.ERROR, con, nocommit=True),
                                     self.set_worker_state(worker_row['id'], WorkerState.ERROR, con, nocommit=True)
                                     )
                await _check_lastseen_and_drop_invocations()
                await con.commit()
                return
            except ConnectionRefusedError as e:
                self.__logger.debug('    :: host down %s', str(e))
                await asyncio.gather(self.set_worker_ping_state(worker_row['id'], WorkerPingState.OFF, con, nocommit=True),
                                     self.set_worker_state(worker_row['id'], WorkerState.OFF, con, nocommit=True)
                                     )
                await _check_lastseen_and_drop_invocations()
                await con.commit()
                return
            except Exception as e:
                self.__logger.debug('    :: ping failed %s %s', type(e), e)
                await asyncio.gather(self.set_worker_ping_state(worker_row['id'], WorkerPingState.ERROR, con, nocommit=True),
                                     self.set_worker_state(worker_row['id'], WorkerState.OFF, con, nocommit=True)
                                     )
                await con.commit()
                return

            if ping_code == WorkerPingReply.IDLE:
                workerstate = WorkerState.IDLE
                # TODO: commented below as it seem to cause race conditions with worker invocation done reporting. NEED CHECKING
                #if await self.reset_invocations_for_worker(worker_row['id'], con=con):
                #    await con.commit()
            else:
                workerstate = WorkerState.BUSY  # in this case received pvalue is current task's progress. u cannot rely on it's precision: some invocations may not support progress reporting
                # TODO: think, can there be race condition here so that worker is already doing something else?
                await con.execute('UPDATE "invocations" SET "progress" = ? WHERE "state" = ? AND "worker_id" = ?', (pvalue, InvocationState.IN_PROGRESS.value, worker_row['id']))

            await asyncio.gather(self.set_worker_ping_state(worker_row['id'], WorkerPingState.WORKING, con, nocommit=True),
                                 self.set_worker_state(worker_row['id'], workerstate, con, nocommit=True),
                                 self.update_worker_lastseen(worker_row['id'], con, nocommit=True)
                                 )
            await con.commit()
            self.__logger.debug('    :: %s', ping_code)

    async def split_task(self, task_id: int, into: int, con: aiosqlite.Connection) -> List[int]:
        """
        con is expected to be a opened db connection with dict factory
        :param into:
        :param con:
        :return:
        """
        if into <= 1:
            raise ValueError('cant split into less or eq than 1 part')

        async with con.execute('SELECT * FROM tasks WHERE "id" = ?', (task_id,)) as cur:
            task_row = await cur.fetchone()
        new_split_level = task_row['split_level'] + 1

        async with con.execute('SELECT MAX("split_id") as m FROM "task_splits"') as maxsplitcur:
            next_split_id = 1 + ((await maxsplitcur.fetchone())['m'] or 0)
        await con.execute('UPDATE tasks SET split_level = ? WHERE "id" = ?',
                          (new_split_level, task_id))
        await con.execute('INSERT INTO "task_splits" ("split_id", "task_id", "split_element", "split_count", "split_level", "origin_task_id") VALUES (?,?,?,?,?,?)',
                          (next_split_id, task_row['id'], 0, into, new_split_level, task_id))
        all_split_ids = [task_id]
        for split_element in range(1, into):
            async with con.execute('INSERT INTO tasks (parent_id, "state", "node_id", '
                                   '"node_input_name", "node_output_name", '
                                   '"work_data", "name", "attributes", "split_level") '
                                   'VALUES (?,?,?,?,?,?,?,?,?)',
                                   (task_row['parent_id'], task_row['state'], task_row['node_id'],
                                    task_row['node_input_name'], task_row['node_output_name'],
                                    task_row['work_data'], task_row['name'], task_row['attributes'], new_split_level)) \
                    as insert_cur:
                new_task_id = insert_cur.lastrowid

            # copy groups
            async with con.execute('SELECT "group" FROM task_groups WHERE "task_id" = ?', (task_id,)) as gcur:
                groups = [x['group'] for x in await gcur.fetchall()]
            if len(groups) > 0:
                await con.executemany('INSERT INTO task_groups ("task_id", "group") VALUES (?, ?)',
                                      zip(itertools.repeat(new_task_id, len(groups)), groups))

            all_split_ids.append(new_task_id)
            await con.execute('INSERT INTO "task_splits" ("split_id", "task_id", "split_element", "split_count", "split_level", "origin_task_id") VALUES (?,?,?,?,?,?)',
                              (next_split_id, new_task_id, split_element, into, new_split_level, task_id))
        # now increase number of children to the parent of the task being splitted
        if task_row['parent_id'] is not None:
            await con.execute('UPDATE "tasks" SET children_count = children_count + ? WHERE "id" = ?', (into - 1, task_row['parent_id']))

        assert into == len(all_split_ids)
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

        async with aiosqlite.connect(self.db_path, timeout=30) as con:  # TODO: is it good to keep con always open?
            con.row_factory = aiosqlite.Row

            tasks = []
            while True:
                nowtime = time.time()

                self.__logger.debug('    ::selecting workers...')
                async with con.execute("SELECT * from workers WHERE ping_state != 1") as cur:
                    # TODO: don't scan the errored and off ones as often?

                    async for row in cur:
                        time_delta = nowtime - (row['last_checked'] or 0)
                        if row['state'] == WorkerState.BUSY.value:
                            tasks.append(asyncio.create_task(self._iter_iter_func(row)))
                        elif row['state'] == WorkerState.IDLE.value and time_delta > 4 * self.__ping_interval:
                            tasks.append(asyncio.create_task(self._iter_iter_func(row)))
                        else:  # worker state is error or off
                            if time_delta > 15 * self.__ping_interval:
                                tasks.append(asyncio.create_task(self._iter_iter_func(row)))

                # now clean the list
                tasks = [x for x in tasks if not x.done()]
                self.__logger.debug('    :: remaining ping tasks: %d', len(tasks))

                await asyncio.sleep(self.__ping_interval)

    #
    # task processing thread
    async def task_processor(self):

        # task processing coroutimes
        async def _awaiter(processor_to_run, task_row, abort_state: TaskState, skip_state: TaskState):  # TODO: process task generation errors
            task_id = task_row['id']
            loop = asyncio.get_event_loop()
            try:
                process_result: ProcessingResult = await loop.run_in_executor(None, processor_to_run, task_row)  # TODO: this should have task and node attributes!
            except NodeNotReadyToProcess:
                async with aiosqlite.connect(self.db_path) as con:
                    await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                      (abort_state.value, task_id))
                    await con.commit()
                return
            except Exception as e:
                async with aiosqlite.connect(self.db_path) as con:
                    await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                      (TaskState.ERROR.value, task_id))
                    await con.execute('UPDATE tasks SET "state_details" = ? WHERE "id" = ?',
                                      (json.dumps({'message': traceback.format_exc(),
                                                   'happened_at': task_row['state'],
                                                   'type': 'exception',
                                                   'exception_str': str(e),
                                                   'exception_type': str(type(e))})
                                       , task_id))
                    await con.commit()
                    self.__logger.exception('error happened %s %s', type(e), e)
                return

            async with aiosqlite.connect(self.db_path) as con:
                con.row_factory = aiosqlite.Row
                await con.execute('UPDATE tasks SET "node_output_name" = ? WHERE "id" = ?',
                                  (process_result.output_name, task_id))
                if process_result.do_kill_task:
                    await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                      (TaskState.DEAD.value, task_id))
                else:
                    if process_result.invocation_job is None:  # if no job to do
                        await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                          (skip_state.value, task_id))
                    else:
                        taskdada_serialized = await process_result.invocation_job.serialize_async()
                        await con.execute('UPDATE tasks SET "work_data" = ?, "state" = ? WHERE "id" = ?',
                                          (taskdada_serialized, TaskState.READY.value, task_id))
                if process_result.do_split_remove:
                    await con.execute('UPDATE task_splits SET "split_sealed" = 1 '
                                      'WHERE "task_id" = ? AND "split_id" = ?',
                                      (task_id, task_row['split_id']))
                    await con.execute('UPDATE tasks SET "split_level" = "split_level" - 1 WHERE "id" = ?',
                                      (task_id,))
                if process_result.attributes_to_set:  # not None or {}
                    attributes = json.loads(task_row['attributes'])
                    attributes.update(process_result.attributes_to_set)
                    attributes = {k: v for k, v in attributes.items() if v is not None}
                    result_serialized = await asyncio.get_event_loop().run_in_executor(None, json.dumps, attributes)
                    await con.execute('UPDATE tasks SET "attributes" = ? WHERE "id" = ?',
                                      (result_serialized, task_id))
                if process_result.spawn_list is not None:
                    await self.spawn_tasks(process_result.spawn_list, con=con)

                if process_result._split_attribs is not None:
                    split_count = len(process_result._split_attribs)
                    for attr_dict, split_task_id in zip(process_result._split_attribs, await self.split_task(task_id, split_count, con)):
                        async with con.execute('SELECT attributes FROM "tasks" WHERE "id" = ?', (split_task_id,)) as cur:
                            split_task_dict = await cur.fetchone()
                        assert split_task_dict is not None
                        split_task_attrs = json.loads(split_task_dict['attributes'])
                        split_task_attrs.update(attr_dict)
                        await con.execute('UPDATE "tasks" SET attributes = ? WHERE "id" = ?', (json.dumps(split_task_attrs), split_task_id))

                await con.commit()

        while True:
            async with aiosqlite.connect(self.db_path) as con:
                con.row_factory = aiosqlite.Row
                _debug_t0 = time.perf_counter()
                async with con.execute('SELECT tasks.*, nodes.type as node_type, nodes.name as node_name, nodes.id as node_id, '
                                       'task_splits.split_id as split_id, task_splits.split_element as split_element, task_splits.split_count as split_count, task_splits.origin_task_id as split_origin_task_id '
                                       'FROM tasks INNER JOIN nodes ON tasks.node_id=nodes.id '
                                       'LEFT JOIN task_splits ON tasks.id=task_splits.task_id AND tasks.split_level=task_splits.split_level AND task_splits.split_sealed=0 '
                                       'WHERE (state = ? OR state = ? OR state = ? OR state = ? OR state = ? ) '
                                       'AND paused = 0 '
                                       'ORDER BY RANDOM()',
                                       (TaskState.WAITING.value, TaskState.READY.value,
                                        TaskState.DONE.value, TaskState.POST_WAITING.value, TaskState.SPAWNED.value)) as cur:
                    all_task_rows = await cur.fetchall()  # we dont want to iterate reading over changing rows - easy to deadlock yourself (as already happened)
                    # if too much tasks here - consider adding LIMIT to execute and work on portions only
                _debug_t1 = time.perf_counter()
                _debug_tc = 0.0
                _debug_tw = 0.0
                _debug_tpw = 0.0
                _debug_tr = 0.0
                _debug_td = 0.0
                _debug_done_cnt = 0
                _debug_wait_cnt = 0
                for task_row in all_task_rows:


                    # async def _post_awaiter(task_id, node_object, task_row):  # TODO: this is almost the same as _awaiter - so merge!
                    #     loop = asyncio.get_event_loop()
                    #     try:
                    #         new_attributes, newtasks = await loop.run_in_executor(None, node_object.postprocess_task, task_row)  # TODO: this should have task and node attributes!
                    #     except NodeNotReadyToProcess:
                    #         async with aiosqlite.connect(self.db_path) as con:
                    #             await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                    #                               (TaskState.POST_WAITING.value, task_id))
                    #             await con.commit()
                    #         return
                    #     except:  # TODO: save error information into database
                    #         async with aiosqlite.connect(self.db_path) as con:
                    #             await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                    #                               (TaskState.ERROR.value, task_id))
                    #             await con.commit()
                    #         return
                    #
                    #     attributes = task_row['attributes']
                    #     attributes.update(new_attributes)
                    #     attributes = {k: v for k, v in attributes.items() if v is not None}
                    #     result_serialized = await asyncio.get_event_loop().run_in_executor(None, json.dumps, attributes)
                    #     async with aiosqlite.connect(self.db_path) as con:
                    #         await con.execute('UPDATE tasks SET "attributes" = ?, "state" = ? WHERE "id" = ?',
                    #                           (result_serialized, TaskState.DONE.value, task_id))
                    #         if newtasks is None or not isinstance(newtasks, list):
                    #             await con.commit()
                    #             return
                    #         await self.spawn_tasks(newtasks, con=con)
                    #         await con.commit()

                    # means task just arrived in the node and is ready to be processed by the node.
                    # processing node generates args,
                    _debug_tmpw = time.perf_counter()
                    if task_row['state'] == TaskState.WAITING.value:
                        if task_row['node_type'] not in pluginloader.plugins:
                            self.__logger.error(f'plugin to process "P{task_row["node_type"]}" not found!')
                            await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                              (TaskState.DONE.value, task_row['id']))
                            _debug_tmp = time.perf_counter()
                            await con.commit()
                            _debug_tc += time.perf_counter() - _debug_tmp
                        else:
                            if not (await self.get_node_object_by_id(task_row['node_id'])).ready_to_process_task(task_row):
                                continue

                            await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                              (TaskState.GENERATING.value, task_row['id']))
                            _debug_tmp = time.perf_counter()
                            await con.commit()
                            _debug_tc += time.perf_counter() - _debug_tmp

                            asyncio.create_task(_awaiter((await self.get_node_object_by_id(task_row['node_id'])).process_task, dict(task_row),
                                                         abort_state=TaskState.WAITING, skip_state=TaskState.POST_WAITING))
                        _debug_tw += time.perf_counter() - _debug_tmpw
                        _debug_wait_cnt += 1
                    #
                    # waiting to be post processed
                    elif task_row['state'] == TaskState.POST_WAITING.value:
                        if task_row['node_type'] not in pluginloader.plugins:
                            self.__logger.error(f'plugin to process "P{task_row["node_type"]}" not found!')
                            await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                              (TaskState.DONE.value, task_row['id']))
                            _debug_tmp = time.perf_counter()
                            await con.commit()
                            _debug_tc += time.perf_counter() - _debug_tmp
                        else:
                            if not (await self.get_node_object_by_id(task_row['node_id'])).ready_to_postprocess_task(task_row):
                                continue

                            await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                              (TaskState.POST_GENERATING.value, task_row['id']))
                            _debug_tmp = time.perf_counter()
                            await con.commit()
                            _debug_tc += time.perf_counter() - _debug_tmp

                            asyncio.create_task(_awaiter((await self.get_node_object_by_id(task_row['node_id'])).postprocess_task, dict(task_row),
                                                         abort_state=TaskState.POST_WAITING, skip_state=TaskState.DONE))
                        _debug_tpw += time.perf_counter() - _debug_tmpw
                    #
                    # real scheduling should happen here
                    elif task_row['state'] == TaskState.READY.value:
                        async with con.execute('SELECT * from workers WHERE state == ?', (WorkerState.IDLE.value,)) as worcur:
                            worker = await worcur.fetchone()
                        if worker is None:  # nothing available
                            continue

                        addr = worker['last_address']
                        try:
                            ip, port = addr.split(':')
                            port = int(port)
                        except:
                            self.__logger.error('error addres converting during unexpected here. ping should have cought it')
                            continue

                        async with aiosqlite.connect(self.db_path) as submit_transaction:
                            async with submit_transaction.execute(
                                    'INSERT INTO invocations ("task_id", "worker_id", "state", "node_id") VALUES (?, ?, ?, ?)',
                                    (task_row['id'], worker['id'], InvocationState.IN_PROGRESS.value, task_row['node_id'])) as incur:
                                invocation_id = incur.lastrowid  # rowid should be an alias to id, acc to sqlite manual

                            work_data = task_row['work_data']
                            assert work_data is not None
                            task: InvocationJob = await asyncio.get_event_loop().run_in_executor(None, InvocationJob.deserialize, work_data)
                            if not task.args():
                                await submit_transaction.rollback()
                                await submit_transaction.execute('UPDATE tasks SET state = ? WHERE "id" = ?',
                                                                 (TaskState.POST_WAITING.value, task_row['id']))
                                _debug_tmp = time.perf_counter()
                                await submit_transaction.commit()
                                _debug_tc += time.perf_counter() - _debug_tmp
                            else:
                                task._set_invocation_id(invocation_id)
                                task._set_task_attributes(json.loads(task_row['attributes']))
                                # TaskData(['bash', '-c', 'echo "boo" && sleep 10 && echo meow'], None, invocation_id)
                                self.__logger.debug(f'submitting task to {addr}')
                                try:
                                    async with WorkerTaskClient(ip, port) as client:
                                        reply = await client.give_task(task, self.__server_address)
                                    self.__logger.debug(f'got reply {reply}')
                                except Exception as e:
                                    self.__logger.error('some unexpected error %s', e)
                                    reply = TaskScheduleStatus.FAILED
                                if reply == TaskScheduleStatus.SUCCESS:
                                    await submit_transaction.execute('UPDATE tasks SET state = ? WHERE "id" = ?',
                                                                     (TaskState.IN_PROGRESS.value, task_row['id']))
                                    await submit_transaction.execute('UPDATE workers SET state = ? WHERE "id" = ?',
                                                                     (WorkerState.BUSY.value, worker['id']))
                                    _debug_tmp = time.perf_counter()
                                    await submit_transaction.commit()
                                    _debug_tc += time.perf_counter() - _debug_tmp
                                else:  # on anything but success - cancel transaction
                                    await submit_transaction.rollback()
                        _debug_tr += time.perf_counter() - _debug_tmpw
                    #
                    # means task is done being processed by current node,
                    # now it should be passed to the next node
                    elif task_row['state'] == TaskState.DONE.value\
                            or task_row['state'] == TaskState.SPAWNED.value:
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
                                # new_split_level = task_row['split_level'] + 1
                                # await con.execute('UPDATE tasks SET node_id = ?, node_input_name = ?, state = ?, work_data = ?, split_level = ?'
                                #                   'WHERE "id" = ?',
                                #                   (all_wires[0]['node_id_in'], all_wires[0]['in_name'], TaskState.WAITING.value, None, new_split_level,
                                #                    task_row['id']))
                                # async with con.execute('SELECT MAX("split_id") as m FROM "task_splits"') as maxsplitcur:
                                #     next_split_id = 1 + ((await maxsplitcur.fetchone())['m'] or 0)
                                # await con.execute('INSERT INTO "task_splits" ("split_id", "task_id", "split_element", "split_count", "split_level", "origin_task_id") VALUES (?,?,?,?,?,?)',
                                #                   (next_split_id, task_row['id'], 0, wire_count, new_split_level, task_row['id']))
                                # for split_element, wire in enumerate(all_wires[1:], 1):
                                #     async with con.execute('INSERT INTO tasks (parent_id, "state", "node_id", "node_input_name", '
                                #                            '"work_data", "name", "attributes", "split_level") '
                                #                            'VALUES (?,?,?,?,?,?,?,?)',
                                #                            (task_row['parent_id'], TaskState.WAITING.value, wire['node_id_in'], wire['in_name'],
                                #                             None, task_row['name'], task_row['attributes'], new_split_level)) \
                                #             as insert_cur:
                                #         new_task_id = insert_cur.lastrowid
                                #     await con.execute('INSERT INTO "task_splits" ("split_id", "task_id", "split_element", "split_count", "split_level", "origin_task_id") VALUES (?,?,?,?,?,?)',
                                #                       (next_split_id, new_task_id, split_element, wire_count, new_split_level, task_row['id']))
                                # # now increase number of children to the parent of the task being splitted
                                # if task_row['parent_id'] is not None:
                                #     await con.execute('UPDATE "tasks" SET children_count = children_count + ? WHERE "id" = ?', (wire_count-1, task_row['parent_id']))
                            _debug_tmp = time.perf_counter()
                            await con.commit()
                            _debug_tc += time.perf_counter() - _debug_tmp
                        else:
                            # the problem is that there are tasks that done, but have no wires to go anywhere
                            # and that is the point, they are done done. But processing thousands of them every time is painful
                            # so we need to somehow prevent them from being amilessly processed
                            # this is a testing desicion, TODO: test and see if thes is a good way to deal with the problem
                            await con.execute('UPDATE "tasks" SET "paused" = 1 WHERE "id" = ?', (task_row['id'],))
                            _debug_tmp = time.perf_counter()
                            await con.commit()
                            _debug_tc += time.perf_counter() - _debug_tmp

                        _debug_td += time.perf_counter() - _debug_tmpw
                        _debug_done_cnt += 1

            _debug_t2 = time.perf_counter()
            self.__logger.debug(f'SELECT took {_debug_t1-_debug_t0} s, process took {_debug_t2-_debug_t1}s, commits {_debug_tc}s')
            self.__logger.debug(f'waiting: {_debug_wait_cnt} in {_debug_tw}, postwaiting: {_debug_tpw}, ready: {_debug_tr}, done: {_debug_done_cnt} in {_debug_td}')
            await asyncio.sleep(self.__processing_interval)
        # test = 0
        # while True:
        #     if test == 0:
        #         test += 1
        #         await asyncio.sleep(21)
        #         # submit a fake task to all working workers
        #         print('\n\ntime to test shit out\n\n')
        #         async with aiosqlite.connect(self.db_path) as con:
        #             con.row_factory = aiosqlite.Row
        #             invocation = 0
        #             async with con.execute('SELECT * from workers WHERE state = 1') as cur:
        #                 async for row in cur:
        #                     task = TaskData(['bash', '-c', 'echo "boo" && sleep 17 && echo meow'], None, invocation)
        #                     addr = row['last_address']
        #                     try:
        #                         ip, port = addr.split(':')
        #                         port = int(port)
        #                     except:
        #                         print('error addres converting during unexpected here. ping should have cought it')
        #                         continue
        #                     print(f'submitting task to {addr}')
        #                     client = WorkerTaskClient(ip, port)
        #                     reply = await client.give_task(task, self.__server_address)
        #                     print(f'got reply {reply}')
        #         print('\n\ntest over\n\n')
        #     await asyncio.sleep(10)

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
        async with aiosqlite.connect(self.db_path) as con:
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

    #
    # worker reports canceled task
    async def task_cancel_reported(self, task: InvocationJob, stdout: str, stderr: str):
        self.__logger.debug('task cancelled reported %s', repr(task))
        async with aiosqlite.connect(self.db_path) as con:
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

    #
    # add new worker to db
    async def add_worker(self, addr: str):  # TODO: all resource should also go here
        async with aiosqlite.connect(self.db_path) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT id from "workers" WHERE "last_address" = ?', (addr,)) as worcur:
                worker_row = await worcur.fetchone()
            if worker_row is not None:
                await self.reset_invocations_for_worker(worker_row['id'], con=con)
                await con.execute('UPDATE "workers" SET '
                                  'cpu_count=?, mem_size=?, gpu_count=?, gmem_size=?, last_seen=?, ping_state=?, state=? '
                                  'WHERE last_address=?',
                                  (1, 1, 1, 1, int(time.time()), WorkerPingState.WORKING.value, WorkerState.OFF.value, addr))
            else:
                await con.execute('INSERT INTO "workers" '
                                  '(cpu_count, mem_size, gpu_count, gmem_size, last_address, last_seen, ping_state, state) '
                                  'VALUES '
                                  '(?, ?, ?, ?, ?, ?, ?, ?)',
                                  (1, 1, 1, 1, addr, int(time.time()), WorkerPingState.WORKING.value, WorkerState.OFF.value))
            await con.commit()

    #
    # cancel invocation
    async def cancel_invocation(self, invocation_id: str):
        self.__logger.debug(f'canceling invocation {invocation_id}')
        async with aiosqlite.connect(self.db_path) as con:
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
        async with aiosqlite.connect(self.db_path) as con:
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
            async with aiosqlite.connect(self.db_path) as con:
                con.row_factory = aiosqlite.Row
                await con.execute('PRAGMA FOREIGN_KEYS = on')
                await con.execute('UPDATE "tasks" SET "node_id" = ? WHERE "id" = ?', (node_id, task_id))
                await con.commit()
        except aiosqlite.IntegrityError:
            self.__logger.error('could not remove node connection because of database integrity check')

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
        async with aiosqlite.connect(self.db_path) as con:
            for task_id in task_ids:
                await con.execute('BEGIN IMMEDIATE') #
                async with con.execute('SELECT "state" FROM tasks WHERE "id" = ?', (task_id,)) as cur:
                    state = await cur.fetchone()
                    if state is None:
                        await con.rollback()
                        continue
                    state = TaskState(state[0])
                if state in (TaskState.IN_PROGRESS, TaskState.GENERATING, TaskState.POST_GENERATING):
                    self.__logger.warning(f'forcing task out of state {state} is not currently implemented')
                    await con.rollback()
                    return

                await con.execute(query, (task_id,))
                #await con.executemany(query, ((x,) for x in task_ids))
                await con.commit()
        #print('boop')

    #
    # change task's paused state
    async def set_task_paused(self, task_ids_or_group: Union[int, Iterable[int], str], paused: bool):
        if isinstance(task_ids_or_group, str):
            async with aiosqlite.connect(self.db_path) as con:
                await con.execute('UPDATE "tasks" SET "paused" = ? WHERE "id" IN (SELECT "task_id" FROM task_groups WHERE "group" = ?)',
                                  (int(paused), task_ids_or_group))
                await con.commit()
            return
        if isinstance(task_ids_or_group, int):
            task_ids_or_group = [task_ids_or_group]
        query = 'UPDATE "tasks" SET "paused" = %d WHERE "id" = ?' % int(paused)
        async with aiosqlite.connect(self.db_path) as con:
            await con.executemany(query, ((x,) for x in task_ids_or_group))
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
        async with aiosqlite.connect(self.db_path) as con:
            await con.execute('UPDATE "nodes" SET "name" = ? WHERE "id" = ?', (node_name, node_id))
            if node_id in self.__node_objects:
                self.__node_objects[node_id].set_name(node_name)
            await con.commit()
        return node_name

    #
    # node reports it's interface was changed. not sure why it exists
    async def node_reports_ui_update(self, node_id):
        assert node_id in self.__node_objects, 'this may be caused by race condition with node deletion'
        # TODO: introduce __node_objects lock? or otherwise secure access
        node_object = self.__node_objects[node_id]
        async with aiosqlite.connect(self.db_path) as con:
            await con.execute('UPDATE "nodes" SET node_object = ? WHERE "id" = ?',
                              (await node_object.serialize_async(), node_id))
            await con.commit()

    #
    # stuff
    async def get_full_ui_state(self, task_groups: Optional[Iterable[str]] = None):
        self.__logger.debug('full update for %s', task_groups)
        async with aiosqlite.connect(self.db_path) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT * from "nodes"') as cur:
                all_nodes = {x['id']: dict(x) for x in await cur.fetchall()}
            async with con.execute('SELECT * from "node_connections"') as cur:
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
                    async with con.execute('SELECT tasks.id, tasks.parent_id, tasks.children_count, tasks.state, tasks.state_details, tasks.paused, tasks.id, tasks.node_id, '
                                           'tasks.node_input_name, tasks.node_output_name, tasks.name, tasks.split_level, '
                                           'task_splits.origin_task_id, task_splits.split_id, GROUP_CONCAT(task_groups."group") as groups, invocations.progress '
                                           'FROM "tasks" '
                                           'LEFT JOIN "task_splits" ON tasks.id=task_splits.task_id AND tasks.split_level=task_splits.split_level '
                                           'LEFT JOIN "task_groups" ON tasks.id=task_groups.task_id '
                                           'LEFT JOIN "invocations" ON tasks.id=invocations.task_id AND invocations.state = %d '
                                           'WHERE task_groups."group" LIKE ? '
                                           'GROUP BY tasks."id"' % InvocationState.IN_PROGRESS.value, (group,)) as cur:  # NOTE: if you change = to LIKE - make sure to GROUP_CONCAT groups too
                        grp_tasks = await cur.fetchall()
                    for task_row in grp_tasks:
                        task = dict(task_row)
                        task['groups'] = set(task['groups'].split(','))
                        if task['id'] in all_tasks:
                            all_tasks[task['id']]['groups'].update(task['groups'])
                        else:
                            all_tasks[task['id']] = task
            async with con.execute('SELECT DISTINCT task_groups."group", task_group_attributes.ctime FROM task_groups LEFT JOIN task_group_attributes ON task_groups."group" = task_group_attributes."group";') as cur:
                all_task_groups = {x['group']: dict(x) for x in await cur.fetchall()}
            data = await create_uidata(all_nodes, all_conns, all_tasks, all_task_groups)
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
        async with aiosqlite.connect(self.db_path) as con:
            con.row_factory = aiosqlite.Row
            vals.append(node_connection_id)
            await con.execute(f'UPDATE node_connections SET {", ".join(parts)} WHERE "id" = ?', vals)
            await con.commit()

    #
    # add node connection callback
    async def add_node_connection(self, out_node_id: int, out_name: str, in_node_id: int, in_name: str) -> int:
        async with aiosqlite.connect(self.db_path) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('INSERT INTO node_connections (node_id_out, out_name, node_id_in, in_name) VALUES (?,?,?,?)',
                                   (out_node_id, out_name, in_node_id, in_name)) as cur:
                ret = cur.lastrowid
            await con.commit()
            return ret

    #
    # remove node connection callback
    async def remove_node_connection(self, node_connection_id: int):
        try:
            async with aiosqlite.connect(self.db_path) as con:
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
        async with aiosqlite.connect(self.db_path) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('INSERT INTO "nodes" ("type", "name") VALUES (?,?)',
                                   (node_type, node_name)) as cur:
                ret = cur.lastrowid
            await con.commit()
            return ret

    async def remove_node(self, node_id: int):
        try:
            async with aiosqlite.connect(self.db_path) as con:
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
        async with aiosqlite.connect(self.db_path) as con:
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

                async with con.execute('INSERT INTO tasks ("name", "attributes", "parent_id", "state", "node_id", "node_output_name") VALUES (?, ?, ?, ?, ?, ?)',
                                       (newtask.name(), json.dumps(newtask._attributes()), parent_task_id,
                                        TaskState.SPAWNED.value if newtask.create_as_spawned() else TaskState.WAITING.value,
                                        node_id, newtask.node_output_name())) as newcur:
                    new_id = newcur.lastrowid

                if parent_task_id is not None:  # inherit all parent's groups
                    # inc children count
                    await con.execute('UPDATE "tasks" SET children_count = children_count + 1 WHERE "id" == ?', (parent_task_id,))
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
            async with aiosqlite.connect(self.db_path) as con:
                con.row_factory = aiosqlite.Row
                await _inner_shit()
                await con.commit()
        return SpawnStatus.SUCCEEDED

    #
    async def node_name_to_id(self, name: str) -> List[int]:
        """
        get the list of node ids that have specified name
        :param name:
        :return:
        """
        async with aiosqlite.connect(self.db_path) as con:
            async with con.execute('SELECT "id" FROM "nodes" WHERE "name" = ?', (name,)) as cur:
                return list(x[0] for x in await cur.fetchall())

    #
    async def get_log_metadata(self, task_id: int):
        """
        get task's log metadata - meaning which nodes it ran on and how
        :param task_id:
        :return: dict[node_id -> dict[invocation_id: None]]
        """
        async with aiosqlite.connect(self.db_path) as con:
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
        async with aiosqlite.connect(self.db_path) as con:
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


async def main_async(db_path=None):
    if db_path is None:
        config = get_config('scheduler')
        db_path = await config.get_option('scheduler.db_path', str(paths.default_main_database_location()))
    db_path = os.path.realpath(os.path.expanduser(db_path))
    # ensure database is initialized
    async with aiosqlite.connect(db_path) as con:
        await con.executescript(sql_init_script)

    scheduler = Scheduler(db_path)
    await scheduler.run()


def main():
    config = get_config('scheduler')
    db_path = config.get_option_noasync('scheduler.db_path', str(paths.default_main_database_location()))
    asyncio.run(main_async(db_path))


if __name__ == '__main__':
    main()
