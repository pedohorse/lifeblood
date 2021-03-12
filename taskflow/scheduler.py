import sys
import os
import time
import json
import importlib.util
from enum import Enum
import asyncio
import aiosqlite

from .worker_task_protocol import WorkerTaskClient, WorkerPingReply, TaskScheduleStatus
from .scheduler_task_protocol import SchedulerTaskProtocol, SpawnStatus
from .scheduler_ui_protocol import SchedulerUiProtocol
from .invocationjob import InvocationJob
from .uidata import create_uidata
from .broadcasting import create_broadcaster
from .nethelpers import address_to_ip_port
from .taskspawn import TaskSpawn
from .basenode import BaseNode
from .nodethings import ProcessingResult
from .exceptions import *
from . import pluginloader

from typing import Optional, Any, AnyStr, List, Iterable, Union, Dict


class WorkerState(Enum):
    OFF = 0
    IDLE = 1
    BUSY = 2
    ERROR = 3


class WorkerPingState(Enum):
    OFF = 0
    CHECKING = 1
    ERROR = 2
    WORKING = 3


class TaskState(Enum):
    WAITING = 0  # arrived at node, does not know what to do
    GENERATING = 1  # node is generating work load
    READY = 2  # ready to be scheduled
    IN_PROGRESS = 3  # is being worked on by a worker
    POST_WAITING = 4  # task is waiting to be post processed by node
    POST_GENERATING = 5  # task is being post processed by node
    DONE = 6  # done, needs further processing
    ERROR = 7  # some internal error, not allowing to process task. NOT INVOCATION ERROR
    SPAWNED = 8  # spawned tasks are just passed down from node's "spawned" output
    DEAD = 9  # task will not be processed any more


class InvocationState(Enum):
    IN_PROGRESS = 0
    FINISHED = 1


class Scheduler:
    def __init__(self, db_file_path, do_broadcasting=True, loop=None):
        print('loading core plugins')
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
        self.__server = loop.create_server(self.scheduler_protocol_factory, '127.0.0.1', 7979, backlog=16)
        self.__server_address = '127.0.0.1:7979'
        self.__ui_address = '127.0.0.1:7989'
        self.__ui_server = loop.create_server(self.ui_protocol_factory, '127.0.0.1', 7989, backlog=16)
        if do_broadcasting:
            broadcast_info = json.dumps({'worker': self.__server_address, 'ui': self.__ui_address})
            self.__broadcasting_server_task = create_broadcaster('taskflow_scheduler', broadcast_info)
        else:
            self.__broadcasting_server_task = loop.create_future()
            self.__broadcasting_server_task.set_result('noop')

        self.__ping_interval = 5
        self.__processing_interval = 2

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
                self.__node_objects[node_id] = await BaseNode.deserialize_async(node_row['node_object'], self)
                return self.__node_objects[node_id]

            newnode: BaseNode = pluginloader.plugins[node_type].create_node_object(node_row['name'], self)
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
                need_commit = False
                async for invoc_row in incur:  # mark all (probably single one) invocations
                    need_commit = True
                    print("fixing dangling invocation %d" % (invoc_row['id'],))
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

            print('    :: pinger started')
            await self.set_worker_ping_state(worker_row['id'], WorkerPingState.CHECKING, con)
            addr = worker_row['last_address']
            ip, port = addr.split(':')  # type: str, str
            print('    :: checking', ip, port)
            if not port.isdigit():
                print('    :: malformed address')
                await asyncio.gather(
                    self.set_worker_ping_state(worker_row['id'], WorkerPingState.ERROR, con, nocommit=True),
                    self.set_worker_state(worker_row['id'], WorkerState.ERROR, con, nocommit=True)
                )
                await _check_lastseen_and_drop_invocations()
                await con.commit()
                return
            try:
                async with WorkerTaskClient(ip, int(port)) as client:
                    ping_code = await client.ping()
            except asyncio.exceptions.TimeoutError:
                print('    :: network error')
                await asyncio.gather(self.set_worker_ping_state(worker_row['id'], WorkerPingState.ERROR, con, nocommit=True),
                                     self.set_worker_state(worker_row['id'], WorkerState.ERROR, con, nocommit=True)
                                     )
                await _check_lastseen_and_drop_invocations()
                await con.commit()
                return
            except ConnectionRefusedError as e:
                print('    :: host down', e)
                await asyncio.gather(self.set_worker_ping_state(worker_row['id'], WorkerPingState.OFF, con, nocommit=True),
                                     self.set_worker_state(worker_row['id'], WorkerState.OFF, con, nocommit=True)
                                     )
                await _check_lastseen_and_drop_invocations()
                await con.commit()
                return
            except Exception as e:
                print('    :: ping failed', type(e), e)
                await asyncio.gather(self.set_worker_ping_state(worker_row['id'], WorkerPingState.ERROR, con, nocommit=True),
                                     self.set_worker_state(worker_row['id'], WorkerState.OFF, con, nocommit=True)
                                     )
                await con.commit()
                return

            if ping_code == WorkerPingReply.IDLE:
                workerstate = WorkerState.IDLE
                if await self.reset_invocations_for_worker(worker_row['id'], con=con):
                    await con.commit()
            else:
                workerstate = WorkerState.BUSY

            await asyncio.gather(self.set_worker_ping_state(worker_row['id'], WorkerPingState.WORKING, con, nocommit=True),
                                 self.set_worker_state(worker_row['id'], workerstate, con, nocommit=True),
                                 self.update_worker_lastseen(worker_row['id'], con, nocommit=True)
                                 )
            await con.commit()
            print('    ::', ping_code)

    #
    # pinger "thread"
    async def worker_pinger(self):
        """
        one of main constantly running coroutines
        responsible for pinging all the workers once in a while in separate tasks each
        TODO: test how well this approach works for 1000+ workers
        :return: NEVER !!
        """

        async with aiosqlite.connect(self.db_path, timeout=30) as con:
            con.row_factory = aiosqlite.Row

            tasks = []
            while True:

                print('    ::selecting workers...')
                async with con.execute("SELECT * from workers WHERE ping_state != 1") as cur:
                    # TODO: don't scan the errored and off ones as often?

                    async for row in cur:
                        tasks.append(asyncio.create_task(self._iter_iter_func(row)))

                # now clean the list
                tasks = [x for x in tasks if not x.done()]
                print('    :: remaining ping tasks:', len(tasks))

                await asyncio.sleep(self.__ping_interval)

    #
    # task processing thread
    async def task_processor(self):
        while True:
            async with aiosqlite.connect(self.db_path) as con:
                con.row_factory = aiosqlite.Row
                async with con.execute('SELECT tasks.*, nodes.type as node_type, nodes.name as node_name, nodes.id as node_id, '
                                       'task_splits.split_id as split_id, task_splits.split_element as split_element, task_splits.split_count as split_count, task_splits.origin_task_id as split_origin_task_id '
                                       'FROM tasks INNER JOIN nodes ON tasks.node_id=nodes.id '
                                       'LEFT JOIN task_splits ON tasks.id=task_splits.task_id AND tasks.split_level=task_splits.split_level AND task_splits.split_sealed=0 '
                                       'WHERE state = ? OR state = ? OR state = ? OR state = ? OR state = ? '
                                       'ORDER BY RANDOM()',
                                       (TaskState.WAITING.value, TaskState.READY.value,
                                        TaskState.DONE.value, TaskState.POST_WAITING.value, TaskState.SPAWNED.value)) as cur:
                    async for task_row in cur:

                        # TODO: this is really fucking unreadeble
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
                            except Exception as e:  # TODO: save error information into database
                                async with aiosqlite.connect(self.db_path) as con:
                                    await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                                      (TaskState.ERROR.value, task_id))
                                    await con.commit()
                                    print('error happened', e, file=sys.stderr)
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
                                await con.commit()

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
                        if task_row['state'] == TaskState.WAITING.value:
                            if task_row['node_type'] not in pluginloader.plugins:
                                print(f'plugin to process "P{task_row["node_type"]}" not found!')
                                await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                                  (TaskState.DONE.value, task_row['id']))
                                await con.commit()
                            else:

                                await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                                  (TaskState.GENERATING.value, task_row['id']))
                                await con.commit()

                                asyncio.create_task(_awaiter((await self.get_node_object_by_id(task_row['node_id'])).process_task, dict(task_row),
                                                             abort_state=TaskState.WAITING, skip_state=TaskState.POST_WAITING))

                        #
                        # waiting to be post processed
                        elif task_row['state'] == TaskState.POST_WAITING.value:
                            if task_row['node_type'] not in pluginloader.plugins:
                                print(f'plugin to process "P{task_row["node_type"]}" not found!')
                                await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                                  (TaskState.DONE.value, task_row['id']))
                                await con.commit()
                            else:
                                await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                                  (TaskState.POST_GENERATING.value, task_row['id']))
                                await con.commit()

                                asyncio.create_task(_awaiter((await self.get_node_object_by_id(task_row['node_id'])).postprocess_task, dict(task_row),
                                                             abort_state=TaskState.POST_WAITING, skip_state=TaskState.DONE))

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
                                print('error addres converting during unexpected here. ping should have cought it', file=sys.stderr)
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
                                    await submit_transaction.commit()
                                else:
                                    task.set_invocation_id(invocation_id)
                                    # TaskData(['bash', '-c', 'echo "boo" && sleep 10 && echo meow'], None, invocation_id)
                                    print(f'submitting task to {addr}')
                                    try:
                                        async with WorkerTaskClient(ip, port) as client:
                                            reply = await client.give_task(task, self.__server_address)
                                        print(f'got reply {reply}')
                                    except Exception as e:
                                        print('some unexpected error', e, file=sys.stderr)
                                        reply = TaskScheduleStatus.FAILED
                                    if reply == TaskScheduleStatus.SUCCESS:
                                        await submit_transaction.execute('UPDATE tasks SET state = ? WHERE "id" = ?',
                                                                         (TaskState.IN_PROGRESS.value, task_row['id']))
                                        await submit_transaction.execute('UPDATE workers SET state = ? WHERE "id" = ?',
                                                                         (WorkerState.BUSY.value, worker['id']))
                                        await submit_transaction.commit()
                                    else:  # on anything but success - cancel transaction
                                        await submit_transaction.rollback()

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
                                        new_split_level = task_row['split_level'] + 1
                                        await con.execute('UPDATE tasks SET node_id = ?, node_input_name = ?, state = ?, work_data = ?, split_level = ?'
                                                          'WHERE "id" = ?',
                                                          (all_wires[0]['node_id_in'], all_wires[0]['in_name'], TaskState.WAITING.value, None, new_split_level,
                                                           task_row['id']))
                                        async with con.execute('SELECT MAX("split_id") as m FROM "task_splits"') as maxsplitcur:
                                            next_split_id = 1 + ((await maxsplitcur.fetchone())['m'] or 0)
                                        await con.execute('INSERT INTO "task_splits" ("split_id", "task_id", "split_element", "split_count", "split_level", "origin_task_id") VALUES (?,?,?,?,?,?)',
                                                          (next_split_id, task_row['id'], 0, wire_count, new_split_level, task_row['id']))
                                        for split_element, wire in enumerate(all_wires[1:], 1):
                                            async with con.execute('INSERT INTO tasks (parent_id, "state", "node_id", "node_input_name", '
                                                                   '"work_data", "name", "attributes", "split_level") '
                                                                   'VALUES (?,?,?,?,?,?,?,?)',
                                                                   (task_row['parent_id'], TaskState.WAITING.value, wire['node_id_in'], wire['in_name'],
                                                                    None, task_row['name'], task_row['attributes'], new_split_level)) \
                                                    as insert_cur:
                                                new_task_id = insert_cur.lastrowid
                                            await con.execute('INSERT INTO "task_splits" ("split_id", "task_id", "split_element", "split_count", "split_level", "origin_task_id") VALUES (?,?,?,?,?,?)',
                                                              (next_split_id, new_task_id, split_element, wire_count, new_split_level, task_row['id']))
                                    await con.commit()

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
    async def task_done_reported(self, task: InvocationJob, return_code: int, stdout: str, stderr: str):
        print('task finished reported', task, 'code', return_code)
        async with aiosqlite.connect(self.db_path) as con:
            con.row_factory = aiosqlite.Row
            await con.execute('UPDATE invocations SET "state" = ?, "return_code" = ? WHERE "id" = ?',
                              (InvocationState.FINISHED.value, return_code, task.invocation_id()))
            async with con.execute('SELECT * FROM invocations WHERE "id" = ?', (task.invocation_id(),)) as incur:
                invocation = await incur.fetchone()
            assert invocation is not None
            await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                              (TaskState.POST_WAITING.value, invocation['task_id']))
            await con.execute('UPDATE workers SET "state" = ? WHERE "id" = ?',
                              (WorkerState.IDLE.value, invocation['worker_id']))
            await con.execute('UPDATE invocations SET "stdout" = ?, "stderr" = ? WHERE "id" = ?',
                              (stdout, stderr, task.invocation_id()))
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
    # node reports it's interface was changed. not sure why it exists
    async def node_reports_ui_update(self, node_object):
        print('suck ur blood, bla, bla', node_object)

    #
    # stuff
    async def get_full_ui_state(self):
        async with aiosqlite.connect(self.db_path) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT * from "nodes"') as cur:
                all_nodes = await cur.fetchall()
            async with con.execute('SELECT * from "node_connections"') as cur:
                all_conns = await cur.fetchall()
            async with con.execute('SELECT tasks.*, task_splits.origin_task_id, task_splits.split_id '
                                   'FROM "tasks" LEFT JOIN "task_splits" ON tasks.id=task_splits.task_id AND tasks.split_level=task_splits.split_level') as cur:
                all_tasks = await cur.fetchall()
            data = await create_uidata(all_nodes, all_conns, all_tasks)
        return data

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
                    print('ERROR CREATING SPAWN TASK: Malformed source', file=sys.stderr)
                    continue
                await con.execute('INSERT INTO tasks ("name", "attributes", "parent_id", "state", "node_id", "node_output_name") VALUES (?, ?, ?, ?, ?, ?)',
                                  (newtask.name(), json.dumps(newtask._attributes()), parent_task_id, TaskState.SPAWNED.value, node_id, newtask.node_output_name()))

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

    async def get_log_metadata(self, task_id: int):
        """
        get task's log metadata - meaning which nodes it ran on and how
        :param task_id:
        :return: dict[node_id -> dict[invocation_id: None]]
        """
        async with aiosqlite.connect(self.db_path) as con:
            con.row_factory = aiosqlite.Row
            logs = {}
            print(f'fetching log metadata for {task_id}')
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
            print(f'fetching for {task_id}, {node_id}', '' if invocation_id is None else invocation_id)
            if invocation_id is None:  # TODO: disable this option
                async with con.execute('SELECT * from "invocations" WHERE "task_id" = ? AND "node_id" = ?',
                                       (task_id, node_id)) as cur:
                    async for entry in cur:
                        logs[entry['id']] = dict(entry)
            else:
                async with con.execute('SELECT * from "invocations" WHERE "task_id" = ? AND "node_id" = ? AND "id" = ?',
                                       (task_id, node_id, invocation_id)) as cur:
                    async for entry in cur:  # should be exactly 1 or 0
                        entry = dict(entry)
                        if entry['state'] == InvocationState.IN_PROGRESS.value or entry['stdout'] is None or entry['stderr'] is None:
                            async with con.execute('SELECT last_address FROM workers WHERE "id" = ?', (entry['worker_id'],)) as worcur:
                                workrow = await worcur.fetchone()
                            if workrow is None:
                                print('WARNING! worker not found during log fetch! this is not supposed to happen! Database inconsistent?')
                            else:
                                try:
                                    async with WorkerTaskClient(*address_to_ip_port(workrow['last_address'])) as client:
                                        stdout, stderr = await client.get_log(invocation_id)
                                    await con.execute('UPDATE "invocations" SET stdout = ?, stderr = ? WHERE "id" = ?',
                                                      (stdout, stderr, invocation_id))
                                    await con.commit()
                                except ConnectionError:
                                    print('could not connect to worker to get freshest logs', file=sys.stderr)
                                else:
                                    entry['stdout'] = stdout
                                    entry['stderr'] = stderr
                        logs[entry['id']] = entry
        return {node_id: logs}

async def main():
    scheduler = Scheduler(os.path.realpath('main.db'))
    await scheduler.run()


def _main():
    asyncio.run(main())


if __name__ == '__main__':
    _main()
