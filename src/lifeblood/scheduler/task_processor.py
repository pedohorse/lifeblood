import sys
import traceback
import json
import itertools
import threading  # for bugfix
from concurrent.futures import ThreadPoolExecutor
import aiosqlite
import asyncio
import time
from .. import logging
from ..enums import WorkerState, InvocationState, TaskState, TaskGroupArchivedState, TaskScheduleStatus
from ..misc import atimeit
from ..worker_messsage_processor import WorkerControlClient
from ..invocationjob import InvocationJob
from ..environment_resolver import EnvironmentResolverArguments
from ..nodethings import ProcessingResult
from ..exceptions import *
from .. import pluginloader
from .. import aiosqlite_overlay
from ..config import get_config
from ..ui_events import TaskData, TaskDelta
from ..net_messages.address import AddressChain

from .scheduler_component_base import SchedulerComponentBase

from typing import Any, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:  # TODO: maybe separate a subset of scheduler's methods to smth like SchedulerData class, or idunno, for now no obvious way to separate, so having a reference back
    from .scheduler import Scheduler


# import tracemalloc
# tracemalloc.start()

class TaskProcessor(SchedulerComponentBase):
    def __init__(self, scheduler: "Scheduler"):
        super().__init__(scheduler)
        self.__logger = logging.get_logger('scheduler.task_processor')

        self.__processing_interval = 5  # we don't need interval too small as now things may kick processor out of sleep as needed
        self.__processing_interval_mult = 1
        self.__dormant_mode_processing_interval_multiplier = 5

        self.awaiter_lock = asyncio.Lock()
        # task processing coroutimes
        self.awaiter_executor = ThreadPoolExecutor(thread_name_prefix='awaiter')  # TODO: max_workers= set from config

        config = get_config('scheduler')
        self.__invocation_attempts = config.get_option_noasync('invocation.default_attempts', 3)  # TODO: config should be directly used when needed to allow dynamically reconfigure running scheduler

    def _main_task(self):
        return self.task_processor()

    def _my_wake(self):
        self.__logger.info('exiting DORMANT mode. mode is STANDARD now')
        self.__processing_interval_mult = 1
        self.poke()

    def _my_sleep(self):
        self.__logger.info('entering DORMANT mode')
        self.__processing_interval_mult = self.__dormant_mode_processing_interval_multiplier

    @atimeit()
    async def _awaiter(self, processor_to_run, task_row, abort_state: TaskState, skip_state: TaskState):  # TODO: process task generation errors
        _bench_point_0 = time.perf_counter()
        task_id = task_row['id']
        loop = asyncio.get_event_loop()

        try:
            async with self.scheduler.get_node_lock_by_id(task_row['node_id']).reader_lock:
                time_processing_start = time.perf_counter()
                process_result: ProcessingResult = await loop.run_in_executor(self.awaiter_executor, processor_to_run, task_row)  # TODO: this should have task and node attributes!
                self.__logger.debug(f'(post)processing for {task_row["node_id"]} took {time.perf_counter()-time_processing_start:.4f}')
        except NodeNotReadyToProcess:
            async with self.awaiter_lock, self.scheduler.data_access.lazy_data_transaction('awaiter_con') as con:
                await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                  (abort_state.value, task_id))
                con.add_after_commit_callback(self.scheduler.ui_state_access.scheduler_reports_task_updated, TaskDelta(task_id, state=abort_state))
                await con.commit(self.poke)
            self.__logger.debug('node reports: not ready to process yet')
            return
        except Exception as e:
            async with self.awaiter_lock, self.scheduler.data_access.lazy_data_transaction('awaiter_con') as con:
                state_details = json.dumps({'message': traceback.format_exc(),
                                            'happened_at': task_row['state'],
                                            'type': 'exception',
                                            'exception_str': str(e),
                                            'exception_type': str(type(e))})
                await con.execute('UPDATE tasks SET "state" = ?, "state_details" = ? WHERE "id" = ?',
                                  (TaskState.ERROR.value,
                                   state_details,
                                   task_id))
                con.add_after_commit_callback(self.scheduler.ui_state_access.scheduler_reports_task_updated, TaskDelta(task_id, state=TaskState.ERROR, state_details=state_details))
                await con.commit(self.poke)
                self.__logger.exception('error happened %s %s', type(e), e)
            return

        _bench_point_1 = time.perf_counter()
        # why is there lock? it looks locking manually is waaaay more efficient than relying on transaction locking
        async with self.awaiter_lock, self.scheduler.data_access.lazy_data_transaction('awaiter_con') as con:
            # con.row_factory = aiosqlite.Row
            # This implicitly starts transaction

            _bench_point_2 = time.perf_counter()
            ui_task_delta = TaskDelta(task_id)  # for ui event
            ui_task_delta_split = None

            if not con.in_transaction:
                await con.execute('BEGIN IMMEDIATE')
                assert con.in_transaction
            _bench_point_3 = time.perf_counter()
            if process_result.output_name:
                await con.execute('UPDATE tasks SET "node_output_name" = ? WHERE "id" = ?',
                                  (process_result.output_name, task_id))
                ui_task_delta.node_output_name = process_result.output_name  # for ui event
            _bench_point_4 = time.perf_counter()

            # note: this may be not obvious, but ALL branches of the next if result in implicit transaction start
            if process_result.do_kill_task:
                await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                  (TaskState.DEAD.value, task_id))
                ui_task_delta.state = TaskState.DEAD  # for ui event
            else:
                if process_result.invocation_job is None:  # if no job to do
                    await con.execute('UPDATE tasks SET "work_data" = ?, "work_data_invocation_attempt" = 0, "state" = ?, "_invoc_requirement_clause" = ? '
                                      'WHERE "id" = ?',
                                      (None, skip_state.value, None,
                                       task_id))
                    ui_task_delta.work_data_invocation_attempt = 0  # for ui event
                    ui_task_delta.state = skip_state  # for ui event
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
                            group_priority = group_priority[0] or 50.0
                    await con.execute('UPDATE tasks SET "work_data" = ?, "work_data_invocation_attempt" = 0, "state" = ?, "_invoc_requirement_clause" = ?, '
                                      'priority = ? '
                                      'WHERE "id" = ?',
                                      (taskdada_serialized, TaskState.READY.value, ':::'.join((invoc_requirements_sql, invoc_requirements_dict_str)),
                                       group_priority + job_priority,
                                       task_id))
                    ui_task_delta.work_data_invocation_attempt = 0  # for ui event
                    ui_task_delta.state = TaskState.READY  # for ui event

            _bench_point_5 = time.perf_counter()
            if process_result.do_split_remove:  # TODO: check that there is no race conditions among splitted tasks to remove the split
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
                    ui_task_delta_split = TaskDelta(task_row['split_origin_task_id'], node_id=task_row['node_id'], state=TaskState.DONE)  # for ui event
                    if process_result.output_name:
                        await con.execute('UPDATE tasks SET "node_output_name" = ? WHERE "id" = ?',
                                          (process_result.output_name, task_row['split_origin_task_id']))
                        ui_task_delta_split.node_output_name = process_result.output_name  # for ui event
                        # so sealed split task will get the same output_name as the task that is sealing the split
                    # and update its attributes if provided
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

            _bench_point_6 = time.perf_counter()
            _bench_point_7 = _bench_point_6
            if process_result.attributes_to_set:  # not None or {}
                attributes = await asyncio.get_event_loop().run_in_executor(None, json.loads, task_row['attributes'] or '{}')
                attributes.update(process_result.attributes_to_set)
                for k, v in process_result.attributes_to_set.items():  # TODO: hmmm, None is a valid value...
                    if v is None:
                        del attributes[k]
                result_serialized = await asyncio.get_event_loop().run_in_executor(None, json.dumps, attributes)
                _bench_point_7 = time.perf_counter()
                await con.execute('UPDATE tasks SET "attributes" = ? WHERE "id" = ?',
                                  (result_serialized, task_id))
            _bench_point_8 = time.perf_counter()

            # process environment resolver arguments if provided
            if (envargs := process_result._environment_resolver_arguments) is not None:
                await con.execute('UPDATE tasks SET environment_resolver_data = ? WHERE "id" = ?',
                                  (await envargs.serialize_async(), task_id))

            _bench_point_9 = time.perf_counter()
            # spawning new tasks after all attributes were set, so children inherit
            # spawn
            if process_result.spawn_list is not None:
                for spawn in process_result.spawn_list:
                    # we do NOT allow spawning children anywhere else but in the same node, and with the task as parent
                    spawn.force_set_node_task_id(task_row['node_id'], task_row['id'])
                await self.scheduler.spawn_tasks(process_result.spawn_list, con=con)

            _bench_point_10 = time.perf_counter()
            # splits
            if process_result._split_attribs is not None:
                split_count = len(process_result._split_attribs)
                for attr_dict, split_task_id in zip(process_result._split_attribs, await self.split_task(task_id, split_count, con)):
                    async with con.execute('SELECT attributes FROM "tasks" WHERE "id" = ?', (split_task_id,)) as cur:
                        split_task_dict = await cur.fetchone()
                    assert split_task_dict is not None
                    split_task_attrs = await asyncio.get_event_loop().run_in_executor(None, json.loads, split_task_dict['attributes'])
                    split_task_attrs.update(attr_dict)
                    await con.execute('UPDATE "tasks" SET attributes = ? WHERE "id" = ?', (json.dumps(split_task_attrs), split_task_id))  # TODO: run dumps in executor

            _bench_point_11 = time.perf_counter()
            con.add_after_commit_callback(self.scheduler.ui_state_access.scheduler_reports_tasks_updated, [ui_task_delta] if ui_task_delta_split is None else [ui_task_delta, ui_task_delta_split])  # ui event
            await con.commit(self.poke)
            _bench_point_12 = time.perf_counter()
        self.__logger.debug(
            f'_awaiter all: {_bench_point_12 - _bench_point_0}: '
            f'process: {_bench_point_0 - _bench_point_1}, '
            f'lock: {_bench_point_1 - _bench_point_2}, '
            f'tran_begin: {_bench_point_3 - _bench_point_2},'
            f'upd_out: {_bench_point_4 - _bench_point_3}, '
            f'upd_rest: {_bench_point_5 - _bench_point_4},'
            f'split_rm: {_bench_point_6 - _bench_point_5}, '
            f'attr_set: {_bench_point_7 - _bench_point_6} + {_bench_point_8 - _bench_point_7}, '
            f'envr_set: {_bench_point_9 - _bench_point_8}, '
            f'spawn: {_bench_point_10 - _bench_point_9}, '
            f'split: {_bench_point_11 - _bench_point_10}, '
            f'commit: {_bench_point_12 - _bench_point_11}')

    # submitter
    @atimeit()
    async def _submitter(self, task_row, worker_row):
        self.__logger.debug(f'submitter started')
        try:
            addr = AddressChain(worker_row['last_address'])
        except ValueError:
            self.__logger.error('error address converting during unexpected here. ping should have cought it')
            addr = None  # set to invalid values to exit in error-checking if a bit below
            # TODO: add nicer error check for invalid address. currently we fail to open connection to None

        task_id = task_row['id']
        ui_task_delta = TaskDelta(task_id)  # for ui event
        work_data = task_row['work_data']
        assert work_data is not None
        task: InvocationJob = await asyncio.get_event_loop().run_in_executor(None, InvocationJob.deserialize, work_data)
        if not task.args():
            async with self.awaiter_lock, self.scheduler.data_access.data_connection() as skipwork_transaction:
                await skipwork_transaction.execute('UPDATE tasks SET state = ? WHERE "id" = ?',
                                                   (TaskState.POST_WAITING.value, task_id))
                await skipwork_transaction.execute('UPDATE workers SET state = ? WHERE "id" = ?',
                                                   (WorkerState.IDLE.value, worker_row['id']))
                # unset resource usage
                await self.scheduler._update_worker_resouce_usage(worker_row['id'], hwid=worker_row['hwid'], connection=skipwork_transaction)
                skipwork_transaction.add_after_commit_callback(self.scheduler.ui_state_access.scheduler_reports_task_updated, TaskDelta(task_id, state=TaskState.POST_WAITING))
                await skipwork_transaction.commit()
                return

        # so task.args() is not None
        async with self.scheduler.data_access.data_connection() as submit_transaction:
            submit_transaction.row_factory = aiosqlite.Row
            async with self.awaiter_lock:
                async with submit_transaction.execute(
                        'INSERT INTO invocations ("task_id", "worker_id", "state", "node_id") VALUES (?, ?, ?, ?)',
                        (task_id, worker_row['id'], InvocationState.INVOKING.value, task_row['node_id'])) as incur:
                    invocation_id = incur.lastrowid  # rowid should be an alias to id, acc to sqlite manual
                await submit_transaction.commit()

            task._set_invocation_id(invocation_id)
            task._set_task_id(task_id)
            async with submit_transaction.execute('SELECT attributes FROM tasks WHERE "id" == ?', (task_id,)) as attcur:
                task_attributes_raw = ((await attcur.fetchone()) or ['{}'])[0]
            task._set_task_attributes(await asyncio.get_event_loop().run_in_executor(None, json.loads, task_attributes_raw))
            self.__logger.debug(f'submitting task to {addr}')
            try:
                # this is potentially a long operation - db must NOT be locked during it
                with WorkerControlClient.get_worker_control_client(addr, self.scheduler.message_processor()) as client:  # type: WorkerControlClient
                    # import random
                    # await asyncio.sleep(random.uniform(0, 8))  # DEBUG! IMITATE HIGH LOAD
                    reply = await client.give_task(task, self.scheduler.server_message_address())
                self.__logger.debug(f'got reply {reply}')
            except Exception as e:
                self.__logger.error('some unexpected error %s %s' % (str(type(e)), str(e)))
                reply = TaskScheduleStatus.FAILED

            async with self.awaiter_lock:
                await submit_transaction.execute('BEGIN IMMEDIATE')
                async with submit_transaction.execute('SELECT "state" FROM workers WHERE "id" == ?', (worker_row['id'],)) as incur:
                    worker_state = WorkerState((await incur.fetchone())[0])
                async with submit_transaction.execute('SELECT "state" FROM invocations WHERE "id" == ?', (invocation_id,)) as incur:
                    # if worker managed to stop and start before we reach this transaction - invocation state will be reset
                    # we have to check it
                    maybe_updated_invocation_state = InvocationState((await incur.fetchone())[0])

                worker_apparently_restarted = False
                if maybe_updated_invocation_state != InvocationState.INVOKING:
                    self.__logger.warning(f'worker seem to have stopped during submission attempt, ignoring, retrying. reply was: {reply}, worker state is: {worker_state}')
                    worker_apparently_restarted = True
                    reply = TaskScheduleStatus.FAILED

                # IF worker state is NOT invoking - then either worker_hello, or worker_bye happened between starting _submitter and here
                if worker_state == WorkerState.OFF:
                    self.__logger.debug('submitter: worker state changed to OFF during submitter work')
                    if reply == TaskScheduleStatus.SUCCESS:
                        self.__logger.warning('submitter succeeded, yet worker state changed to OFF in the middle of submission. forcing reply to FAIL')
                        reply = TaskScheduleStatus.FAILED

                # this assert should never break: as hello preserves INVOKING state, and we catch worker restart case
                assert worker_apparently_restarted or worker_state != WorkerState.IDLE, f'worker restarted={worker_apparently_restarted}, state={worker_state}'

                if reply == TaskScheduleStatus.SUCCESS:
                    await submit_transaction.execute('UPDATE tasks SET state = ?, '
                                                     '"work_data_invocation_attempt" = "work_data_invocation_attempt" + 1 '
                                                     'WHERE "id" = ?',
                                                     (TaskState.IN_PROGRESS.value, task_id))
                    ui_task_delta.state = TaskState.IN_PROGRESS  # for ui event
                    async with submit_transaction.execute('SELECT "work_data_invocation_attempt" FROM tasks WHERE "id" == ?', (task_id,)) as tmpcur:  # TODO: remove this extra query!  # ui event
                        ui_task_delta.work_data_invocation_attempt = (await tmpcur.fetchone())['work_data_invocation_attempt']  # TODO: maybe remove work_data_invocation_attempt from generic ui events/data at all  # ui event
                    await submit_transaction.execute('UPDATE workers SET state = ? WHERE "id" = ?',
                                                     (WorkerState.BUSY.value, worker_row['id']))
                    await submit_transaction.execute('UPDATE invocations SET state = ? WHERE "id" = ?',
                                                     (InvocationState.IN_PROGRESS.value, invocation_id))
                else:  # on anything but success - cancel transaction
                    self.__logger.debug(f'submitter failed, rolling back for wid {worker_row["id"]}')
                    await submit_transaction.execute('UPDATE tasks SET state = ? WHERE "id" = ?',
                                                     (TaskState.READY.value,
                                                      task_id))
                    ui_task_delta.state = TaskState.READY  # for ui event
                    await submit_transaction.execute('UPDATE workers SET state = ? WHERE "id" = ?',
                                                     (WorkerState.IDLE.value if worker_state != WorkerState.OFF else WorkerState.OFF.value,
                                                      worker_row['id']))
                    await submit_transaction.execute('DELETE FROM invocations WHERE "id" = ?',
                                                     (invocation_id,))
                    # update resource usage to none
                    await self.scheduler._update_worker_resouce_usage(worker_row['id'], hwid=worker_row['hwid'], connection=submit_transaction)
                    # TODO: if task was reverted we MIGHT need to poke scheduler after transaction commit to process task again straight awau
                    #  Or something else, have other things to do now, think about this one later
                submit_transaction.add_after_commit_callback(self.scheduler.ui_state_access.scheduler_reports_task_updated, ui_task_delta)  # ui event
                await submit_transaction.commit()

    async def task_processor(self):
        # this will hold references to tasks created with asyncio.create_task
        tasks_to_wait = set()
        stop_task = asyncio.create_task(self._stop_event.wait())
        kick_wait_task = asyncio.create_task(self._poke_event.wait())
        gc_counter = 0
        # tm_counter = 0
        self._main_task_is_ready_now()
        while not self._stop_event.is_set():
            data_access = self.scheduler.data_access
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
                async with data_access.data_connection() as con:
                    con.row_factory = aiosqlite.Row
                    async with con.execute('SELECT "id" FROM invocations WHERE state == ?',
                                           (InvocationState.IN_PROGRESS.value,)) as inv:
                        filtered_invocs = set(x['id'] for x in await inv.fetchall())
                for inv in tuple(data_access.mem_cache_invocations.keys()):
                    if inv not in filtered_invocs:  # Note: since task finish/cancel reporting is in the same thread as this - there will not be race conditions for del, as there's no await
                        del data_access.mem_cache_invocations[inv]
                filtered_invocs.clear()
                # prune done

                self.__logger.debug(f'size of temp db cache: {_gszofdr({1: data_access.mem_cache_invocations, 2: data_access.mem_cache_workers_resources, 3: data_access.mem_cache_workers_state})}')
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
            async with data_access.data_connection() as con:
                con.row_factory = aiosqlite.Row

                for task_state in (TaskState.WAITING, TaskState.READY, TaskState.DONE, TaskState.POST_WAITING, TaskState.SPAWNED):
                    _debug_sel = time.perf_counter()
                    async with con.execute('SELECT tasks.id, tasks.parent_id, tasks.children_count, tasks.active_children_count, tasks.state, {attrs}'
                                           'tasks.node_id, tasks.node_input_name, tasks.node_output_name, tasks.name, tasks.split_level, '
                                           'tasks.work_data, tasks.work_data_invocation_attempt, tasks._invoc_requirement_clause, '
                                           'nodes.type as node_type, nodes.id as node_id, '
                                           'task_splits.split_id as split_id, task_splits.split_element as split_element, task_splits.split_count as split_count, task_splits.origin_task_id as split_origin_task_id '
                                           'FROM tasks INNER JOIN nodes ON tasks.node_id=nodes.id '
                                           'LEFT JOIN task_splits ON tasks.id=task_splits.task_id '
                                           'WHERE (state = ?) '
                                           'AND paused = 0 '
                                           'AND dead = 0 '
                                           'ORDER BY {prio_sort} RANDOM()'.format(
                            prio_sort='tasks.priority DESC, ' if task_state == TaskState.READY else '',
                            attrs='attributes, environment_resolver_data, ' if task_state in (TaskState.WAITING, TaskState.POST_WAITING) else ''
                            ),

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
                                try:
                                    if not (await self.scheduler._get_node_object_by_id(task_row['node_id'])).ready_to_process_task(task_row):
                                        continue
                                except Exception:
                                    self.__logger.exception('a node bugged out on fast ready check. ignoring the check')

                                # await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                #                   (TaskState.GENERATING.value, task_row['id']))
                                set_to_stuff.append((TaskState.GENERATING.value, task_row['id']))
                                total_state_changes += 1
                                # NOTE: awaiters are NOT started here, just coroutines created
                                awaiters.append(self._awaiter((await self.scheduler._get_node_object_by_id(task_row['node_id']))._process_task_wrapper, dict(task_row),
                                                              abort_state=TaskState.WAITING, skip_state=TaskState.POST_WAITING))
                        if set_to_stuff:
                            # ui event
                            con.add_after_commit_callback(
                                self.scheduler.ui_state_access.scheduler_reports_tasks_updated, [TaskDelta(_task_id, state=TaskState(_new_state)) for _new_state, _task_id in set_to_stuff]
                            )
                            #
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
                                if not (await self.scheduler._get_node_object_by_id(task_row['node_id'])).ready_to_postprocess_task(task_row):
                                    continue

                                # await con.execute('UPDATE tasks SET "state" = ? WHERE "id" = ?',
                                #                   (TaskState.POST_GENERATING.value, task_row['id']))
                                set_to_stuff.append((TaskState.POST_GENERATING.value, task_row['id']))
                                total_state_changes += 1

                                awaiters.append(self._awaiter((await self.scheduler._get_node_object_by_id(task_row['node_id']))._postprocess_task_wrapper, dict(task_row),
                                                              abort_state=TaskState.POST_WAITING, skip_state=TaskState.DONE))
                        if set_to_stuff:
                            # ui event
                            con.add_after_commit_callback(
                                self.scheduler.ui_state_access.scheduler_reports_tasks_updated, [TaskDelta(_task_id, state=TaskState(_new_state)) for _new_state, _task_id in set_to_stuff]
                            )
                            #
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
                        ui_task_deltas = []  # for ui event
                        for task_row in all_task_rows:
                            # check max attempts first
                            if task_row['work_data_invocation_attempt'] >= self.__invocation_attempts:
                                state_details = json.dumps({'message': 'maximum invocation attempts reached',
                                                               'happened_at': task_row['state'],
                                                               'type': 'limit',
                                                               'limit_threshold': self.__invocation_attempts,
                                                               'limit_value': task_row['work_data_invocation_attempt']})
                                await con.execute('UPDATE tasks SET "state" = ?, "state_details" = ? WHERE "id" = ?',
                                                  (TaskState.ERROR.value,
                                                   state_details,
                                                   task_row['id']))
                                total_state_changes += 1
                                ui_task_deltas.append(TaskDelta(task_row['id'], state=TaskState.ERROR, state_details=state_details))  # for ui event
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
                                state_details = json.dumps({'message': traceback.format_exc(),
                                                               'happened_at': task_row['state'],
                                                               'type': 'exception',
                                                               'exception_str': str(e),
                                                               'exception_type': str(type(e))})
                                await con.execute('UPDATE tasks SET "state" = ?, "state_details" = ? WHERE "id" = ?',
                                                  (TaskState.ERROR.value,
                                                   state_details,
                                                   task_row['id']))
                                total_state_changes += 1
                                ui_task_deltas.append(TaskDelta(task_row['id'], state=TaskState.ERROR, state_details=state_details))  # for ui event
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
                            ui_task_deltas.append(TaskDelta(task_row['id'], state=TaskState.INVOKING))  # for ui event
                            await con.execute('UPDATE workers SET state = ? WHERE "id" = ?',
                                              (WorkerState.INVOKING.value, worker['id']))
                            # set resource usage straight away
                            try:
                                await self.scheduler._update_worker_resouce_usage(worker['id'], resources=requirements_clause_dict, hwid=worker['hwid'], connection=con)
                            except NotEnoughResources:
                                self.__logger.warning(f'inconsistence in worker resource tracking! could not submit to worker {worker["id"]}')
                                continue

                            submitters.append(self._submitter(dict(task_row), dict(worker)))
                            self.__logger.debug('submitter scheduled')
                        # ui event
                        if ui_task_deltas:
                            con.add_after_commit_callback(
                                self.scheduler.ui_state_access.scheduler_reports_tasks_updated, ui_task_deltas
                            )
                        #
                        await con.commit()
                        for coro in submitters:
                            tasks_to_wait.add(asyncio.create_task(coro))
                    #
                    # means task is done being processed by current node,
                    # now it should be passed to the next node
                    elif task_state == TaskState.DONE or task_state == TaskState.SPAWNED:
                        ui_task_deltas = []  # for ui event
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
                                    # ui event
                                    ui_task_deltas.append(TaskDelta(task_row['id'], node_id=wire['node_id_in'], node_input_name=wire['in_name'],
                                                                    state=TaskState.WAITING))  # for ui event
                                    #
                                else:
                                    for i, splited_task_id in enumerate(await self.split_task(task_row['id'], wire_count, con)):
                                        await con.execute('UPDATE tasks SET node_id = ?, node_input_name = ?, state = ?, work_data = ?'
                                                          'WHERE "id" = ?',
                                                          (all_wires[i]['node_id_in'], all_wires[i]['in_name'], TaskState.WAITING.value, None,
                                                           splited_task_id))
                                        ui_task_deltas.append(TaskDelta(splited_task_id, node_id=all_wires[i]['node_id_in'], node_input_name=all_wires[i]['in_name'],
                                                                        state=TaskState.WAITING))  # for ui event
                                        total_state_changes += 1
                                    total_state_changes += 1  # this is for original (for split) task changing state to SPLITTED

                            else:
                                # the problem is that there are tasks that done, but have no wires to go anywhere
                                # and that is the point, they are done done. But processing thousands of them every time is painful
                                # so we need to somehow prevent them from being amilessly processed
                                # this is a testing desicion, TODO: test and see if thes is a good way to deal with the problem
                                await con.execute('UPDATE "tasks" SET "paused" = 1 WHERE "id" = ?', (task_row['id'],))
                                ui_task_deltas.append(TaskDelta(task_row['id'], paused=True))  # for ui event

                        # ui event
                        if ui_task_deltas:
                            con.add_after_commit_callback(
                                self.scheduler.ui_state_access.scheduler_reports_tasks_updated, ui_task_deltas
                            )
                        #
                        await con.commit()

                    self.__logger.debug(f'{task_state.name} took: {time.perf_counter() - _debug_pstart}')

                # out of processing loop, but still in db connection
                if total_processed == 0:
                    # check maybe it's time to sleep
                    if len(tasks_to_wait) == 0:
                        # instead of NOT IN  here using explicit IN cuz this way db index works # async with con.execute('SELECT COUNT(id) AS total FROM tasks WHERE paused = 0 AND state NOT IN (?, ?)', (TaskState.ERROR.value, TaskState.DEAD.value)) as cur:
                        async with con.execute('SELECT COUNT(id) AS total FROM tasks WHERE paused = 0 AND state IN ({}) AND dead = 0'
                                                       .format(','.join(str(state.value)
                                                                        for state in TaskState
                                                                        if state not in (TaskState.ERROR, TaskState.DEAD, TaskState.SPLITTED)))) as cur:
                            total = await cur.fetchone()
                        if total is None or total['total'] == 0:
                            self.__logger.debug('no useful tasks seem to be available')
                            self.sleep()
                else:
                    self.scheduler.wake()

            processing_time = time.perf_counter() - _debug_con
            if processing_time > 1.0:
                self.__logger.info(f'processing run in {processing_time}')
            else:
                self.__logger.debug(f'processing run in {processing_time}')

            # and wait for a bit

            sleeping_tasks = (stop_task, kick_wait_task)

            wdone, _ = await asyncio.wait(sleeping_tasks, timeout=0 if total_state_changes > 0 else self.__processing_interval * self.__processing_interval_mult,
                                          return_when=asyncio.FIRST_COMPLETED)
            if kick_wait_task in wdone:
                self._reset_poke_event()
                kick_wait_task = asyncio.create_task(self._poke_event.wait())

            # stopping
            if stop_task in wdone:
                break

        #
        # Out of while - means we are stopping. time to save all the nodes
        self.__logger.info('finishing task processor...')
        for task in (stop_task, kick_wait_task):
            if not task.done():
                task.cancel()
        if len(tasks_to_wait) > 0:
            await asyncio.wait(tasks_to_wait, return_when=asyncio.ALL_COMPLETED)
        self.__logger.info('task processor finished')

    # helpers

    async def split_task(self, task_id: int, into: int, con: aiosqlite_overlay.ConnectionWithCallbacks) -> List[int]:
        """
        con is expected to be a opened db connection with dict factory
        :param task_id
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
            next_split_id = 1 + ((await maxsplitcur.fetchone())['m'] or 0)  # TODO: do i need this? can i just rely on AUTOINCREMENT ?
        await con.execute('UPDATE tasks SET state = ? WHERE "id" = ?',
                          (TaskState.SPLITTED.value, task_id))
        con.add_after_commit_callback(self.scheduler.ui_state_access.scheduler_reports_task_updated, TaskDelta(task_id, state=TaskState.SPLITTED))  # ui event
        # await con.execute('INSERT INTO "task_splits" ("split_id", "task_id", "split_element", "split_count", "origin_task_id") VALUES (?,?,?,?,?)',
        #                   (next_split_id, task_row['id'], 0, into, task_id))
        all_split_ids = []
        all_split_data = []
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
            all_split_data.append(TaskData(new_task_id, None, 0, 0, TaskState(task_row['state']), '', False, task_row['node_id'],
                                           task_row['node_input_name'], task_row['node_output_name'], task_row['name'],
                                           new_split_level, 0, None, task_id, next_split_id, None, set(groups)))
        # now increase number of children to the parent of the task being splitted

        assert into == len(all_split_ids)
        con.add_after_commit_callback(self.scheduler.ui_state_access.scheduler_reports_tasks_added, all_split_data)  # ui event
        self.scheduler.wake()  # do we need it here?
        return all_split_ids
