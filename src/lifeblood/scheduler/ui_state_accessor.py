import asyncio
import dataclasses

import aiosqlite
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from dataclasses import dataclass
import time
from enum import Enum
from queue import Queue
from ..logging import get_logger
from ..misc import atimeit, aperformance_measurer
from ..enums import InvocationState, TaskState, TaskGroupArchivedState, WorkerState, WorkerType, UIEventType
from ..exceptions import NotSubscribedError
from ..scheduler_event_log import SchedulerEventLog
from ..ui_events import TaskEvent, TaskFullState, TasksUpdated, TasksRemoved, TasksChanged
from ..ui_protocol_data import TaskBatchData, UiData, TaskGroupData, TaskGroupBatchData, TaskGroupStatisticsData, \
    NodeGraphStructureData, WorkerBatchData, WorkerData, WorkerResources, NodeConnectionData, NodeData, TaskData, TaskDelta
from .scheduler_component_base import SchedulerComponentBase
from .data_access import DataAccess

from typing import Dict, Iterable, List, Optional, Tuple, TYPE_CHECKING, Set, Union

if TYPE_CHECKING:  # TODO: maybe separate a subset of scheduler's methods to smth like SchedulerData class, or idunno, for now no obvious way to separate, so having a reference back
    from .scheduler import Scheduler


class QueueEventType(Enum):
    ADDED = 0
    UPDATED = 1
    REMOVED = 2


@dataclass
class LogSubscription:
    expiration_timestamp: float
    event_log: SchedulerEventLog

    def is_expired(self) -> bool:
        return time.time() >= self.expiration_timestamp


class UIStateAccessor(SchedulerComponentBase):
    def __init__(self, scheduler: "Scheduler"):
        super().__init__(scheduler)
        self.__logger = get_logger('scheduler.ui_state_accessor')
        self.__data_access = scheduler.data_access
        self.__ui_cache = {'groups': {}, 'last_update_time': None}

        self.__global_next_event_id = 0  # "atomic" counter of events

        # config
        self.__housekeeping_interval = 60

        # for ui
        self.__latest_graph_ui_state: Optional[NodeGraphStructureData] = None
        self.__latest_graph_ui_event_id = 0
        #
        # the logic here: ui may request updates for (g1, g2, skip_dead) - so that's our key for the log,
        # full updates go there.
        # but individual task events may go to multiple logs, e.g. to (g1, g2), (g1,), (g0, g1) ...
        self.__task_group_event_logs: Dict[Tuple[Tuple[str, ...], bool], LogSubscription] = {}  # maps keys as requested by viewer to logs
        self.__task_group_to_logs: Dict[str, List[LogSubscription]] = {}  # maps task group to list of corresponding logs
        self.__pruning_tasks = []  # TODO: these tasks need to be cancelled clearly on stop

        self.__task_event_preprocess_queue = asyncio.Queue()
        #
        self.__task_group_mapping: Optional[Dict[int, Set[str]]] = None
        self.__group_task_mapping: Optional[Dict[str, Set[id]]] = None
        self.__task_group_mapping_update_alock = asyncio.Lock()
        self.__requested_task_group_force_refresh = True

        # a pool of pools to execute log commands in. the point is to ensure log always runs on ONE SAME THREAD
        # running log methods on ONE SAME THREAD will ensure lockless race-free work
        self.__pool_list = []
        for i in range(4):  # TODO: config!
            self.__pool_list.append(ThreadPoolExecutor(max_workers=1))

    def _get_next_event_id(self):
        """
        get and inc counter
        """
        eid = self.__global_next_event_id
        self.__global_next_event_id += 1
        return eid

    # scheduler component impl

    def _main_task(self):
        return self.main_task()

    def _my_sleep(self):
        pass

    def _my_wake(self):
        pass

    def sleep(self):  # we don't have concept of sleep here
        pass

    def wake(self):  # we don't have concept of sleep here
        pass

    # housekeeping

    # main task

    async def main_task(self):
        self.__logger.info('ui state accessor started')
        self._main_task_is_ready_now()
        await asyncio.gather(self.house_keeping(), self.process_event_queue())
        self.__logger.info('ui state accessor finished')

    async def house_keeping(self):
        stop_task = asyncio.create_task(self._stop_event.wait())
        wakeup_task = None

        while not self._stop_event.is_set():

            self.prune_event_subscriptions()
            if len(self.__pruning_tasks) > 0:  # prune the pruners
                pruned_tasks = []
                for task in self.__pruning_tasks:
                    if task.done():
                        await task
                    else:
                        pruned_tasks.append(task)
                self.__pruning_tasks = pruned_tasks

            ##

            if wakeup_task is None:
                wakeup_task = asyncio.create_task(self._poke_event.wait())
            sleeping_tasks = (stop_task, wakeup_task)

            done, _ = await asyncio.wait(sleeping_tasks, timeout=self.__housekeeping_interval, return_when=asyncio.FIRST_COMPLETED)
            if wakeup_task in done:
                self._reset_poke_event()
                wakeup_task = None

            # end when stop is set
            if stop_task in done:
                break

        # cleaning up potentially pending tasks
        if not stop_task.done():
            stop_task.cancel()
        if wakeup_task is not None and not wakeup_task.done():
            wakeup_task.cancel()

    async def process_event_queue(self):
        stop_task = asyncio.create_task(self._stop_event.wait())
        while not self._stop_event.is_set():
            get_task = asyncio.create_task(self.__task_event_preprocess_queue.get())

            waiting_tasks = (get_task, stop_task)
            done, _ = await asyncio.wait(waiting_tasks, return_when=asyncio.FIRST_COMPLETED)
            if stop_task in done:
                for task in waiting_tasks:
                    if not task.done():
                        task.cancel()
                break

            queue_event_type, event_id, event_timestamp, element_data = await get_task
            if len(self.__task_group_event_logs) == 0:
                continue

            # only in case of removed - we need to know previous groups
            if queue_event_type == QueueEventType.REMOVED:
                _, groups = element_data
                group_sets = (set(groups),)
            elif queue_event_type in (QueueEventType.REMOVED, QueueEventType.ADDED):
                task_datas, groups = element_data  # type: List[TaskData], Optional[Set[str]]
                if groups is None:
                    group_sets = [x.groups for x in task_datas]
                    # group_sets = await self._get_tasks_groups([x.id for x in task_datas])
                else:
                    group_sets = (set(groups),)
            else:  # otherwise we take fresh groups
                group_sets = await self._get_tasks_groups([x.id for x in element_data])

            affected_logs = set()
            for group_set in group_sets:
                for group in group_set:
                    if group not in self.__task_group_to_logs:
                        continue
                    affected_logs.update(x.event_log for x in self.__task_group_to_logs[group])
            if len(affected_logs) == 0:
                continue

            dbuid = self.__data_access.db_uid
            # time to fetch info and build event
            if queue_event_type == QueueEventType.REMOVED:
                task_ids, _ = element_data
                event = TasksRemoved(dbuid, task_ids)
            elif queue_event_type == QueueEventType.ADDED:
                task_datas, _ = element_data
                event = TasksUpdated(dbuid, TaskBatchData(dbuid, {x.id: x for x in task_datas}))
            elif queue_event_type == QueueEventType.UPDATED:
                event = TasksChanged(dbuid, element_data)
            else:
                raise NotImplementedError('IMPOSSIBRU!')

            # force given event_id, ensure correct ids and timestamps
            event.event_id = event_id
            event.timestamp = event_timestamp
            #  put event into logs
            for log in affected_logs:
                executor = self.__get_executor_for_log(log)
                await asyncio.get_event_loop().run_in_executor(executor, log.add_event, event)

    def prune_event_subscriptions(self):
        for k, v in list(self.__task_group_event_logs.items()):  # type: Tuple[Tuple[str, ...], bool], LogSubscription
            if v.is_expired():
                self.remove_task_event_subscription_if_expired(k)

    #

    #

    @property
    def graph_update_id(self):
        return self.__latest_graph_ui_event_id

    def bump_graph_update_id(self):
        self.__latest_graph_ui_state = None
        self.__latest_graph_ui_event_id += 1

    # @atimeit(0.005)
    # async def get_full_ui_state(self, task_groups: Optional[Iterable[str]] = None, skip_dead=True, skip_archived_groups=True) -> UiData:
    #     self.__logger.debug(f'full update for {task_groups}')
    #     now = datetime.now()
    #     group_totals_update_interval = 5
    #     async with self.__data_access.data_connection() as con:
    #         con.row_factory = aiosqlite.Row
    #         async with con.execute('SELECT "id", "type", "name" FROM "nodes"') as cur:
    #             all_nodes = {x['id']: dict(x) for x in await cur.fetchall()}
    #         async with con.execute('SELECT * FROM "node_connections"') as cur:
    #             all_conns = {x['id']: dict(x) for x in await cur.fetchall()}
    #         if not task_groups:  # None or []
    #             all_tasks = dict()
    #             # async with con.execute('SELECT tasks.*, task_splits.origin_task_id, task_splits.split_id, GROUP_CONCAT(task_groups."group") as groups, invocations.progress '
    #             #                        'FROM "tasks" '
    #             #                        'LEFT JOIN "task_splits" ON tasks.id=task_splits.task_id AND tasks.split_level=task_splits.split_level '
    #             #                        'LEFT JOIN "task_groups" ON tasks.id=task_groups.task_id '
    #             #                        'LEFT JOIN "invocations" ON tasks.id=invocations.task_id AND invocations.state = %d '
    #             #                        'GROUP BY tasks."id"' % InvocationState.IN_PROGRESS.value) as cur:
    #             #     all_tasks_rows = await cur.fetchall()
    #             # for task_row in all_tasks_rows:
    #             #     task = dict(task_row)
    #             #     if task['groups'] is None:
    #             #         task['groups'] = set()
    #             #     else:
    #             #         task['groups'] = set(task['groups'].split(','))  # TODO: enforce no commas (,) in group names
    #             #     all_tasks[task['id']] = task
    #         else:
    #             all_tasks = dict()
    #             for group in task_groups:
    #                 # _dbg = time.perf_counter()
    #                 async with con.execute('SELECT tasks.id, tasks.parent_id, tasks.children_count, tasks.active_children_count, tasks.state, tasks.state_details, tasks.paused, tasks.node_id, '
    #                                        'tasks.node_input_name, tasks.node_output_name, tasks.name, tasks.split_level, tasks.work_data_invocation_attempt, '
    #                                        'task_splits.origin_task_id, task_splits.split_id, invocations."id" as invoc_id, GROUP_CONCAT(task_groups."group") as groups '
    #                                        'FROM "tasks" '
    #                                        'LEFT JOIN "task_groups" ON tasks.id=task_groups.task_id AND task_groups."group" == ?'
    #                                        'LEFT JOIN "task_splits" ON tasks.id=task_splits.task_id '
    #                                        'LEFT JOIN "invocations" ON tasks.id=invocations.task_id AND invocations.state = ? '
    #                                        'WHERE task_groups."group" == ? AND tasks.dead {dodead} '
    #                                        'GROUP BY tasks."id"'.format(dodead=f'== 0' if skip_dead else 'IN (0,1)'),
    #                                        (group, InvocationState.IN_PROGRESS.value, group)) as cur:  # NOTE: if you change = to LIKE - make sure to GROUP_CONCAT groups too
    #                     grp_tasks = await cur.fetchall()
    #                 # print(f'fetch groups: {time.perf_counter() - _dbg}')
    #                 for task_row in grp_tasks:
    #                     task = dict(task_row)
    #                     task['progress'] = self.__data_access.mem_cache_invocations.get(task['invoc_id'], {}).get('progress', None)
    #                     task['groups'] = set(task['groups'].split(','))
    #                     if task['id'] in all_tasks:
    #                         all_tasks[task['id']]['groups'].update(task['groups'])
    #                     else:
    #                         all_tasks[task['id']] = task
    #         # _dbg = time.perf_counter()
    #         #async with con.execute('SELECT DISTINCT task_groups."group", task_group_attributes.ctime FROM task_groups LEFT JOIN task_group_attributes ON task_groups."group" = task_group_attributes."group"') as cur:
    #
    #         # some things are updated onlt once in a while, not on every update
    #         need_group_totals_update = (now - (self.__ui_cache.get('last_update_time', None) or datetime.fromtimestamp(0))).total_seconds() > group_totals_update_interval
    #         #async with con.execute('SELECT "group", "ctime", "state", "priority" FROM task_group_attributes' + (f' WHERE state == {TaskGroupArchivedState.NOT_ARCHIVED.value}' if skip_archived_groups else '')) as cur:
    #         if need_group_totals_update:
    #             sqlexpr =  'SELECT "group", "ctime", "state", "priority", tdone, tprog, terr, tall FROM task_group_attributes ' \
    #                        'LEFT JOIN ' \
    #                       f'(SELECT SUM(state=={TaskState.DONE.value}) as tdone, ' \
    #                        f'       SUM(state=={TaskState.IN_PROGRESS.value}) as tprog, ' \
    #                        f'       SUM(state=={TaskState.ERROR.value}) as terr, ' \
    #                        f'       COUNT() as tall, "group" as grp FROM tasks JOIN task_groups ON tasks."id"==task_groups.task_id WHERE tasks.dead==0 GROUP BY "group") ' \
    #                        'ON "grp"==task_group_attributes."group" ' \
    #                        + (f' WHERE state == {TaskGroupArchivedState.NOT_ARCHIVED.value}' if skip_archived_groups else '')
    #         else:
    #             sqlexpr = 'SELECT "group", "ctime", "state", "priority" FROM task_group_attributes' + (f' WHERE state == {TaskGroupArchivedState.NOT_ARCHIVED.value}' if skip_archived_groups else '')
    #         async with con.execute(sqlexpr) as cur:
    #             all_task_groups = {x['group']: dict(x) for x in await cur.fetchall()}
    #         if need_group_totals_update:
    #             self.__ui_cache['last_update_time'] = now
    #             self.__ui_cache['groups'] = {group: {k: attrs[k] for k in ('tdone', 'tprog', 'terr', 'tall')} for group, attrs in all_task_groups.items()}
    #         else:
    #             for group in all_task_groups:
    #                 all_task_groups[group].update(self.__ui_cache['groups'].get(group, {}))
    #
    #         # print(f'distinct groups: {time.perf_counter() - _dbg}')
    #         # _dbg = time.perf_counter()
    #         async with con.execute('SELECT workers."id", '
    #                                'cpu_count, '
    #                                'total_cpu_count, '
    #                                'cpu_mem, '
    #                                'total_cpu_mem, '
    #                                'gpu_count, '
    #                                'total_gpu_count, '
    #                                'gpu_mem, '
    #                                'total_gpu_mem, '
    #                                'workers."hwid", '
    #                                'last_address, workers."state", worker_type, invocations.node_id, invocations.task_id, invocations."id" as invoc_id, '
    #                                'GROUP_CONCAT(worker_groups."group") as groups '
    #                                'FROM workers '
    #                                'LEFT JOIN invocations ON workers."id" == invocations.worker_id AND invocations."state" == 0 '
    #                                'LEFT JOIN worker_groups ON workers."hwid" == worker_groups.worker_hwid '
    #                                'LEFT JOIN resources ON workers.hwid == resources.hwid '
    #                                'GROUP BY workers."id"') as cur:
    #             all_workers = {x['id']: x for x in ({**dict(x),
    #                                                  'last_seen': self.__data_access.mem_cache_workers_state[x['id']]['last_seen'],
    #                                                  'progress': self.__data_access.mem_cache_invocations.get(x['invoc_id'], {}).get('progress', None)
    #                                                  } for x in await cur.fetchall())}
    #             for worker_data in all_workers.values():
    #                 worker_data['groups'] = set(worker_data['groups'].split(',')) if worker_data['groups'] else set()
    #         # print(f'workers: {time.perf_counter() - _dbg}')
    #     return await asyncio.get_event_loop().run_in_executor(None, _create_uidata_from_raw_noasync,
    #                                                           self.__data_access.db_uid,
    #                                                           all_nodes,
    #                                                           all_conns,
    #                                                           all_tasks,
    #                                                           all_workers,
    #                                                           all_task_groups)

    # ui query functions

    async def get_task_groups_ui_state(self, fetch_statistics=False, skip_archived_groups=True, offset=0, limit=-1) -> TaskGroupBatchData:
        self.__logger.debug(f'tasks groups update for offset={offset}, limit={"unlim" if limit < 0 else limit}')
        # group_totals_update_interval = 5
        # now = datetime.now()
        async with self.__data_access.data_connection() as con, \
                aperformance_measurer(threshold_to_report=0.005, name='get_task_groups_ui_state'):
            con.row_factory = aiosqlite.Row
            # need_group_totals_update = (now - (self.__ui_cache.get('last_update_time', None) or datetime.fromtimestamp(0))).total_seconds() > group_totals_update_interval
            # fetch_statistics = fetch_statistics and need_group_totals_update
            if fetch_statistics:
                sqlexpr = 'SELECT "group", "ctime", "state", "priority", tdone, tprog, terr, tall FROM task_group_attributes ' \
                          'LEFT JOIN ' \
                          f'(SELECT SUM(state=={TaskState.DONE.value}) as tdone, ' \
                          f'       SUM(state=={TaskState.IN_PROGRESS.value}) as tprog, ' \
                          f'       SUM(state=={TaskState.ERROR.value}) as terr, ' \
                          f'       COUNT() as tall, "group" as grp FROM tasks JOIN task_groups ON tasks."id"==task_groups.task_id WHERE tasks.dead==0 GROUP BY "group") ' \
                          'ON "grp"==task_group_attributes."group" ' \
                          + (f' WHERE state == {TaskGroupArchivedState.NOT_ARCHIVED.value}' if skip_archived_groups else '')
            else:
                sqlexpr = 'SELECT "group", "ctime", "state", "priority" FROM task_group_attributes' + (f' WHERE state == {TaskGroupArchivedState.NOT_ARCHIVED.value}' if skip_archived_groups else '')
            if offset > 0 or limit >= 0:
                sqlexpr += f' LIMIT {int(limit)} OFFSET {int(offset)}'
            async with con.execute(sqlexpr) as cur:
                all_task_groups = {x['group']: dict(x) for x in await cur.fetchall()}

            # # now update cache or pull statistics from cache
            # if fetch_statistics:
            #     self.__ui_cache['last_update_time'] = now
            #     self.__ui_cache['groups'] = {group: {k: attrs[k] for k in ('tdone', 'tprog', 'terr', 'tall')} for group, attrs in all_task_groups.items()}
            # elif fetch_statistics:  # if fetch requested, but cache is still valid - get cache
            #     for group in all_task_groups:
            #         all_task_groups[group].update(self.__ui_cache['groups'].get(group, {}))

        return await asyncio.get_event_loop().run_in_executor(None, _pack_task_groups, self.__data_access.db_uid, all_task_groups)

    async def get_tasks_ui_state(self, task_groups: Optional[Iterable[str]] = None, skip_dead=True) -> TaskBatchData:
        self.__logger.debug(f'tasks update for {task_groups}')
        async with self.__data_access.data_connection() as con, \
                aperformance_measurer(threshold_to_report=0.005, name='get_tasks_ui_state'):
            con.row_factory = aiosqlite.Row

            all_tasks = dict()
            for group in task_groups:
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
                    task['progress'] = self.__data_access.mem_cache_invocations.get(task['invoc_id'], {}).get('progress', None)
                    task['groups'] = set(task['groups'].split(','))
                    if task['id'] in all_tasks:
                        all_tasks[task['id']]['groups'].update(task['groups'])
                    else:
                        all_tasks[task['id']] = task

        return await asyncio.get_event_loop().run_in_executor(None, _pack_tasks_data, self.__data_access.db_uid, all_tasks)

    async def get_nodes_ui_state(self) -> NodeGraphStructureData:
        if self.__latest_graph_ui_state is None:
            self.__logger.debug('nodes update')
            async with self.__data_access.data_connection() as con, \
                    aperformance_measurer(threshold_to_report=0.005, name='get_nodes_ui_state'):
                con.row_factory = aiosqlite.Row
                async with con.execute('SELECT "id", "type", "name" FROM "nodes"') as cur:
                    all_nodes = {x['id']: dict(x) for x in await cur.fetchall()}
                async with con.execute('SELECT * FROM "node_connections"') as cur:
                    all_conns = {x['id']: dict(x) for x in await cur.fetchall()}

            self.__latest_graph_ui_state = await asyncio.get_event_loop().run_in_executor(None, _pack_nodes_connections_data, self.__data_access.db_uid, all_nodes, all_conns)
        return self.__latest_graph_ui_state

    async def get_workers_ui_state(self) -> WorkerBatchData:
        self.__logger.debug('workers update')
        async with self.__data_access.data_connection() as con, \
                aperformance_measurer(threshold_to_report=0.005, name='get_workers_ui_state'):
            con.row_factory = aiosqlite.Row
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
                all_workers = {x['id']: x for x in ({**dict(x),
                                                     'last_seen': self.__data_access.mem_cache_workers_state[x['id']]['last_seen'],
                                                     'progress': self.__data_access.mem_cache_invocations.get(x['invoc_id'], {}).get('progress', None)
                                                     } for x in await cur.fetchall())}
                for worker_data in all_workers.values():
                    worker_data['groups'] = set(worker_data['groups'].split(',')) if worker_data['groups'] else set()

        return await asyncio.get_event_loop().run_in_executor(None, _pack_workers_from_raw, self.__data_access.db_uid, all_workers)

    #
    # task group mapping related crap
    #

    def force_refresh_task_group_mapping(self):
        """
        call this if you know task groups changed.
         you DON'T NEED TO call this if tasks were added or removed together with their groups -
         because calls to scheduler_reports_task_added scheduler_reports_task_removed_from_group will call this method anyway
        """
        self.__requested_task_group_force_refresh = True
        self.__task_group_mapping = None
        self.__group_task_mapping = None

    async def __refetch_groups(self):
        """
        refetch __task_group_mapping from database
        """
        while self.__requested_task_group_force_refresh:  # if it was set again during awaits inside
            self.__requested_task_group_force_refresh = False
            async with self.__task_group_mapping_update_alock:
                async with self.__data_access.data_connection() as con, \
                        aperformance_measurer(threshold_to_report=0.005, name='get_workers_ui_state'):
                    con.row_factory = aiosqlite.Row
                    async with con.execute('SELECT task_id, "group" FROM task_groups') as cur:
                        rows = await cur.fetchall()

                def _do():
                    d = {}
                    di = {}
                    for row in rows:
                        d.setdefault(row['task_id'], set()).add(row['group'])
                        di.setdefault(row['group'], set()).add(row['task_id'])
                    return d, di

                self.__task_group_mapping, self.__group_task_mapping = await asyncio.get_event_loop().run_in_executor(None, _do)

    async def _get_tasks_groups(self, task_ids: List[int]) -> List[Set[str]]:
        if self.__task_group_mapping is None:
            await self.__refetch_groups()
        ret = []
        for task_id in task_ids:
            ret.append(self.__task_group_mapping.get(task_id) or set())
        return ret

    async def _get_group_tasks(self, group: str) -> Set[int]:
        if self.__group_task_mapping is None:
            await self.__refetch_groups()
        return self.__group_task_mapping.get(group, None)
    #

    def scheduler_reports_tasks_added(self, task_datas: List[TaskData], added_where: Optional[Set[str]] = None):
        """
        task info will be queried from DB
        """
        self.force_refresh_task_group_mapping()
        if len(self.__task_group_event_logs) == 0:
            return
        eid = self._get_next_event_id()
        ets = time.time_ns()
        self.__task_event_preprocess_queue.put_nowait((QueueEventType.ADDED, eid, ets, (task_datas, added_where)))

    def scheduler_reports_task_added(self, task_data: TaskData, added_where: Optional[Set[str]] = None):
        """
        if task_raw is None - it will be fetched from DB
        """
        self.scheduler_reports_tasks_added([task_data], added_where)

    def scheduler_reports_tasks_updated(self, task_deltas: List[TaskDelta]):
        if len(self.__task_group_event_logs) == 0:
            return
        eid = self._get_next_event_id()
        ets = time.time_ns()
        self.__task_event_preprocess_queue.put_nowait((QueueEventType.UPDATED, eid, ets, task_deltas))

    def scheduler_reports_task_updated(self, task_delta: TaskDelta):
        self.scheduler_reports_tasks_updated([task_delta])

    def scheduler_reports_tasks_removed_from_group(self, task_ids: List[int], groups: Iterable[str]):
        self.force_refresh_task_group_mapping()
        if len(self.__task_group_event_logs) == 0:
            return
        eid = self._get_next_event_id()
        ets = time.time_ns()
        self.__task_event_preprocess_queue.put_nowait((QueueEventType.REMOVED, eid, ets, (task_ids, groups)))

    def scheduler_reports_task_groups_added(self, groups):
        self.force_refresh_task_group_mapping()

    def scheduler_reports_task_groups_changed(self, groups):  # TODO: this has to be changed, for now it's way too broad
        self.force_refresh_task_group_mapping()

    def scheduler_reports_task_groups_removed(self, groups):
        self.force_refresh_task_group_mapping()

    #

    def __get_executor_for_log(self, log) -> ThreadPoolExecutor:
        return self.__pool_list[hash(log) % len(self.__pool_list)]

    async def subscribe_to_task_events_for_groups(self, task_groups: Iterable[str], skip_dead: bool, subscribe_for_seconds: float) -> Tuple[TaskEvent, ...]:
        """

        :param task_groups:
        :param skip_dead:
        :param subscribe_for_seconds:
        :return:
        """
        last_race_check_id = -1
        group_key = (tuple(sorted(task_groups)), skip_dead)
        if group_key in self.__task_group_event_logs:  # if so - update
            self.__task_group_event_logs[group_key].expiration_timestamp = time.time() + subscribe_for_seconds
            self.__logger.debug(f'ui subscription to "{group_key}" was prolonged')

            log = self.__task_group_event_logs[group_key].event_log
            executor = self.__get_executor_for_log(log)
            events = await asyncio.get_event_loop().run_in_executor(executor, log.get_since_event, -1, True)  # try to see if we already have a chain of events starting with full state update

            if len(events) > 0 and events[0].event_type == UIEventType.FULL_STATE:  # so if we have a set of events starting with full state
                return events
            self.__logger.debug('no FULL_STATE event in log, reissuing full ui update event')
            if len(events) > 0:
                last_race_check_id = events[-1].event_id
        else:
            log_sub = LogSubscription(time.time() + subscribe_for_seconds, SchedulerEventLog(log_time_length_max=60))
            self.__task_group_event_logs[group_key] = log_sub
            for group in task_groups:
                self.__task_group_to_logs.setdefault(group, []).append(log_sub)

            # now add pruning task
            async def _timed_prune():
                while (log_sub := self.__task_group_event_logs.get(group_key)) is not None:
                    await asyncio.sleep((log_sub.expiration_timestamp - time.time()) * 1.1)  # 1.1 just cuz
                    self.remove_task_event_subscription_if_expired(group_key)

            self.__pruning_tasks.append(asyncio.create_task(_timed_prune()))
            self.__logger.debug(f'ui subscription to "{group_key}" was added')

        log = self.__task_group_event_logs[group_key].event_log
        executor = self.__get_executor_for_log(log)

        # there's no way i can think of to guarantee no race conditions between full state query and other events happening
        # in such case we cannot order full update properly, no way to tell what happened first
        # so for now we just detect (it's not even a guaranteed detect...) if other events happen during full update
        eid = self._get_next_event_id()  # it seeeems that this will be the least destructive way. we risk having older events after full update, but at least final picture will be correct
        ets = time.time_ns()
        state = await self.get_tasks_ui_state(task_groups, skip_dead)
        planck_events = log.get_since_event(last_race_check_id)  # no need for executor - this should be fast, and we don't want to poke event loop yet
        if len(planck_events) > 0:
            self.__logger.warning(f'full state race condition detected! could not order events: {eid}, {[e.event_id for e in planck_events]}')

        state_event = TaskFullState(self.__data_access.db_uid, state)
        state_event.event_id = eid
        state_event.timestamp = ets
        # even though there might be older events in the queue - since we have strict global event id - the state_event will be put into a proper place
        await asyncio.get_event_loop().run_in_executor(executor, log.add_event, state_event)

        return state_event,

    def has_task_event_subscription(self, key: Tuple[Tuple[str, ...], bool]) -> bool:
        return key in self.__task_group_event_logs

    def remove_task_event_subscription_if_expired(self, key: Tuple[Tuple[str, ...], bool]) -> bool:
        """
        removes subscription key ONLY if it's expired
        returns True if subscription was indeed removed
        """
        if (log_sub := self.__task_group_event_logs.get(key)) and log_sub is not None and log_sub.is_expired():
            self.__task_group_event_logs.pop(key)
            for group in key[0]:
                self.__task_group_to_logs[group].remove(log_sub)
                if len(self.__task_group_to_logs[group]) == 0:
                    self.__task_group_to_logs.pop(group)
            self.__logger.debug(f'ui subscription to "{key}" was removed')
            return True
        return False

    async def get_events_for_groups_since_event_id(self, task_groups: Iterable[str], skip_dead: bool, last_known_event_id: int) -> Tuple[TaskEvent, ...]:
        group_key = (tuple(sorted(task_groups)), skip_dead)
        if group_key not in self.__task_group_event_logs:
            raise NotSubscribedError()
        log = self.__task_group_event_logs[group_key].event_log
        executor = self.__get_executor_for_log(log)
        events = await asyncio.get_event_loop().run_in_executor(executor, log.get_since_event, last_known_event_id, True)
        return events

    def subscriptions_count(self):
        return len(self.__task_group_event_logs)

# scheduler helpers

def _pack_workers_from_raw(db_uid: int, ui_workers: dict) -> "WorkerBatchData":
    """
    this is scheduler helper function, it's incoming data format is dictated purely by scheduler
    """
    workers = {}
    for worker_id, worker_raw in ui_workers.items():
        assert worker_id == worker_raw['id']
        res = WorkerResources(worker_raw['cpu_count'], worker_raw['total_cpu_count'],
                              worker_raw['cpu_mem'], worker_raw['total_cpu_mem'],
                              worker_raw['gpu_count'], worker_raw['total_gpu_count'],
                              worker_raw['gpu_mem'], worker_raw['total_gpu_mem'])
        workers[worker_id] = WorkerData(worker_id, res, str(worker_raw['hwid']), worker_raw['last_address'], worker_raw['last_seen'],
                                        WorkerState(worker_raw['state']), WorkerType(worker_raw['worker_type']),
                                        worker_raw['node_id'], worker_raw['task_id'], worker_raw['invoc_id'], worker_raw['progress'],
                                        worker_raw['groups'])

    return WorkerBatchData(db_uid, workers)


def _pack_nodes_connections_data(db_uid: int, ui_nodes, ui_connections) -> "NodeGraphStructureData":
    if ui_nodes is None or ui_connections is None:
        if ui_connections is not None or ui_connections is not None:
            raise RuntimeError('both ui_nodes and ui_connections must be none, or not none')
    nodes = {}
    connections = {}
    for node_id, node_raw in ui_nodes.items():
        assert node_id == node_raw['id']
        node_data = NodeData(node_id, node_raw['name'], node_raw['type'])
        nodes[node_id] = node_data
    for conn_id, con_raw in ui_connections.items():
        assert conn_id == con_raw['id']
        conn_data = NodeConnectionData(conn_id, con_raw['node_id_in'], con_raw['in_name'], con_raw['node_id_out'], con_raw['out_name'])
        connections[conn_id] = conn_data

    return NodeGraphStructureData(db_uid, nodes, connections)


def _pack_task_data(task_id, task_raw: dict) -> "TaskData":
    return TaskData(task_id, task_raw['parent_id'], task_raw['children_count'], task_raw['active_children_count'],
           TaskState(task_raw['state']), task_raw['state_details'], task_raw['paused'] != 0, task_raw['node_id'],
           task_raw['node_input_name'], task_raw['node_output_name'], task_raw['name'], task_raw['split_level'],
           task_raw['work_data_invocation_attempt'], task_raw['progress'], task_raw['origin_task_id'],
           task_raw['split_id'], task_raw['invoc_id'], task_raw['groups'])


def _pack_tasks_data(db_uid: int, ui_tasks: Union[dict, List[dict]]) -> "TaskBatchData":
    tasks = {}
    if isinstance(ui_tasks, dict):
        for task_id, task_raw in ui_tasks.items():
            assert task_id == task_raw['id']
            task_data = _pack_task_data(task_id, task_raw)
            tasks[task_id] = task_data
    elif isinstance(ui_tasks, list):
        for task_raw in ui_tasks:
            task_id = task_raw['id']
            task_data = _pack_task_data(task_id, task_raw)
            tasks[task_id] = task_data

    return TaskBatchData(db_uid, tasks)


def _pack_task_groups(db_uid: int, all_task_groups) -> "TaskGroupBatchData":
    task_groups = {}
    for group_name, group_raw in all_task_groups.items():
        assert group_name == group_raw['group']
        if 'tdone' in group_raw:  # if has stat:
            stat = TaskGroupStatisticsData(group_raw['tdone'], group_raw['tprog'], group_raw['terr'], group_raw['tall'])
        else:
            stat = None
        task_groups[group_name] = TaskGroupData(group_name, group_raw['ctime'], TaskGroupArchivedState(group_raw['state']),
                                                group_raw['priority'], stat)

    return TaskGroupBatchData(db_uid, task_groups)


def _create_uidata_from_raw_noasync(db_uid, ui_nodes, ui_connections, ui_tasks, ui_workers, all_task_groups):

    node_graph_data = _pack_nodes_connections_data(db_uid, ui_nodes, ui_connections)
    tasks = _pack_tasks_data(db_uid, ui_tasks)
    worker_batch_data = _pack_workers_from_raw(db_uid, ui_workers)
    task_groups = _pack_task_groups(db_uid, all_task_groups)

    return UiData(db_uid, node_graph_data, tasks, worker_batch_data, task_groups)
