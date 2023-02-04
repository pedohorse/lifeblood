import asyncio
import aiosqlite
from datetime import datetime
from ..logging import get_logger
from ..misc import atimeit
from ..enums import InvocationState, TaskState, TaskGroupArchivedState, WorkerState, WorkerType
from ..ui_protocol_data import TaskBatchData, UiData, TaskGroupData, TaskGroupBatchData, TaskGroupStatisticsData, \
    NodeGraphStructureData, WorkerBatchData, WorkerData, WorkerResources, NodeConnectionData, NodeData, TaskData
from .data_access import DataAccess

from typing import Iterable, Optional


class UIStateAccessor:
    def __init__(self, data_accessor: DataAccess):
        self.__logger = get_logger('scheduler.ui_state_accessor')
        self.__data_access = data_accessor
        self.__ui_cache = {'groups': {}, 'last_update_time': None}

    @atimeit(0.005)
    async def get_full_ui_state(self, task_groups: Optional[Iterable[str]] = None, skip_dead=True, skip_archived_groups=True) -> UiData:
        self.__logger.debug('full update for %s', task_groups)
        now = datetime.now()
        group_totals_update_interval = 5
        async with self.__data_access.data_connection() as con:
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
                        task['progress'] = self.__data_access.mem_cache_invocations.get(task['invoc_id'], {}).get('progress', None)
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
                all_workers = {x['id']: x for x in ({**dict(x),
                                                     'last_seen': self.__data_access.mem_cache_workers_state[x['id']]['last_seen'],
                                                     'progress': self.__data_access.mem_cache_invocations.get(x['invoc_id'], {}).get('progress', None)
                                                     } for x in await cur.fetchall())}
                for worker_data in all_workers.values():
                    worker_data['groups'] = set(worker_data['groups'].split(',')) if worker_data['groups'] else set()
            # print(f'workers: {time.perf_counter() - _dbg}')
        return await asyncio.get_event_loop().run_in_executor(None, _create_uidata_from_raw_noasync,
                                                              self.__data_access.db_uid,
                                                              all_nodes,
                                                              all_conns,
                                                              all_tasks,
                                                              all_workers,
                                                              all_task_groups)

    @atimeit(0.005)
    async def get_task_groups_ui_state(self, fetch_statistics=False, skip_archived_groups=True, offset=0, limit=-1) -> TaskGroupBatchData:
        self.__logger.debug('tasks groups update for %s')
        group_totals_update_interval = 5
        now = datetime.now()
        async with self.__data_access.data_connection() as con:
            need_group_totals_update = (now - (self.__ui_cache.get('last_update_time', None) or datetime.fromtimestamp(0))).total_seconds() > group_totals_update_interval
            fetch_statistics = fetch_statistics and need_group_totals_update
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

            # now update cache or pull statistics from cache
            if fetch_statistics:
                self.__ui_cache['last_update_time'] = now
                self.__ui_cache['groups'] = {group: {k: attrs[k] for k in ('tdone', 'tprog', 'terr', 'tall')} for group, attrs in all_task_groups.items()}
            elif fetch_statistics:  # if fetch requested, but cache is still valid - get cache
                for group in all_task_groups:
                    all_task_groups[group].update(self.__ui_cache['groups'].get(group, {}))

        return await asyncio.get_event_loop().run_in_executor(None, _pack_task_groups, all_task_groups)

    @atimeit(0.005)
    async def get_tasks_ui_state(self, task_groups: Optional[Iterable[str]] = None, skip_dead=True) -> TaskBatchData:
        self.__logger.debug('tasks update for %s', task_groups)
        async with self.__data_access.data_connection() as con:
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

        return await asyncio.get_event_loop().run_in_executor(None, _pack_tasks_data, all_tasks)

    @atimeit(0.005)
    async def get_nodes_ui_state(self) -> NodeGraphStructureData:
        self.__logger.debug('nodes update')
        async with self.__data_access.data_connection() as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT "id", "type", "name" FROM "nodes"') as cur:
                all_nodes = {x['id']: dict(x) for x in await cur.fetchall()}
            async with con.execute('SELECT * FROM "node_connections"') as cur:
                all_conns = {x['id']: dict(x) for x in await cur.fetchall()}

        return await asyncio.get_event_loop().run_in_executor(None, _pack_nodes_connections_data, all_nodes, all_conns)

    @atimeit(0.005)
    async def get_workers_ui_state(self) -> WorkerBatchData:
        self.__logger.debug('workers update')
        async with self.__data_access.data_connection() as con:
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

        return await asyncio.get_event_loop().run_in_executor(None, _pack_workers_from_raw, worker_data)


# scheduler helpers

def _pack_workers_from_raw(ui_workers: dict) -> "WorkerBatchData":
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

    return WorkerBatchData(workers)


def _pack_nodes_connections_data(ui_nodes, ui_connections) -> "NodeGraphStructureData":
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

    return NodeGraphStructureData(nodes, connections)


def _pack_tasks_data(ui_tasks) -> "TaskBatchData":
    tasks = {}
    for task_id, task_raw in ui_tasks.items():
        assert task_id == task_raw['id']
        task_data = TaskData(task_id, task_raw['parent_id'], task_raw['children_count'], task_raw['active_children_count'],
                             TaskState(task_raw['state']), task_raw['state_details'], task_raw['paused'] != 0, task_raw['node_id'],
                             task_raw['node_input_name'], task_raw['node_output_name'], task_raw['name'], task_raw['split_level'],
                             task_raw['work_data_invocation_attempt'], task_raw['progress'], task_raw['origin_task_id'],
                             task_raw['split_id'], task_raw['invoc_id'], task_raw['groups'])
        tasks[task_id] = task_data

    return TaskBatchData(tasks)


def _pack_task_groups(all_task_groups) -> "TaskGroupBatchData":
    task_groups = {}
    for group_name, group_raw in all_task_groups.items():
        assert group_name == group_raw['group']
        stat = TaskGroupStatisticsData(group_raw['tdone'], group_raw['tprog'], group_raw['terr'], group_raw['tall'])
        task_groups[group_name] = TaskGroupData(group_name, group_raw['ctime'], TaskGroupArchivedState(group_raw['state']),
                                                group_raw['priority'], stat)

    return TaskGroupBatchData(task_groups)


def _create_uidata_from_raw_noasync(db_uid, ui_nodes, ui_connections, ui_tasks, ui_workers, all_task_groups):

    node_graph_data = _pack_nodes_connections_data(ui_nodes, ui_connections)
    tasks = _pack_tasks_data(ui_tasks)
    worker_batch_data = _pack_workers_from_raw(ui_workers)
    task_groups = _pack_task_groups(all_task_groups)

    return UiData(db_uid, node_graph_data, tasks, worker_batch_data, task_groups)
