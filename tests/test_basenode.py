import os.path
import random
import sqlite3
from unittest import IsolatedAsyncioTestCase
import tempfile
from lifeblood.node_plugin_base import BaseNodeWithTaskRequirements, ProcessingResult
from lifeblood.worker_resource_definition import WorkerResourceDefinition, WorkerResourceDataType
from lifeblood.processingcontext import ProcessingContext
from lifeblood.invocationjob import InvocationJob
from lifeblood.enums import WorkerType
from lifeblood.scheduler.data_access import DataAccess
from lifeblood_testing_common.scheduler_config_provider_default_override import SchedulerConfigProviderOverrides


class NoNodeWithReq(BaseNodeWithTaskRequirements):
    @classmethod
    def label(cls) -> str:
        return 'noop'

    @classmethod
    def type_name(cls) -> str:
        return 'testing.noop'

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        ij = InvocationJob(['nop'])
        return ProcessingResult(ij)


class TestBaseNodes(IsolatedAsyncioTestCase):
    async def test_requirements(self):
        node = NoNodeWithReq('foo')

        node.set_param_value('__requirements__.res', 4)
        node.set_param_value('__requirements__.name_res_0', "cpu_count")
        node.set_param_value('__requirements__.type_res_0', 0)  # float
        node.set_param_value('__requirements__.f_min_res_0', 1.2)
        node.set_param_value('__requirements__.f_pref_res_0', 2.3)
        node.set_param_value('__requirements__.name_res_1', "cpu_mem_b")
        node.set_param_value('__requirements__.type_res_1', 0)  # float
        node.set_param_value('__requirements__.f_min_res_1', 5600000000)
        node.set_param_value('__requirements__.f_pref_res_1', 6700000000)
        node.set_param_value('__requirements__.name_res_2', "gpu_count")
        node.set_param_value('__requirements__.type_res_2', 0)  # float
        node.set_param_value('__requirements__.f_min_res_2', 3.4)
        node.set_param_value('__requirements__.f_pref_res_2', 4.5)
        node.set_param_value('__requirements__.name_res_3', "gpu_mem_b")
        node.set_param_value('__requirements__.type_res_3', 0)  # float
        node.set_param_value('__requirements__.f_min_res_3', 7800000000)
        node.set_param_value('__requirements__.f_pref_res_3', 8900000000)

        node.set_param_value('__requirements__.worker_groups', 'sasha masha dasha')
        node.set_param_value('__requirements__.worker_type', WorkerType.SCHEDULER_HELPER.value)
        node.set_param_value('__requirements__.priority_adjustment', 9.0)

        res = node._process_task_wrapper({}, {})

        reqs = res.invocation_job.requirements()
        print(reqs.final_where_clause())

        # test individual elements

        self.assertEqual(1.2, reqs.min_resource('cpu_count'))  # .min_cpu_count())
        self.assertEqual(2.3, reqs.preferred_resource('cpu_count'))  # .preferred_cpu_count())
        self.assertEqual(3.4, reqs.min_resource('gpu_count'))  # .min_gpu_count())
        self.assertEqual(4.5, reqs.preferred_resource('gpu_count'))  # .preferred_gpu_count())
        self.assertEqual(5600000000, reqs.min_resource('cpu_mem_b'))  # .min_memory_bytes())
        self.assertEqual(6700000000, reqs.preferred_resource('cpu_mem_b'))  # .preferred_memory_bytes())
        self.assertEqual(7800000000, reqs.min_resource('gpu_mem_b'))  # .min_gpu_memory_bytes())
        self.assertEqual(8900000000, reqs.preferred_resource('gpu_mem_b'))  # .preferred_gpu_memory_bytes())

        self.assertSetEqual({'sasha', 'masha', 'dasha'}, reqs.groups())
        self.assertEqual(WorkerType.SCHEDULER_HELPER, reqs.worker_type())
        self.assertEqual(9.0, res.invocation_job.priority())

        # sanity test clause

        sql_part = reqs.final_where_clause()
        print(sql_part)
        self.assertIn('cpu_count', sql_part)
        self.assertIn('cpu_mem_b', sql_part)
        self.assertIn('gpu_count', sql_part)
        self.assertIn('gpu_mem_b', sql_part)

        rng = random.Random(1366613)
        _stat_pass = 0
        _stat_fail = 0

        fd, temp_db_path = tempfile.mkstemp(suffix='_test.db', dir='/dev/shm' if os.path.exists('/dev/shm') else None)
        try:
            for i in range(100):
                with open(temp_db_path, 'w') as f:
                    pass
                data_access = DataAccess(config_provider=SchedulerConfigProviderOverrides(
                    temp_db_path, 30,
                    resource_definitions=(  # this matches what we defined in params above, would be nice to combine these
                        WorkerResourceDefinition('cpu_count', WorkerResourceDataType.GENERIC_FLOAT, '', ''),
                        WorkerResourceDefinition('cpu_mem_b', WorkerResourceDataType.GENERIC_FLOAT, '', ''),
                        WorkerResourceDefinition('gpu_count', WorkerResourceDataType.GENERIC_FLOAT, '', ''),
                        WorkerResourceDefinition('gpu_mem_b', WorkerResourceDataType.GENERIC_FLOAT, '', ''),
                    ),
                ))
                async with data_access.data_connection() as con:
                    async with con.execute('SELECT "id" FROM "workers"') as cur:
                        self.assertEqual(0, len(await cur.fetchall()))

                    cpu_c = rng.uniform(1, 4)
                    gpu_c = rng.uniform(3, 6)
                    cpu_m = rng.randint(5000000000, 8000000000)
                    gpu_m = rng.randint(7000000000, 10000000000)
                    wt = rng.choice(list(WorkerType))

                    await con.execute('INSERT INTO "workers" '
                                      '(hwid, '
                                      'last_address, last_seen, ping_state, state, worker_type) '
                                      'VALUES '
                                      '(?, ?, ?, ?, ?, ?)',
                                      (12345, '', 0, 0, 0, wt.value))
                    await con.execute('INSERT INTO resources '
                                      '(hwid, cpu_count, total_cpu_count, '
                                      'cpu_mem_b, total_cpu_mem_b, '
                                      'gpu_count, total_gpu_count, '
                                      'gpu_mem_b, total_gpu_mem_b) '
                                      'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) '
                                      'ON CONFLICT(hwid) DO UPDATE SET '
                                      'cpu_count=excluded.cpu_count, total_cpu_count=excluded.total_cpu_count, '
                                      'cpu_mem_b=excluded.cpu_mem_b, total_cpu_mem_b=excluded.total_cpu_mem_b, '
                                      'gpu_count=excluded.gpu_count, total_gpu_count=excluded.total_gpu_count, '
                                      'gpu_mem_b=excluded.gpu_mem_b, total_gpu_mem_b=excluded.total_gpu_mem_b',
                                      (12345,
                                       cpu_c,
                                       cpu_c,
                                       cpu_m,
                                       cpu_m,
                                       gpu_c,
                                       gpu_c,
                                       gpu_m,
                                       gpu_m)
                                      )

                    grps = rng.sample(('sasha', 'masha', 'dasha', 'pasha', 'lesha'), rng.randint(0, 3))
                    if grps:
                        await con.executemany(f'INSERT INTO worker_groups (worker_hwid, "group") VALUES (?, ?)',
                                              ((12345, x) for x in grps))
                    await con.commit()

                    async with con.execute(f'SELECT "id", "cpu_count", "cpu_mem_b", "gpu_count_b", "gpu_mem" FROM workers '
                                           f'LEFT JOIN resources ON workers.hwid == resources.hwid '
                                           f'WHERE {sql_part}') as cur:
                        res = await cur.fetchall()
                    if all((1.2 <= cpu_c,
                            5600000000 <= cpu_m,
                            3.4 <= gpu_c,
                            7800000000 <= gpu_m,
                            WorkerType.SCHEDULER_HELPER == wt,
                            'sasha' in grps or 'masha' in grps or 'dasha' in grps)):
                        self.assertEqual(1, len(res), f'{res} {(cpu_c, cpu_m, gpu_c, gpu_m)}')
                        self.assertEqual(1, res[0][0], f'{res} {(cpu_c, cpu_m, gpu_c, gpu_m)}')
                        _stat_pass += 1
                    else:
                        self.assertEqual(0, len(res), f'{res} {(cpu_c, cpu_m, gpu_c, gpu_m)}')
                        _stat_fail += 1
        finally:
            os.close(fd)
            os.unlink(temp_db_path)
        print(f'wedging tests:\n\taccept: {_stat_pass}\n\tdecline: {_stat_fail}')

    def test_resource_definition_simple(self):
        fd, temp_db_path = tempfile.mkstemp(suffix='_test.db', dir='/dev/shm' if os.path.exists('/dev/shm') else None)
        try:
            config = SchedulerConfigProviderOverrides(
                main_db_location=temp_db_path,
                resource_definitions=(
                    WorkerResourceDefinition('fooo', WorkerResourceDataType.GENERIC_FLOAT, '', '', 12.3),
                    WorkerResourceDefinition('boar', WorkerResourceDataType.GENERIC_INT, '', '', 234),
                )
            )

            data_access = DataAccess(config_provider=config)

            # Not the best test, as it tests with implementation-specific defaults
            # but before data_access is properly separated from scheduler - there's no proper unit testing it separately
            with sqlite3.connect(temp_db_path) as con:
                con.row_factory = sqlite3.Row
                cur = con.execute('PRAGMA table_info(resources)')
                resource_rows = {x['name']: x for x in cur.fetchall() if x['name'] != 'hwid'}
                cur.close()
            self.assertSetEqual({'fooo', 'total_fooo', 'boar', 'total_boar'}, set(resource_rows.keys()))
            self.assertEqual(12.3, float(resource_rows['fooo']['dflt_value']))
            self.assertEqual(12.3, float(resource_rows['total_fooo']['dflt_value']))
            self.assertEqual(234, int(resource_rows['boar']['dflt_value']))
            self.assertEqual(234, int(resource_rows['total_boar']['dflt_value']))
        finally:
            os.close(fd)
            os.unlink(temp_db_path)
