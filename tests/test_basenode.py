import random
from unittest import TestCase, mock
import sqlite3
from lifeblood.basenode import BaseNodeWithTaskRequirements, BaseNode, ProcessingResult
from lifeblood.processingcontext import ProcessingContext
from lifeblood.invocationjob import InvocationJob
from lifeblood.enums import WorkerType
from lifeblood.db_misc import sql_init_script


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


class TestBaseNodes(TestCase):
    def test_requirements(self):
        node = NoNodeWithReq('foo')
        node.set_param_value('worker cpu cost', 1.2)
        node.set_param_value('worker cpu cost preferred', 2.3)
        node.set_param_value('worker gpu cost', 3.4)
        node.set_param_value('worker gpu cost preferred', 4.5)
        node.set_param_value('worker mem cost', 5.6)
        node.set_param_value('worker mem cost preferred', 6.7)
        node.set_param_value('worker gpu mem cost', 7.8)
        node.set_param_value('worker gpu mem cost preferred', 8.9)

        node.set_param_value('worker groups', 'sasha masha dasha')
        node.set_param_value('worker type', WorkerType.SCHEDULER_HELPER.value)
        node.set_param_value('priority adjustment', 9.0)

        res = node._process_task_wrapper({})

        reqs = res.invocation_job.requirements()

        # test individual elements

        self.assertEqual(1.2, reqs.min_cpu_count())
        self.assertEqual(2.3, reqs.preferred_cpu_count())
        self.assertEqual(3.4, reqs.min_gpu_count())
        self.assertEqual(4.5, reqs.preferred_gpu_count())
        self.assertEqual(5600000000, reqs.min_memory_bytes())
        self.assertEqual(6700000000, reqs.preferred_memory_bytes())
        self.assertEqual(7800000000, reqs.min_gpu_memory_bytes())
        self.assertEqual(8900000000, reqs.preferred_gpu_memory_bytes())

        self.assertSetEqual({'sasha', 'masha', 'dasha'}, reqs.groups())
        self.assertEqual(WorkerType.SCHEDULER_HELPER, reqs.worker_type())
        self.assertEqual(9.0, res.invocation_job.priority())

        # sanity test clause

        sql_part = reqs.final_where_clause()
        print(sql_part)
        self.assertIn('cpu_count', sql_part)
        self.assertIn('cpu_mem', sql_part)
        self.assertIn('gpu_count', sql_part)
        self.assertIn('gpu_mem', sql_part)

        rng = random.Random(1366613)
        _stat_pass = 0
        _stat_fail = 0
        for i in range(1000):
            with sqlite3.connect(':memory:') as con:
                con.executescript(sql_init_script)
                con.commit()
                cur = con.cursor()
                cur.execute('SELECT "id" FROM "workers"')
                self.assertEqual(0, len(cur.fetchall()))

                cpu_c = rng.uniform(1, 4)
                gpu_c = rng.uniform(3, 6)
                cpu_m = rng.randint(5000000000, 8000000000)
                gpu_m = rng.randint(7000000000, 10000000000)
                wt = rng.choice(list(WorkerType))

                con.execute('INSERT INTO "workers" '
                            '(hwid, '
                            'last_address, last_seen, ping_state, state, worker_type) '
                            'VALUES '
                            '(?, ?, ?, ?, ?, ?)',
                            (12345, '', 0, 0, 0, wt.value))
                con.execute('INSERT INTO resources '
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
                    con.executemany(f'INSERT INTO worker_groups (worker_hwid, "group") VALUES (?, ?)',
                                    ((12345, x) for x in grps))
                con.commit()

                cur.execute(f'SELECT "id", "cpu_count", "cpu_mem", "gpu_count", "gpu_mem" FROM workers '
                            f'LEFT JOIN resources ON workers.hwid == resources.hwid '
                            f'WHERE {sql_part}')
                res = cur.fetchall()
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

        print(f'wedging tests:\n\taccept: {_stat_pass}\n\tdecline: {_stat_fail}')
