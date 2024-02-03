import random
from asyncio import Event
from lifeblood.scheduler import Scheduler
from lifeblood.worker import Worker
from lifeblood.basenode import ProcessingError
from lifeblood_testing_common.nodes_common import TestCaseBase, PseudoContext

from typing import List


class TestWedge(TestCaseBase):
    async def test_basic_functions_wait(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            rng = random.Random(9377151)
            for _ in range(200):
                task = context.create_pseudo_task_with_attrs({'foo': rng.randint(-999, 999), 'bar': rng.uniform(-999, 999), 'googa': [rng.randint(-999, 999) for _ in range(rng.randint(1, 256))]})

                rmode = 0 if rng.random() > 0.5 else 1
                rstart = rng.randint(-9999, 9999) if rng.random() > 0.5 else rng.uniform(-9999, 9999)
                rend = rng.randint(-9999, 9999) if rng.random() > 0.5 else rng.uniform(-9999, 9999)
                rcount = rng.randint(-999, 9999)
                if rmode == 1 and rcount < 0:
                    rcount *= -1
                rmax = rend
                rinc = (rmax - rstart) / (rcount - 1) if rcount > 1 else 2*(rmax - rstart)
                if int(rinc) == rinc:
                    rinc = int(rinc)

                node = context.create_node('wedge', 'footest')
                node.set_param_value('wedge count', 1)
                node.set_param_value('wtype_0', rmode)  # by count or by inc
                node.set_param_value('attr_0', 'wooga')
                node.set_param_value('from_0', rstart)
                node.set_param_value('to_0', rend)
                node.set_param_value('count_0', rcount)
                node.set_param_value('max_0', rmax)
                node.set_param_value('inc_0', rinc)

                if rcount < 0 and rmode == 0:  # forbid negative count in count mode
                    self.assertRaises(ProcessingError, context.process_task, node, task)
                    continue
                res = context.process_task(node, task)

                self.assertIsNotNone(res)
                if rmode == 1 and (isinstance(rinc, float) or isinstance(rstart, float) or isinstance(rend, float)):  # in inc case and float start/end we cannot guarantee
                    self.assertTrue(rcount == len(res._split_attribs) or rcount == len(res._split_attribs)+1, msg=f'{rcount} not eq to {len(res._split_attribs)}')
                for i, attrs in enumerate(res._split_attribs):
                    if rcount == 1:
                        t = 1.0
                    else:
                        t = i / (rcount - 1)
                    self.assertAlmostEqual(rstart*(1-t) + rend*(t), attrs['wooga'], msg=f'failed with {rstart}..{rend}, i={i}, cnt={rcount}')

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def test_wedgings(self):
        exp_pairs = [
            (
                (('att1', 0, 1, 10, 10, 0, 0),),
                (10, [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,)])
            ),
            (
                (('att1', 1, 1, 0, 0, 10, 1),),
                (10, [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,)])
            ),

            (
                (('att1', 0, 1.5, 10.5, 10, 0, 0),),
                (10, [(1.5,), (2.5,), (3.5,), (4.5,), (5.5,), (6.5,), (7.5,), (8.5,), (9.5,), (10.5,)])
            ),
            (
                (('att1', 1, 1.5, 0, 0, 10.5, 1),),
                (10, [(1.5,), (2.5,), (3.5,), (4.5,), (5.5,), (6.5,), (7.5,), (8.5,), (9.5,), (10.5,)])
            ),

            (
                (('att1', 0, 1/3, 1/3 + 9, 10, 0, 0),),
                (10, [(1/3,), (1/3+1,), (1/3+2,), (1/3+3,), (1/3+4,), (1/3+5,), (1/3+6,), (1/3+7,), (1/3+8,), (1/3+9,)])
            ),
            (
                (('att1', 1, 1/3, 0, 0, 1/3 + 9, 1),),
                (10, [(1/3,), (1/3+1,), (1/3+2,), (1/3+3,), (1/3+4,), (1/3+5,), (1/3+6,), (1/3+7,), (1/3+8,), (1/3+9,)])
            ),

            (
                (('att1', 0, 1.567, 2.345, 6, 0, 0),),
                (6, [(1.567,), (1.7226,), (1.8782,), (2.0338,), (2.1894,), (2.345,)])
            ),
            (
                (('att1', 1, 1.567, 0, 0, 2.345, 0.1556),),
                (6, [(1.567,), (1.7226,), (1.8782,), (2.0338,), (2.1894,), (2.345,)])
            ),

            (
                (('att1', 0, 1, 2.5, 4, 0, 0), ('att2', 0, 11, 15, 3, 0, 0)),
                (12, [(1.0, 11), (1.0, 13), (1.0, 15), (1.5, 11), (1.5, 13), (1.5, 15), (2.0, 11), (2.0, 13), (2.0, 15), (2.5, 11), (2.5, 13), (2.5, 15)])
            ),
            (
                (('att1', 1, 1, 0, 0, 2.5, 0.5), ('att2', 1, 11, 0, 0, 15, 2)),
                (12, [(1.0, 11), (1.0, 13), (1.0, 15), (1.5, 11), (1.5, 13), (1.5, 15), (2.0, 11), (2.0, 13), (2.0, 15), (2.5, 11), (2.5, 13), (2.5, 15)])
            ),
        ]

        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):

            task = context.create_pseudo_task_with_attrs({})

            for exp_inputs, exp_results in exp_pairs:

                node = context.create_node('wedge', 'footest')
                node.set_param_value('wedge count', len(exp_inputs))
                attr_name_order = []
                for i, (attr_name, rmode, rfrom, rto, rcount, rmax, rinc) in enumerate(exp_inputs):
                    print(i, attr_name, rmode, rfrom, rto, rcount, rmax, rinc)
                    attr_name_order.append(attr_name)
                    node.set_param_value(f'wtype_{i}', rmode)  # by count or by inc
                    node.set_param_value(f'attr_{i}', attr_name)
                    node.set_param_value(f'from_{i}', rfrom)
                    node.set_param_value(f'to_{i}', rto)
                    node.set_param_value(f'count_{i}', rcount)
                    node.set_param_value(f'max_{i}', rmax)
                    node.set_param_value(f'inc_{i}', rinc)

                res = context.process_task(node, task)

                self.assertIsNotNone(res)
                attr_tuples = [tuple(attrs[n] for n in attr_name_order) for attrs in res._split_attribs]
                self.assertEqual(exp_results[0], len(attr_tuples))

                if exp_results[1] is not None:
                    # now some elaborate scheming to compare floats with AlmostEqual
                    if len(exp_results[1]) and any(isinstance(x, float) for x in exp_results[1][0]):
                        for ex, ac in zip(exp_results[1], attr_tuples):
                            for xex, xac in zip(ex, ac):
                                if isinstance(xex, float):
                                    self.assertAlmostEqual(xex, xac, msg=f'float lists differ at: {ex} vs {ac}. from {exp_results} vs {attr_tuples}')
                                else:
                                    self.assertEqual(xex, xac, msg=f'float lists differ at: {ex} vs {ac}. from {exp_results} vs {attr_tuples}')
                    else:
                        self.assertListEqual(exp_results[1], attr_tuples)

        await self._helper_test_node_with_arg_update(
            _logic
        )
