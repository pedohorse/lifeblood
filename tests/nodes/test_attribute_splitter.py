import random
from asyncio import Event
from lifeblood.scheduler import Scheduler
from lifeblood.worker import Worker
from lifeblood.basenode import BaseNode, ProcessingError
from lifeblood.exceptions import NodeNotReadyToProcess
from .common import TestCaseBase, PseudoContext

from typing import List


class TestWedge(TestCaseBase):
    async def test_basic_functions_list(self):
        exp_pairs_list = [
            (
                ('foofattr', 4, []),
                ([],)
            ),
            (
                ('foofattr', 4, [5]),
                ([5],)
            ),
            (
                ('foofattr', 4, [1, 3, 5, 7]),
                ([1, 3, 5, 7],)
            ),
            (
                ('foofattr', 4, [1, 3, 5, 7, 9, 2, 4, 6, 8, 0]),
                ([1, 3, 5, 7], [9, 2, 4, 6], [8, 0])
            ),
            (
                ('foofattr', 4, [1, 3, 5, 7, 9, 2, 4, 6, 8]),
                ([1, 3, 5, 7], [9, 2, 4, 6], [8])
            ),
            (
                ('foofattr', 4, [1, 3, 5, 7, 9, 2, 4,]),
                ([1, 3, 5, 7], [9, 2, 4])
            ),
            (
                ('foofattr', 0, [1, 3, 5, 7, 9, 2, 4, ]),
                None
            ),
            (
                ('foofattr', -1, [1, 3, 5, 7, 9, 2, 4, ]),
                None
            ),
        ]

        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            # rng = random.Random(9377151)

            for exp_inputs, exp_results in exp_pairs_list:
                attr_name, chunk_size, attr_val = exp_inputs

                task = context.create_pseudo_task_with_attrs({attr_name: attr_val})
                node = context.create_node('attribute_splitter', 'footest')
                node.set_param_value('split type', 'list')
                node.set_param_value('attribute name', attr_name)
                node.set_param_value('chunk size', chunk_size)

                if exp_results is None:  # expect error
                    self.assertRaises(ProcessingError, context.process_task, node, task)
                    continue

                res = context.process_task(node, task)
                self.assertIsNotNone(res)

                self.assertEqual(len(exp_results), len(res._split_attribs), f'len mismatch! expected {exp_results}, actual {res._split_attribs}')
                for exp, attr in zip(exp_results, res._split_attribs):
                    self.assertIn(attr_name, attr)
                    self.assertEqual(exp, attr[attr_name])

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def test_basic_functions_range(self):
        exp_pairs_list = [
            (
                #                          chsz chcnt  st end inc type spl-by  mode
                ('astart', 'aend', 'asize',   4,    0,  1, 10, 1,   0,     0,   0),
                ((1, 4, None), (5, 8, None), (9, 10, None))
            ),
            (
                #                          chsz chcnt  st end inc type spl-by  mode
                ('astart', 'aend', 'asize',   3,    0, 10, 12, 1,   0,     0,   2),
                ((10, 12, 3), )
            ),
            (
                #                          chsz chcnt  st end inc type spl-by  mode
                ('astart', 'aend', 'asize',   3,    0, 10, 13, 1,   0,     0,   2),
                ((10, 12, 3), (13, 13, 1))
            ),
            (
                #                          chsz chcnt  st end inc type spl-by  mode
                ('astart', 'aend', 'asize',   3,    0, 10, 12, 10,   0,     0,   2),
                ((10, 12, 1), )
            ),
            (
                #                          chsz chcnt  st end inc type spl-by  mode
                ('astart', 'aend', 'asize',   3,    0, 10, 15, 1,   0,     0,   2),
                ((10, 12, 3), (13, 15, 3))
            ),
            (
                #                          chsz chcnt  st end inc type spl-by  mode
                ('astart', 'aend', 'asize',   3,    0, 10, 15, 10,   0,     0,   2),
                ((10, 15, 1),)
            ),
            (
                #                          chsz chcnt  st end inc type spl-by  mode
                ('astart', 'aend', 'asize',   3,    0, 10, 16, 10,   0,     0,   2),
                ((10, 16, 1), )
            ),
            (
                #                          chsz chcnt  st end inc type spl-by  mode
                ('astart', 'aend', 'asize',   3,    0, 15, 38, 3,   0,     0,   2),
                ((15, 21, 3), (24, 30, 3), (33, 38, 2))
            ),
            #
            # count
            (
                #                          chsz chcnt  st end inc type spl-by  mode
                ('astart', 'aend', 'asize',   0,   10, 10, 12, 3,   0,     1,   0),
                ((10, 12, None),)
            ),
            (
                #                          chsz chcnt  st end inc type spl-by  mode
                ('astart', 'aend', 'asize',   0,   10, 10, 15, 3,   0,     1,   0),
                ((10, 10, None), (13, 15, None))
            ),
            (
                #                          chsz chcnt  st end inc type spl-by  mode
                ('astart', 'aend', 'asize',   0,   10, 15, 16, 3,   0,     1,   1),
                ((15, None, 1),)
            ),
            (
                #                          chsz chcnt  st end inc type spl-by  mode
                ('astart', 'aend', 'asize',   0,   3, 15, 29, 3,   0,     1,   2),
                ((15, 18, 2), (21, 24, 2), (27, 29, 1))
            ),
            (
                #                          chsz chcnt  st end inc type spl-by  mode
                ('astart', 'aend', 'asize',   0,   4, 15, 29, 3,   0,     1,   2),
                ((15, 18, 2), (21, 21, 1), (24, 24, 1), (27, 29, 1))
            ),
        ]

        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            # rng = random.Random(9377151)

            for exp_inputs, exp_results in exp_pairs_list:
                start_name, end_name, size_name, chunk_size, chunk_count, start, end, inc, rtype, split_by, out_mode = exp_inputs
                print(f'{start_name},{end_name},{size_name}: chunksize={chunk_size}, chunk_count={chunk_count}, {start}/{end}/{inc}, rtype={rtype}, split_by={split_by}, out_mode={out_mode}')

                task = context.create_pseudo_task_with_attrs({})
                node = context.create_node('attribute_splitter', 'footest')
                node.set_param_value('split type', 'range')
                node.set_param_value('out start name', start_name)
                node.set_param_value('out end name', end_name)
                node.set_param_value('out size name', size_name)
                node.set_param_value('range chunk', chunk_size)
                node.set_param_value('range count', chunk_count)
                node.set_param_value('range start', start)
                node.set_param_value('range end', end)
                node.set_param_value('range inc', inc)
                node.set_param_value('out range type', rtype)
                node.set_param_value('range split by', split_by)
                node.set_param_value('range mode', out_mode)

                if exp_results is None:  # expect error
                    self.assertRaises(ProcessingError, context.process_task, node, task)
                    continue

                res = context.process_task(node, task)
                self.assertIsNotNone(res)

                self.assertEqual(len(exp_results), len(res._split_attribs), f'len mismatch! expected {exp_results}, actual {res._split_attribs}')
                print(res._split_attribs)
                for (e_start, e_end, e_size), attr in zip(exp_results, res._split_attribs):
                    self.assertEqual(e_start, attr[start_name])
                    if e_end is not None:
                        self.assertEqual(e_end, attr[end_name])
                    else:
                        self.assertNotIn(end_name, attr)
                    if e_size is not None:
                        self.assertEqual(e_size, attr[size_name])
                    else:
                        self.assertNotIn(size_name, attr)

        await self._helper_test_node_with_arg_update(
            _logic
        )
