import random
import json
from asyncio import Event
from lifeblood.scheduler import Scheduler
from lifeblood.worker import Worker
from lifeblood.basenode import BaseNode
from lifeblood_testing_common.nodes_common import TestCaseBase, PseudoContext

from typing import List


class TestSplitWaiter(TestCaseBase):
    async def test_basic_functions_wait(self):
        await self._helper_test_basic_functions(0)

    async def test_basic_functions_wait_with_garbage(self):
        await self._helper_test_basic_functions(20)

    async def test_basic_functions_nowait(self):
        await self._helper_test_basic_functions(0, False)

    async def test_basic_functions_nowait_with_garbage(self):
        await self._helper_test_basic_functions(20, False)

    async def test_serialization(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            rng = random.Random(1666661)
            main_task = context.create_pseudo_task_with_attrs({'boo': 32})
            tsplits = context.create_pseudo_split_of(main_task, [
                {'foo': 5},
                {'foo': 8},
                {'foo': 1},
                {'foo': 3},
            ])

            node: BaseNode = context.create_node('split_waiter', 'footest')

            def _prune_dicts(dic):
                return {
                    k: v for k, v in dic.items() if k not in (
                        '_SplitAwaiterNode__main_lock',  # lock is not part of the state, locks should be unique
                        '_parameters',  # parameters to be compared separately, by a different, generic test set, they are not part of state anyway
                        '_BaseNode__parent_nid'  # node ids will be different, state does not cover it
                    )
                }

            def _do_test():
                state = node.get_state()
                node_test: BaseNode = context.create_node('split_waiter', 'footest')
                node_test.set_state(state)
                self.assertDictEqual(_prune_dicts(node.__dict__), _prune_dicts(node_test.__dict__))

                # we expect state to be json-serializeable
                state = json.loads(json.dumps(state))
                node_test: BaseNode = context.create_node('split_waiter', 'footest')
                node_test.set_state(state)
                self.assertDictEqual(_prune_dicts(node.__dict__), _prune_dicts(node_test.__dict__))

            all_tasks = tsplits + [main_task]
            for _ in range(100):
                tasks = list(tsplits)
                rng.shuffle(tasks)
                for _ in range(rng.randint(0, 5)):
                    tasks.append(rng.choice(all_tasks))

                _do_test()
                for task in tasks:
                    context.process_task(node, task)
                    _do_test()
                context.process_task(node, main_task)
                _do_test()

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def _helper_test_basic_functions_wait(self, garbage_split_count):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            rng = random.Random(716394)
            main_task = context.create_pseudo_task_with_attrs({'boo': 32})
            tsplits = context.create_pseudo_split_of(main_task, [
                {'foo': 5},
                {'foo': 8},
                {'foo': 1},
                {'foo': 3},
            ])
            garbage_tasks = []
            garbage_splits = []
            for i in range(garbage_split_count):
                gtask = context.create_pseudo_task_with_attrs({'boo': rng.randint(-111, 111)})
                garbage_tasks.append(gtask)
                garbage_splits += context.create_pseudo_split_of(gtask, [{} for _ in range(rng.randint(1, 12))])
            garbage_tasks_all = garbage_tasks + garbage_splits
            rng.shuffle(garbage_tasks_all)
            def _process_garbage_tasks(node):
                for _ in range(rng.randint(0, len(garbage_tasks_all) // 2)):
                    context.process_task(node, rng.choice(garbage_tasks_all))

            for _ in range(100):
                node = context.create_node('split_waiter', 'footest')
                node.set_param_value('wait for all', True)
                node.set_param_value('transfer_attribs', 1)
                node.set_param_value('transfer_type_0', 'extend')
                node.set_param_value('src_attr_name_0', 'foo')
                node.set_param_value('dst_attr_name_0', 'bar')
                node.set_param_value('sort_by_0', '_builtin_id')

                _process_garbage_tasks(node)

                rng.shuffle(tsplits)
                for task in tsplits[:-1]:
                    self.assertIsNone(context.process_task(node, task))
                    _process_garbage_tasks(node)

                last_res = context.process_task(node, tsplits[-1])
                self.assertIsNotNone(last_res)

                _process_garbage_tasks(node)

                ress = []
                for task in tsplits[:-1]:
                    res = context.process_task(node, task)
                    self.assertIsNotNone(res)
                    ress.append(res)
                    _process_garbage_tasks(node)

                ress.append(last_res)

                # check attrs
                self.assertSetEqual({'boo'}, set(main_task.attributes().keys()))
                self.assertEqual(32, main_task.attributes()['boo'])
                self.assertListEqual([5, 8, 1, 3], ress[0].split_attributes_to_set['bar'])
                self.assertTrue(ress[0].do_split_remove)
                for res in ress[1:]:
                    self.assertFalse(res.do_split_remove)
                for res in ress:
                    self.assertTrue(res.do_kill_task)

                # check cleanup
                self.assertFalse(node._debug_has_internal_data_for_split(tsplits[0].task_dict()['split_id']))

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def _helper_test_basic_functions(self, garbage_split_count, do_wait=True):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            rng = random.Random(716394)
            main_task = context.create_pseudo_task_with_attrs({'boo': 32})
            tsplits = context.create_pseudo_split_of(main_task, [
                {'foo': 5},
                {'foo': 8},
                {'foo': 1},
                {'foo': 3},
            ])
            garbage_tasks = []
            garbage_splits = []
            for i in range(garbage_split_count):
                gtask = context.create_pseudo_task_with_attrs({'boo': rng.randint(-111, 111)})
                garbage_tasks.append(gtask)
                garbage_splits += context.create_pseudo_split_of(gtask, [{} for _ in range(rng.randint(1, 12))])
            garbage_tasks_all = garbage_tasks + garbage_splits
            rng.shuffle(garbage_tasks_all)
            def _process_garbage_tasks(node):
                for _ in range(rng.randint(0, len(garbage_tasks_all) // 2)):
                    context.process_task(node, rng.choice(garbage_tasks_all))

            for _ in range(100):
                node = context.create_node('split_waiter', 'footest')
                node.set_param_value('wait for all', do_wait)
                node.set_param_value('transfer_attribs', 1)
                node.set_param_value('transfer_type_0', 'extend')
                node.set_param_value('src_attr_name_0', 'foo')
                node.set_param_value('dst_attr_name_0', 'bar')
                node.set_param_value('sort_by_0', '_builtin_id')

                _process_garbage_tasks(node)

                rng.shuffle(tsplits)
                if do_wait:
                    for task in tsplits[:-1]:
                        self.assertIsNone(context.process_task(node, task))
                        _process_garbage_tasks(node)

                    last_res = context.process_task(node, tsplits[-1])
                    self.assertIsNotNone(last_res)

                    _process_garbage_tasks(node)

                    ress = []
                    for task in tsplits[:-1]:
                        res = context.process_task(node, task)
                        self.assertIsNotNone(res)
                        ress.append(res)
                        _process_garbage_tasks(node)

                    ress.append(last_res)

                    # check attrs
                    self.assertSetEqual({'boo'}, set(main_task.attributes().keys()))
                    self.assertEqual(32, main_task.attributes()['boo'])
                    self.assertListEqual([5, 8, 1, 3], ress[0].split_attributes_to_set['bar'])
                    self.assertTrue(ress[0].do_split_remove)
                    for res in ress[1:]:
                        self.assertFalse(res.do_split_remove)
                    for res in ress:
                        self.assertTrue(res.do_kill_task)

                else:  # no wait for all

                    res = context.process_task(node, tsplits[0])
                    self.assertIsNotNone(res)
                    self.assertTrue(res.do_split_remove)
                    self.assertTrue(res.do_kill_task)
                    self.assertDictEqual({}, res.split_attributes_to_set)  # no attribs are set in non-wait mode
                    for task in tsplits[1:]:
                        res = context.process_task(node, task)
                        self.assertIsNotNone(res)
                        self.assertFalse(res.do_split_remove)
                        self.assertTrue(res.do_kill_task)
                        self.assertDictEqual({}, res.split_attributes_to_set)
                        _process_garbage_tasks(node)

                # check cleanup
                self.assertFalse(node._debug_has_internal_data_for_split(tsplits[0].task_dict()['split_id']))

        await self._helper_test_node_with_arg_update(
            _logic
        )
