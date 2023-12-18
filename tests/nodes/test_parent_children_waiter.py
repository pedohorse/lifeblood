from asyncio import Event
import random
from lifeblood.scheduler import Scheduler
from lifeblood.worker import Worker
from lifeblood.basenode import BaseNode
from lifeblood.enums import TaskState
from lifeblood.exceptions import NodeNotReadyToProcess
from .common import TestCaseBase, PseudoContext

from typing import List


class TestParentChildrenWaiter(TestCaseBase):
    """
    The idea of the work logic of the node is that all required tasks need to reach the node first
    If last task that reached node was NOT the parent - parent HAS to be processed once more
    as parent is processed on last step - it's let though straight away
    then all children are let through when processed.

    Important part is that parent has to be processed before children can be released.
    Is it implementation detail? or a core feature?
    Currently - it's a core feature, as only Parent knows when ALL children have passed,
    as active children count is dynamic, some children may die before reaching this node

    This node ONLY determined behaviour currently is the normal graph processing.
    Tests below DON NOT cover cases of human intervention: tasks being force-moved between nodes, killed, resubmitted, etc.
    """
    async def test_standard_function(self):
        rng = random.Random(7269321)
        await self._helper_test_regular(0, 0)
        await self._helper_test_regular(0, 100)

    async def test_when_unreached_child_becomes_dead(self):
        rng = random.Random(7269321)
        await self._helper_test_regular(rng.randint(1, 10), 0)
        await self._helper_test_regular(rng.randint(1, 10), 100)

    async def test_recursive(self):
        rng = random.Random(7269321)
        await self._helper_test_recursive(0, 0)
        await self._helper_test_recursive(0, 100)

    async def test_recursive_when_unreached_child_becomes_dead(self):
        rng = random.Random(7269321)
        await self._helper_test_recursive(rng.randint(1, 10), 0)
        await self._helper_test_recursive(rng.randint(1, 10), 100)

    async def test_serialization(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            rng = random.Random(1666661)
            task_parent = context.create_pseudo_task_with_attrs({}, 333, None)
            task_child0 = context.create_pseudo_task_with_attrs({'foo': 2}, 456, 333)
            task_child1 = context.create_pseudo_task_with_attrs({'foo': 5}, 123, 333)
            task_child2 = context.create_pseudo_task_with_attrs({'foo': 9}, 666, 333)

            task_child0.set_input_name('children')
            task_child1.set_input_name('children')
            task_child2.set_input_name('children')

            node: BaseNode = context.create_node('parent_children_waiter', 'footest')

            def _prune_dicts(dic):
                return {
                    k: v for k, v in dic.items() if k not in (
                        '_ParentChildrenWaiterNode__main_lock',  # lock is not part of the state, locks should be unique
                        '_parameters',  # parameters to be compared separately, by a different, generic test set, they are not part of state anyway
                        '_BaseNode__parent_nid'  # node ids will be different, state does not cover it
                    )
                }

            def _do_test():
                state = node.get_state()
                node_test: BaseNode = context.create_node('parent_children_waiter', 'footest')
                node_test.set_state(state)

                self.assertDictEqual(_prune_dicts(node.__dict__), _prune_dicts(node_test.__dict__))

            all_tasks = (task_child0, task_child1, task_child2, task_parent)
            for _ in range(100):
                tasks = [task_child0, task_child1, task_child2]
                rng.shuffle(tasks)
                for _ in range(rng.randint(0, 5)):
                    tasks.append(rng.choice(all_tasks))

                _do_test()
                for task in tasks:
                    context.process_task(node, task)
                    _do_test()
                context.process_task(node, task_parent)
                _do_test()

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def _helper_test_regular(self, dying_children_count, random_tasks_count):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            rng = random.Random(16666661)
            task_parent = context.create_pseudo_task_with_attrs({}, 333, None)
            task_child0 = context.create_pseudo_task_with_attrs({'foo': 2}, 456, 333)
            task_child1 = context.create_pseudo_task_with_attrs({'foo': 5}, 123, 333)
            task_child2 = context.create_pseudo_task_with_attrs({'foo': 9}, 666, 333)

            task_child0.set_input_name('children')
            task_child1.set_input_name('children')
            task_child2.set_input_name('children')

            tasks_to_die = []
            for i in range(dying_children_count):
                tasks_to_die.append(context.create_pseudo_task_with_attrs({'foo': rng.randint(0, 999)}, 888 + i, 333))
            random_tasks = [context.create_pseudo_task_with_attrs(
                {'foo': rng.random()},
                rng.randint(2000, 3000),
                rng.choice([123, 456, 666]) if rng.random() < 0.1 else rng.randint(2000, 2100)
            ) for _ in range(random_tasks_count)]

            def _process_random_tasks(node):
                for _ in range(rng.randint(0, random_tasks_count // 2)):
                    context.process_task(node, rng.choice(random_tasks))
            tasks = [task_parent, task_child0, task_child1, task_child2]

            for _ in range(100):
                node: BaseNode = context.create_node('parent_children_waiter', 'footest')
                node.set_param_value('transfer_attribs', 1)
                node.set_param_value('src_attr_name_0', 'foo')
                node.set_param_value('transfer_type_0', 'extend')
                node.set_param_value('dst_attr_name_0', 'bar')
                node.set_param_value('sort_by_0', '_builtin_id')

                _process_random_tasks(node)
                rng.shuffle(tasks)
                otasks = tasks[:-1]
                for task in otasks:
                    if rng.random() > 0.5 and len(tasks_to_die) > 0:
                        tasks_to_die.pop().set_state(TaskState.DEAD)
                    self.assertIsNone(context.process_task(node, task))
                    _process_random_tasks(node)

                while len(tasks_to_die):
                    for task in reversed(tasks):  # reverse just to start with the last that was not checked in prev loop
                        self.assertIsNone(context.process_task(node, task))
                    tasks_to_die.pop().set_state(TaskState.DEAD)
                    _process_random_tasks(node)

                if tasks[-1] is task_parent:
                    self.assertIsNotNone(context.process_task(node, tasks[-1]))
                else:
                    self.assertIsNone(context.process_task(node, tasks[-1]))
                    self.assertIsNotNone(context.process_task(node, task_parent))
                    otasks.remove(task_parent)
                    otasks.append(tasks[-1])

                _process_random_tasks(node)
                rng.shuffle(otasks)
                for task in otasks:
                    self.assertIsNotNone(context.process_task(node, task))
                    _process_random_tasks(node)

                # check attribs
                self.assertListEqual([5, 2, 9], task_parent.attributes()['bar'])

                # check cleanup
                random_tasks_parent_ids = set(x.parent_id() for x in random_tasks)
                for task in tasks:
                    # if some of generated "garbage" tasks references some of tested tasks as parent - their info MAY be internal data (or may not, depending on processing order)
                    if task.id() not in random_tasks_parent_ids:
                        self.assertFalse(node._debug_has_internal_data_for_task(task.id()))

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def _helper_test_recursive(self, dying_children_count, random_tasks_count):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            rng = random.Random(6667162)
            task_parent = context.create_pseudo_task_with_attrs({}, 333, None)
            task_child0 = context.create_pseudo_task_with_attrs({'foo': 2}, 456, 333)
            task_child1 = context.create_pseudo_task_with_attrs({'foo': 5}, 123, 333)
            task_child2 = context.create_pseudo_task_with_attrs({'foo': 9}, 666, 333)

            task_child1_child0 = context.create_pseudo_task_with_attrs({'foo': 15}, 234, 123)
            task_child1_child1 = context.create_pseudo_task_with_attrs({'foo': 25}, 12, 123)
            task_child1_child2 = context.create_pseudo_task_with_attrs({'foo': 35}, 777, 123)

            task_child0.set_input_name('children')
            task_child1.set_input_name('children')
            task_child2.set_input_name('children')
            task_child1_child0.set_input_name('children')
            task_child1_child1.set_input_name('children')
            task_child1_child2.set_input_name('children')

            tasks_to_die = []
            for i in range(dying_children_count):
                tasks_to_die.append(context.create_pseudo_task_with_attrs({'foo': rng.randint(0, 999)}, 888 + i, 333))
            random_tasks = [context.create_pseudo_task_with_attrs(
                {'foo': rng.random()},
                rng.randint(2000, 3000),
                rng.randint(2000, 2100)) for _ in range(random_tasks_count)]
            def _process_random_tasks(node):
                for _ in range(rng.randint(0, random_tasks_count // 2)):
                    context.process_task(node, rng.choice(random_tasks))

            task1_kids = [task_child1_child0, task_child1_child1, task_child1_child2]
            tasks = [task_parent, task_child0, task_child1, task_child2] + task1_kids

            for _ in range(100):
                node: BaseNode = context.create_node('parent_children_waiter', 'footest')
                node.set_param_value('transfer_attribs', 1)
                node.set_param_value('src_attr_name_0', 'foo')
                node.set_param_value('transfer_type_0', 'extend')
                node.set_param_value('dst_attr_name_0', 'bar')
                node.set_param_value('sort_by_0', '_builtin_id')
                node.set_param_value('recursive', True)

                _process_random_tasks(node)
                rng.shuffle(tasks)
                otasks = tasks[:-1]
                for task in otasks:
                    if rng.random() > 0.5 and len(tasks_to_die) > 0:
                        tasks_to_die.pop().set_state(TaskState.DEAD)
                    self.assertIsNone(context.process_task(node, task))
                    _process_random_tasks(node)

                while len(tasks_to_die):
                    for task in reversed(tasks):  # reverse just to start with the last that was not checked in prev loop
                        self.assertIsNone(context.process_task(node, task))
                    tasks_to_die.pop().set_state(TaskState.DEAD)
                    _process_random_tasks(node)

                if tasks[-1] is task_parent:
                    if tasks[-2] is not task_child1:
                        # parent of grandkids must be attempted to be processed before higher parents
                        self.assertIsNone(context.process_task(node, task_child1))
                    self.assertIsNotNone(context.process_task(node, tasks[-1]))
                else:
                    self.assertIsNone(context.process_task(node, tasks[-1]))
                    self.assertIsNone(context.process_task(node, task_child1))
                    self.assertIsNotNone(context.process_task(node, task_parent))
                    otasks.remove(task_parent)
                    otasks.append(tasks[-1])

                print('checking rest')
                rng.shuffle(otasks)
                child1_passed = False
                for task in otasks:
                    _process_random_tasks(node)
                    # fist parent will have to pass before grandchildren
                    if not child1_passed and task in task1_kids:
                        self.assertIsNone(context.process_task(node, task))
                        otasks.append(task)
                        continue
                    if task is task_child1:
                        child1_passed = True
                    self.assertIsNotNone(context.process_task(node, task))
                _process_random_tasks(node)

                # check attribs
                self.assertListEqual([25, 5, 15, 2, 9, 35], task_parent.attributes()['bar'])

                # check cleanup
                random_tasks_parent_ids = set(x.parent_id() for x in random_tasks)
                for task in tasks:
                    # if some of generated "garbage" tasks references some of tested tasks as parent - their info MAY be internal data (or may not, depending on processing order)
                    if task.id() not in random_tasks_parent_ids:
                        self.assertFalse(node._debug_has_internal_data_for_task(task.id()))

        await self._helper_test_node_with_arg_update(
            _logic
        )
