from asyncio import Event
from lifeblood.scheduler import Scheduler
from lifeblood.worker import Worker
from lifeblood.basenode import BaseNode
from lifeblood.exceptions import NodeNotReadyToProcess
from .common import TestCaseBase, PseudoContext

from typing import List


class TestWaitForTaskNode(TestCaseBase):
    async def test_basic_functions(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            node: BaseNode = context.create_node('wait_for_task_value', 'footest')
            node.set_param_value('condition value', '`task["cond"]`')
            node.set_param_value('expected values', '`task["exp"]`')

            task0_attrs = {
                'cond': '1',
                'exp': '2 3'
            }
            task1_attrs = {
                'cond': '2',
                'exp': '3'
            }
            task2_attrs = {
                'cond': '3',
                'exp': '1 2'
            }
            task3_attrs = {
                'cond': '4',
                'exp': '1 2 3'
            }
            task0 = context.create_pseudo_task_with_attrs(task0_attrs, 234)
            task1 = context.create_pseudo_task_with_attrs(task1_attrs, 235)
            task2 = context.create_pseudo_task_with_attrs(task2_attrs, 236)
            task3 = context.create_pseudo_task_with_attrs(task3_attrs, 237)

            # task0
            self.assertTrue(node.ready_to_process_task(task0.task_dict()))  # first we have to process to add contribution to the pool
            self.assertRaises(NodeNotReadyToProcess, node.process_task, task0.get_context_for(node))  # but after that process will raise not ready
            self.assertFalse(node.ready_to_process_task(task0.task_dict()))  # after that ready should return false

            # now comes task1
            self.assertTrue(node.ready_to_process_task(task1.task_dict()))
            self.assertRaises(NodeNotReadyToProcess, node.process_task, task1.get_context_for(node))
            self.assertFalse(node.ready_to_process_task(task1.task_dict()))

            # task0 still not ready to process, and processing would result in raise
            self.assertFalse(node.ready_to_process_task(task0.task_dict()))
            self.assertRaises(NodeNotReadyToProcess, node.process_task, task0.get_context_for(node))

            # now task2
            self.assertTrue(node.ready_to_process_task(task2.task_dict()))
            node.process_task(task2.get_context_for(node))  # should finish fine

            # now task0 and task1 should pass
            self.assertTrue(node.ready_to_process_task(task0.task_dict()))
            self.assertTrue(node.ready_to_process_task(task1.task_dict()))
            node.process_task(task0.get_context_for(node))  # should finish fine
            node.process_task(task1.get_context_for(node))  # should finish fine

            # check that expected values are still there
            self.assertTrue(node.ready_to_process_task(task3.task_dict()))
            node.process_task(task3.get_context_for(node))  # should finish fine

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def test_trivial1(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            node: BaseNode = context.create_node('wait_for_task_value', 'footest')
            node.set_param_value('condition value', '`task["cond"]`')
            node.set_param_value('expected values', '`task["exp"]`')

            task0 = context.create_pseudo_task_with_attrs({
                'cond': 'qwe',
                'exp': ''
            }, 234)
            self.assertTrue(node.ready_to_process_task(task0.task_dict()))
            node.process_task(task0.get_context_for(node))  # should finish fine

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def test_trivial1a(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            node: BaseNode = context.create_node('wait_for_task_value', 'footest')
            node.set_param_value('condition value', '`task["cond"]`')
            node.set_param_value('expected values', '`task["exp"]`')

            task0 = context.create_pseudo_task_with_attrs({
                'cond': 'qwe',
                'exp': '    '
            }, 234)
            self.assertTrue(node.ready_to_process_task(task0.task_dict()))
            node.process_task(task0.get_context_for(node))  # should finish fine

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def test_trivial2(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            node: BaseNode = context.create_node('wait_for_task_value', 'footest')
            node.set_param_value('condition value', '`task["cond"]`')
            node.set_param_value('expected values', '`task["exp"]`')

            task0 = context.create_pseudo_task_with_attrs({
                'cond': 'qwe',
                'exp': 'qwe'
            }, 234)
            self.assertTrue(node.ready_to_process_task(task0.task_dict()))
            node.process_task(task0.get_context_for(node))  # should finish fine

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def test_trivial3(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            node: BaseNode = context.create_node('wait_for_task_value', 'footest')
            node.set_param_value('condition value', '`task["cond"]`')
            node.set_param_value('expected values', '`task["exp"]`')

            task0 = context.create_pseudo_task_with_attrs({
                'cond': '    qwe  ',
                'exp': '  qwe   '
            }, 234)
            self.assertTrue(node.ready_to_process_task(task0.task_dict()))
            node.process_task(task0.get_context_for(node))  # should finish fine

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def test_trivial4(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            node: BaseNode = context.create_node('wait_for_task_value', 'footest')
            node.set_param_value('condition value', '`task["cond"]`')
            node.set_param_value('expected values', '`task["exp"]`')

            task0 = context.create_pseudo_task_with_attrs({
                'cond': 'qwe',
                'exp': ''
            }, 234)
            self.assertTrue(node.ready_to_process_task(task0.task_dict()))
            node.process_task(task0.get_context_for(node))  # should finish fine

        await self._helper_test_node_with_arg_update(
            _logic
        )

    # rescheduler behaviour testings

    async def test_reschedule_with_different_cond(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            node: BaseNode = context.create_node('wait_for_task_value', 'footest')
            node.set_param_value('condition value', '`task["cond"]`')
            node.set_param_value('expected values', '`task["exp"]`')

            task0 = context.create_pseudo_task_with_attrs({
                'cond': 'qwe',
                'exp': ''
            }, 234)

            task1 = context.create_pseudo_task_with_attrs({
                'cond': '',
                'exp': 'qwe'
            }, 235)
            task2 = context.create_pseudo_task_with_attrs({
                'cond': '',
                'exp': 'rty'
            }, 236)

            self.assertTrue(node.ready_to_process_task(task0.task_dict()))
            node.process_task(task0.get_context_for(node))  # should finish fine

            # now task1 should be processable, but task2 should not
            self.assertTrue(node.ready_to_process_task(task1.task_dict()))
            self.assertFalse(node.ready_to_process_task(task2.task_dict()))
            node.process_task(task1.get_context_for(node))  # should finish fine
            self.assertRaises(NodeNotReadyToProcess, node.process_task, task2.get_context_for(node))

            # imitate reschedule of the task0
            task0.update_attribs({'cond': 'rty'})
            self.assertTrue(node.ready_to_process_task(task0.task_dict()))
            node.process_task(task0.get_context_for(node))  # should finish fine

            # now task2 should be processable, but task1 should not
            self.assertTrue(node.ready_to_process_task(task2.task_dict()))
            self.assertFalse(node.ready_to_process_task(task1.task_dict()))
            node.process_task(task2.get_context_for(node))  # should finish fine
            self.assertRaises(NodeNotReadyToProcess, node.process_task, task1.get_context_for(node))

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def test_reschedule_with_from_empty_cond1(self):
        await self._helper_test_reschedule_with_from_empty_cond(0)

    async def test_reschedule_with_from_empty_cond2(self):
        await self._helper_test_reschedule_with_from_empty_cond(1)

    async def _helper_test_reschedule_with_from_empty_cond(self, var: int):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            node: BaseNode = context.create_node('wait_for_task_value', 'footest')
            node.set_param_value('condition value', '`task["cond"]`')
            node.set_param_value('expected values', '`task["exp"]`')

            task0 = context.create_pseudo_task_with_attrs({
                'cond': '',
                'exp': '' if var % 2 == 0 else 'foo'
            }, 234)

            task1 = context.create_pseudo_task_with_attrs({
                'cond': '',
                'exp': 'qwe'
            }, 235)
            task2 = context.create_pseudo_task_with_attrs({
                'cond': '',
                'exp': 'rty'
            }, 236)

            if var == 0:
                self.assertTrue(node.ready_to_process_task(task0.task_dict()))
                node.process_task(task0.get_context_for(node))  # should finish fine
            elif var == 1:
                self.assertFalse(node.ready_to_process_task(task0.task_dict()))
                self.assertRaises(NodeNotReadyToProcess, node.process_task, task0.get_context_for(node))
            else:
                raise NotImplementedError()

            # now task1 task2 should not be processable
            self.assertFalse(node.ready_to_process_task(task1.task_dict()))
            self.assertFalse(node.ready_to_process_task(task2.task_dict()))
            self.assertRaises(NodeNotReadyToProcess, node.process_task, task1.get_context_for(node))
            self.assertRaises(NodeNotReadyToProcess, node.process_task, task2.get_context_for(node))

            # imitate reschedule of the task0
            task0.update_attribs({'cond': 'rty'})
            # this below should be true for both var 0,1 cases
            self.assertTrue(node.ready_to_process_task(task0.task_dict()))
            if var == 0:
                node.process_task(task0.get_context_for(node))  # should finish fine
            elif var == 1:
                self.assertRaises(NodeNotReadyToProcess, node.process_task, task0.get_context_for(node))

            # now task1 should be processable, but task2 should not
            self.assertTrue(node.ready_to_process_task(task2.task_dict()))
            self.assertFalse(node.ready_to_process_task(task1.task_dict()))
            node.process_task(task2.get_context_for(node))  # should finish fine
            self.assertRaises(NodeNotReadyToProcess, node.process_task, task1.get_context_for(node))

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def test_reschedule_with_to_empty_cond_without_exp_set(self):
        await self._helper_test_reschedule_with_to_empty_cond(0)

    async def test_reschedule_with_to_empty_cond_with_exp_set(self):
        await self._helper_test_reschedule_with_to_empty_cond(1)

    async def _helper_test_reschedule_with_to_empty_cond(self, var: int):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            node: BaseNode = context.create_node('wait_for_task_value', 'footest')
            node.set_param_value('condition value', '`task["cond"]`')
            node.set_param_value('expected values', '`task["exp"]`')

            task0 = context.create_pseudo_task_with_attrs({
                'cond': 'qwe',
                'exp': '' if var % 2 == 0 else 'foo'
            }, 234)

            task1 = context.create_pseudo_task_with_attrs({
                'cond': '',
                'exp': 'qwe'
            }, 235)
            task2 = context.create_pseudo_task_with_attrs({
                'cond': '',
                'exp': 'rty'
            }, 236)

            # this below should be true for both var 0,1 cases
            self.assertTrue(node.ready_to_process_task(task0.task_dict()))
            if var == 0:  # exp is empty
                node.process_task(task0.get_context_for(node))  # should finish fine
            elif var == 1:  # exp is NOT empty
                self.assertRaises(NodeNotReadyToProcess, node.process_task, task0.get_context_for(node))
            else:
                raise NotImplementedError()

            # now task1 should pass, task2 should not
            self.assertTrue(node.ready_to_process_task(task1.task_dict()))
            self.assertFalse(node.ready_to_process_task(task2.task_dict()))
            node.process_task(task1.get_context_for(node))  # should finish fine
            self.assertRaises(NodeNotReadyToProcess, node.process_task, task2.get_context_for(node))

            # imitate reschedule of the task0
            task0.update_attribs({'cond': ''})

            # even cond is '' - node MUST allow processing to change inner state
            self.assertTrue(node.ready_to_process_task(task0.task_dict()))
            if var == 0:  # exp is empty
                node.process_task(task0.get_context_for(node))  # should finish fine
            elif var == 1:  # exp is NOT empty
                self.assertRaises(NodeNotReadyToProcess, node.process_task, task0.get_context_for(node))
                # but after processing - should not allow processing again, IF exp is not empty
                self.assertFalse(node.ready_to_process_task(task0.task_dict()))

            # now task1 task2 should not pass
            self.assertFalse(node.ready_to_process_task(task1.task_dict()))
            self.assertFalse(node.ready_to_process_task(task2.task_dict()))
            self.assertRaises(NodeNotReadyToProcess, node.process_task, task1.get_context_for(node))
            self.assertRaises(NodeNotReadyToProcess, node.process_task, task2.get_context_for(node))

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def test_reschedule_with_both_cond_exp_set_to_nothing(self):
        await self._helper_test_reschedule_with_to_from_empty_cond_exp(0)

    async def test_reschedule_with_both_cond_exp_set_from_nothing(self):
        await self._helper_test_reschedule_with_to_from_empty_cond_exp(1)

    async def _helper_test_reschedule_with_to_from_empty_cond_exp(self, var: int):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            node: BaseNode = context.create_node('wait_for_task_value', 'footest')
            node.set_param_value('condition value', '`task["cond"]`')
            node.set_param_value('expected values', '`task["exp"]`')

            task0 = context.create_pseudo_task_with_attrs({
                'cond': 'qwe' if var % 2 == 0 else '',
                'exp': 'foo' if var % 2 == 0 else ''
            }, 234)

            task1 = context.create_pseudo_task_with_attrs({
                'cond': '',
                'exp': 'qwe'
            }, 235)
            task2 = context.create_pseudo_task_with_attrs({
                'cond': '',
                'exp': 'rty'
            }, 236)

            def _check1():
                # now task1 should pass, task2 should not
                self.assertTrue(node.ready_to_process_task(task1.task_dict()))
                self.assertFalse(node.ready_to_process_task(task2.task_dict()))
                node.process_task(task1.get_context_for(node))  # should finish fine
                self.assertRaises(NodeNotReadyToProcess, node.process_task, task2.get_context_for(node))

            def _check2():
                # now task1 task2 should not pass
                self.assertFalse(node.ready_to_process_task(task1.task_dict()))
                self.assertFalse(node.ready_to_process_task(task2.task_dict()))
                self.assertRaises(NodeNotReadyToProcess, node.process_task, task1.get_context_for(node))
                self.assertRaises(NodeNotReadyToProcess, node.process_task, task2.get_context_for(node))

            self.assertTrue(node.ready_to_process_task(task0.task_dict()))
            if var == 0:
                self.assertRaises(NodeNotReadyToProcess, node.process_task, task0.get_context_for(node))
            elif var == 1:
                node.process_task(task0.get_context_for(node))  # should finish fine
            else:
                raise NotImplementedError()

            if var == 0:
                _check1()
            elif var == 1:
                _check2()
            else:
                raise NotImplementedError()

            # imitate reschedule of the task0
            if var == 0:
                task0.update_attribs({'cond': '', 'exp': ''})
            elif var == 1:
                task0.update_attribs({'cond': 'qwe', 'exp': 'foo'})

            # this below should be true for both var 0,1 cases
            self.assertTrue(node.ready_to_process_task(task0.task_dict()))
            if var == 0:
                node.process_task(task0.get_context_for(node))  # should finish fine
            elif var == 1:
                self.assertRaises(NodeNotReadyToProcess, node.process_task, task0.get_context_for(node))

            if var == 0:
                _check2()
            elif var == 1:
                _check1()

        await self._helper_test_node_with_arg_update(
            _logic
        )
