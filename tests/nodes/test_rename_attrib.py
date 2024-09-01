import random
from asyncio import Event
from lifeblood.scheduler import Scheduler
from lifeblood.worker import Worker
from lifeblood.nodethings import ProcessingError
from lifeblood_testing_common.nodes_common import TestCaseBase, PseudoContext

from typing import List


class TestRenameAttrib(TestCaseBase):
    async def test_noop(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            task = context.create_pseudo_task_with_attrs({'foo': 123, 'bar': 'wow'})

            node = context.create_node('rename_attrib', 'footest')

            res = context.process_task(node, task)
            self.assertEqual({}, res.attributes_to_set)

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def test_same(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            task = context.create_pseudo_task_with_attrs({'foo': 123, 'bar': 'wow'})

            node = context.create_node('rename_attrib', 'footest')
            node.set_param_value('num', 1)
            node.set_param_value('oldname_0', 'foo')
            node.set_param_value('newname_0', 'foo')

            res = context.process_task(node, task)
            self.assertEqual({}, res.attributes_to_set)

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def test_basic(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            for pref, suff in (('', ''), (' ', ''), ('  ', ''), ('', ' '), ('', '  '), (' ', ' '), (' ', '  '), ('  ', ' ')):
                task = context.create_pseudo_task_with_attrs({'foo': 123, 'bar': 'wow'})

                node = context.create_node('rename_attrib', 'footest')
                node.set_param_value('num', 1)
                node.set_param_value('oldname_0', f'{pref}foo{suff}')
                node.set_param_value('newname_0', f'{pref}bla{suff}')

                res = context.process_task(node, task)
                self.assertEqual({
                    'bla': 123,
                    'foo': None,
                }, res.attributes_to_set)

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def test_chain(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):

            task = context.create_pseudo_task_with_attrs({'foo': 123, 'bar': 'wow'})

            node = context.create_node('rename_attrib', 'footest')
            node.set_param_value('ignore errors', False)
            node.set_param_value('num', 2)
            node.set_param_value('oldname_0', f'foo')
            node.set_param_value('newname_0', f'middle')
            node.set_param_value('oldname_1', f'middle')
            node.set_param_value('newname_1', f'bla')

            res = context.process_task(node, task)
            self.assertEqual({
                'bla': 123,
                'foo': None,
                'middle': None,
            }, res.attributes_to_set)

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def test_overlap(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            task = context.create_pseudo_task_with_attrs({'foo': 123, 'bar': 'wow'})

            node = context.create_node('rename_attrib', 'footest')
            node.set_param_value('num', 1)
            node.set_param_value('oldname_0', 'foo')
            node.set_param_value('newname_0', 'bar')

            res = context.process_task(node, task)
            self.assertEqual({
                'bar': 123,
                'foo': None,
            }, res.attributes_to_set)

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def test_nonexisting_error(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            task = context.create_pseudo_task_with_attrs({'foo': 123, 'bar': 'wow'})

            node = context.create_node('rename_attrib', 'footest')
            node.set_param_value('ignore errors', False)
            node.set_param_value('num', 1)
            node.set_param_value('oldname_0', 'faaaaa')
            node.set_param_value('newname_0', 'bla')

            self.assertRaises(ProcessingError, context.process_task, node, task)

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def test_nonexisting_noerror(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            task = context.create_pseudo_task_with_attrs({'foo': 123, 'bar': 'wow'})

            node = context.create_node('rename_attrib', 'footest')
            node.set_param_value('ignore errors', True)
            node.set_param_value('num', 1)
            node.set_param_value('oldname_0', 'faaaaa')
            node.set_param_value('newname_0', 'bla')

            res = context.process_task(node, task)
            self.assertEqual({}, res.attributes_to_set)

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def test_empty_error(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            for from_attr, to_attr in [('', 'foo'), ('foo', ''), ('', ''), (' ', '   '), ('bar', '  '), (' ', 'bar')]:
                task = context.create_pseudo_task_with_attrs({'foo': 123, 'bar': 'wow'})

                node = context.create_node('rename_attrib', 'footest')
                node.set_param_value('ignore errors', False)
                node.set_param_value('num', 1)
                node.set_param_value('oldname_0', from_attr)
                node.set_param_value('newname_0', to_attr)

                self.assertRaises(ProcessingError, context.process_task, node, task)

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def test_empty_noerror(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            for from_attr, to_attr in [('', 'foo'), ('foo', ''), ('', ''), (' ', '   '), ('bar', '  '), (' ', 'bar')]:
                task = context.create_pseudo_task_with_attrs({'foo': 123, 'bar': 'wow'})

                node = context.create_node('rename_attrib', 'footest')
                node.set_param_value('ignore errors', True)
                node.set_param_value('num', 1)
                node.set_param_value('oldname_0', from_attr)
                node.set_param_value('newname_0', to_attr)

                res = context.process_task(node, task)
                self.assertEqual({}, res.attributes_to_set)  # expect noop

        await self._helper_test_node_with_arg_update(
            _logic
        )
