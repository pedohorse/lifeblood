import random
import ast
from asyncio import Event
from lifeblood.scheduler.scheduler import Scheduler
from lifeblood.worker import Worker
from lifeblood.nodethings import ProcessingError
from lifeblood_testing_common.nodes_common import TestCaseBase, PseudoContext

from typing import List


class TestHipScript(TestCaseBase):
    async def test_trivial(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            task = context.create_pseudo_task_with_attrs({})

            node = context.create_node('hip_script', 'footest')

            node.set_param_value('hip path', '/tmp/some/path/stuff.hip')
            node.set_param_value('script', 'do_things()')

            res = context.process_task(node, task)
            script = res.invocation_job.extra_files()['work_to_do.py']
            ast.parse(script)

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def test_empty_script(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            task = context.create_pseudo_task_with_attrs({})

            node = context.create_node('hip_script', 'footest')

            node.set_param_value('hip path', '/tmp/some/path/stuff.hip')
            node.set_param_value('script', '')

            res = context.process_task(node, task)
            script = res.invocation_job.extra_files()['work_to_do.py']
            ast.parse(script)

        await self._helper_test_node_with_arg_update(
            _logic
        )

    async def test_empty_hip_path(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            task = context.create_pseudo_task_with_attrs({})

            node = context.create_node('hip_script', 'footest')

            node.set_param_value('hip path', '')
            node.set_param_value('script', '')

            self.assertRaises(ProcessingError, context.process_task, node, task)

        await self._helper_test_node_with_arg_update(
            _logic
        )
