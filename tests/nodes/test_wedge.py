import random
from asyncio import Event
from lifeblood.scheduler import Scheduler
from lifeblood.worker import Worker
from lifeblood.basenode import BaseNode, ProcessingError
from lifeblood.exceptions import NodeNotReadyToProcess
from .common import TestCaseBase, PseudoContext

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
