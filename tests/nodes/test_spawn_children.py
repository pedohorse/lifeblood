from asyncio import Event
import string
import random
from lifeblood.scheduler import Scheduler
from lifeblood.worker import Worker
from lifeblood.basenode import BaseNode
from lifeblood.enums import TaskState
from lifeblood.exceptions import NodeNotReadyToProcess
from .common import TestCaseBase, PseudoContext

from typing import List


valid_chars = string.printable
valid_chars = valid_chars.replace('`', '')


class TestParentChildrenWaiter(TestCaseBase):
    async def test_basic(self):
        async def _logic(sched: Scheduler, workers: List[Worker], done_waiter: Event, context: PseudoContext):
            rng = random.Random(132435)
            more_attrs = {'alfa' + ''.join(rng.choice(valid_chars) + 'b' for _ in range(rng.randint(1, 32))): rng.randint(1, 111) for _ in range(rng.randint(1, 14))}
            attrs = {
                'attrib_baba': 135,
                'attr_foofa': 'magrat',
                'a_loopa': [1, 2, 'a'],
                'beeba': ['aqa', 3.3],
                **more_attrs
            }
            task = context.create_pseudo_task_with_attrs(attrs)

            for _ in range(100):
                num_children = 3
                num_attr_name = ''.join(rng.choice(valid_chars) for _ in range(rng.randint(1, 32)))
                child_base_name = ''.join(rng.choice(valid_chars) for _ in range(rng.randint(1, 32)))

                node: BaseNode = context.create_node('spawn_children', 'footest')
                node.set_param_value('count', num_children)
                node.set_param_value('number attribute', num_attr_name)
                node.set_param_value('child basename', child_base_name)
                node.set_param_value('inherit attributes', 'a_loopa alfa* *foofa')

                res = context.process_task(node, task)
                self.assertIsNotNone(res)

                self.assertEqual(num_children, len(res.spawn_list))
                expected_numbers = set(range(num_children))
                for task_spawn in res.spawn_list:
                    self.assertSetEqual({num_attr_name, *{x for x in more_attrs}, 'attr_foofa', 'a_loopa'}, set(task_spawn._attributes().keys()))
                    self.assertEqual([1, 2, 'a'], task_spawn.attribute_value('a_loopa'))
                    self.assertEqual('magrat', task_spawn.attribute_value('attr_foofa'))
                    for attr, value in more_attrs.items():
                        self.assertEqual(value, task_spawn.attribute_value(attr))
                        
                    expected_numbers.remove(task_spawn.attribute_value(num_attr_name))
                    self.assertTrue(task_spawn.name().startswith(child_base_name))
                self.assertEqual(0, len(expected_numbers))

        await self._helper_test_node_with_arg_update(
            _logic
        )
