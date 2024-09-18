import copy
import random
from unittest import TestCase
from lifeblood.ui_events_tools import collapse_task_event_list
from lifeblood.ui_events import TaskFullState, TasksChanged, TasksRemoved, TasksUpdated
from lifeblood.ui_protocol_data import TaskData, TaskBatchData, TaskDelta
from lifeblood.enums import TaskState


class TestUIEventsTools(TestCase):
    def test_collapse_task_event_list_trivial(self):
        self.assertIsNone(collapse_task_event_list([]))

        data = TaskBatchData(
            12345,
            {
                2: TaskData(
                    2, None, 4, 3, TaskState.WAITING, 'bla blaa', False, 55, 'maino', 'bleion', 'footask', 5, 6, 0.567, 7, 8, 9, {'qwe', 'asd', 'zxc'},
                )
            }
        )
        fullstate = TaskFullState(
            12345,
            data,
        )

        self.assertEqual(data, collapse_task_event_list([fullstate]))

    def test_collapse_task_event_list_errors(self):
        self.assertRaises(RuntimeError, collapse_task_event_list, [
            TaskFullState(
                12345, TaskBatchData(12345, {})
            ),
            TasksChanged(
                23456, []
            ),
        ])

        self.assertRaises(RuntimeError, collapse_task_event_list, [
            TasksRemoved(
                12345, (1,)
            )
        ])

        self.assertRaises(RuntimeError, collapse_task_event_list, [
            TasksChanged(
                12345, [
                    TaskDelta(
                        2,
                        children_count=123,
                    ),
                ],
            )
        ])

    def test_collapse_task_event_list_common1(self):
        fullstate_init = TaskFullState(
            12345,
            TaskBatchData(
                12345,
                {
                    2: TaskData(
                        2, None, 4, 3, TaskState.WAITING, 'bla blaa', False, 55, 'maino', 'bleion', 'footask', 5, 6, 0.567, 7, 8, 9, {'qwe', 'asd', 'zxc'},
                    )
                }
            ),
        )
        fulldata_final = TaskBatchData(
            12345,
            {
                2: TaskData(
                    2, None, 4, 3, TaskState.WAITING, 'bla blaa', False, 55, 'maino', 'bleion', 'footask', 5, 6, 0.567, 7, 8, 9, {'qwe', 'asd', 'zxc'},
                ),
                22: TaskData(
                    22, None, 44, 33, TaskState.POST_WAITING, 'beeba', True, 555, 'maino1', 'bleion1', 'bartask', 55, 66, 0.5678, 77, 88, 99, {'fgh'},
                ),
            }
        )
        event_list = [
            fullstate_init,
            TasksUpdated(
                12345,
                TaskBatchData(
                    12345,
                    {
                        22: TaskData(
                            22, None, 0, 33, TaskState.DONE, None, True, 555, 'maino1', '', 'bartask', 55, 66, 0, 77, 88, 99, set(),
                        ),
                    }
                )
            ),
            TasksChanged(
                12345,
                [
                    TaskDelta(
                        22,
                        children_count=44,
                        state=TaskState.POST_WAITING,
                        state_details='beeba',
                        node_output_name='badbad',
                        groups={'hhh'},
                    )
                ]
            ),
            TasksUpdated(
                12345,
                TaskBatchData(
                    12345,
                    {
                        33: TaskData(
                            33, None, 0, 33, TaskState.DONE, None, True, 555, 'agageh', '', 'jgfjft', 55, 66, 0, 77, 88, 99, set(),
                        ),
                    }
                )
            ),
            TasksChanged(
                12345,
                [
                    TaskDelta(
                        22,
                        node_output_name='bleion1',
                        progress=0.5678,
                        groups={'fgh'},
                    )
                ]
            ),
            TasksRemoved(
                12345,
                (33,),
            )
        ]

        self.assertEqual(
            fulldata_final,
            collapse_task_event_list(event_list)
        )

    def test_ensure_source_unmodified(self):
        update_event = TasksUpdated(
            12345,
            TaskBatchData(
                12345,
                {
                    123: TaskData(123, 234, 444, 333, TaskState.GENERATING, 'nope', True, 345, 'floo', 'flee', 'nonde', 456, 567, 0.51423, 678, 789, 890, {'karrr'},),
                }
            )
        )
        full_event = TaskFullState(
            12345,
            TaskBatchData(
                12345,
                {
                    123: TaskData(123, 234, 444, 333, TaskState.GENERATING, 'nope', True, 345, 'floo', 'flee', 'nonde', 456, 567, 0.51423, 678, 789, 890, {'karrr'}, ),
                }
            )
        )
        delta_event = TasksChanged(
            12345,
            [
                TaskDelta(123, children_count=999, split_origin_task_id=888, name='foooooooo')
            ]
        )

        update_event_control = copy.deepcopy(update_event)
        full_event_control = copy.deepcopy(full_event)

        collapsed_data = collapse_task_event_list([full_event, delta_event])
        self.assertIsNotNone(collapsed_data)
        collapsed_data = collapse_task_event_list([update_event, delta_event])
        self.assertIsNotNone(collapsed_data)

        self.assertEqual(update_event_control, update_event)
        self.assertEqual(full_event_control, full_event)

    def test_random_change(self):
        rng = random.Random(1827361)
        for _ in range(999):
            fields = list(TaskDelta.__annotations__.keys())
            rng.shuffle(fields)
            delta = TaskDelta(123)
            attrs_set = {}
            for field in fields[:rng.randint(0, len(fields))]:
                if field == 'id':
                    continue
                # NOTE: we ignore typing, which may cause test fails on correct implementations
                val = random.randint(0, 99999)
                setattr(delta, field, val)
                attrs_set[field] = val

            task_data_control = TaskData(123, 234, 444, 333, TaskState.GENERATING, 'nope', True, 345, 'floo', 'flee', 'nonde', 456, 567, 0.51423, 678, 789, 890, {'karrr'},)
            task_data = TaskData(123, 234, 444, 333, TaskState.GENERATING, 'nope', True, 345, 'floo', 'flee', 'nonde', 456, 567, 0.51423, 678, 789, 890, {'karrr'},)
            event_list = [
                TasksUpdated(
                    12345,
                    TaskBatchData(
                        12345,
                        {
                            123: task_data,
                        }
                    )
                ),
                TasksChanged(
                    12345,
                    [
                        delta
                    ]
                )
            ]

            collapsed_data = collapse_task_event_list(event_list)
            self.assertIsNotNone(collapsed_data)

            # ensure that original event was not changed
            self.assertEqual(task_data_control, task_data)

            self.assertSetEqual({123}, set(collapsed_data.tasks.keys()))
            for field in TaskDelta.__annotations__.keys():
                if field in attrs_set:
                    expected_val = attrs_set[field]
                else:
                    expected_val = getattr(task_data_control, field)

                self.assertEqual(expected_val, getattr(collapsed_data.tasks[123], field), f'fail in "{field}" field')
