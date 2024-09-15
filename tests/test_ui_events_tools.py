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

