import io
from unittest import TestCase
from io import BytesIO
from lifeblood.enums import TaskState, WorkerType, WorkerState, TaskGroupArchivedState
from lifeblood.buffered_connection import BufferedReaderWrapper
from lifeblood.ui_protocol_data import UiData, NodeGraphStructureData, \
    NodeData, NodeConnectionData, TaskBatchData, TaskData, WorkerData, WorkerResources, WorkerBatchData, \
    TaskGroupData, TaskGroupStatisticsData, TaskGroupBatchData, TaskDelta
from lifeblood.ui_events import TasksChanged, TasksUpdated, TasksRemoved, TaskFullState, TaskEvent


class Tests(TestCase):
    def test_trivial1(self):
        exp_uid = UiData(
            123456,
            NodeGraphStructureData(0, {
                1: NodeData(1, "name1", "supertype"),
                22: NodeData(22, "foo", "megatype")
            }, {
                32: NodeConnectionData(32, 321, "mama", 234, "papa"),
                41: NodeConnectionData(41, 123, "quaq", 919, "guac")
            }),
            TaskBatchData(0, {
                11: TaskData(11, None, 10, 5, TaskState.ERROR, "oh noooo", True, 1531, "main", "out",
                             'a task', 2, 11, 52.1, 8484, 158, 816, {'groo', 'froo'})
            }), WorkerBatchData(0, {
                121: WorkerData(121, WorkerResources(12.3, 23.4, 929283838, 939384848, 56.7, 67.8, 84847575, 85857676),
                                '1928374', '127.1.2.333:blo', 1234567, WorkerState.BUSY, WorkerType.STANDARD,
                                1534, 8273, 7573, 55.5, {"borker", "gorker"})
            }), TaskGroupBatchData(0, {
                'grooup foo': TaskGroupData('grooup foo', 2345678, TaskGroupArchivedState.ARCHIVED, 22.3,
                                            TaskGroupStatisticsData(45, 56, 67, 78))
            }))

        buffer = BytesIO()
        buffer.write(b'cat')
        exp_uid.serialize(buffer)
        print(buffer.tell())
        footer = b'qweasdzxc123qweasdzxc123qweasdzxc123qweasdzxc123@!@qweasdzxc123qweasdzxc123qweasdzxc123qweasdzxc123'
        buffer.write(footer)
        print(buffer.tell())

        buffer.seek(0)
        reader = BufferedReaderWrapper(io.BufferedReader(buffer), 128)
        self.assertEqual(b'cat', buffer.read(3))
        test_uid = UiData.deserialize(reader)

        self.assertEqual(exp_uid.db_uid, test_uid.db_uid)
        self.assertEqual(exp_uid.graph_data, test_uid.graph_data)
        self.assertEqual(exp_uid.tasks, test_uid.tasks)
        self.assertEqual(exp_uid.workers, test_uid.workers)
        self.assertEqual(exp_uid.task_groups, test_uid.task_groups)
        self.assertEqual(footer, reader.readexactly(len(footer)))

    def test_many_groups(self):
        groups = {}
        for i in range(10000):
            tgdata = TaskGroupData(f'groupo_{i}', 123456+i, TaskGroupArchivedState.NOT_ARCHIVED, 51+0.0001*i, TaskGroupStatisticsData(i*2, i, i, i*4))
            groups[tgdata.name] = tgdata
        batch_data = TaskGroupBatchData(0, groups)

        buffer = BytesIO()
        batch_data.serialize(buffer)
        print(buffer.tell())
        # TODO: what am i testing here?
        buffer.seek(0)
        returned_data = TaskGroupBatchData.deserialize(BufferedReaderWrapper(io.BufferedReader(buffer), 128))
        self.assertEqual(batch_data, returned_data)

    def test_delta_serialization_simple(self):
        def _do_check(task_delta):
            buffer = BytesIO()
            task_delta.serialize(buffer)
            buffer.seek(0)
            rtd = TaskDelta.deserialize(BufferedReaderWrapper(io.BufferedReader(buffer), ))
            self.assertEqual(task_delta, rtd)

        _do_check(TaskDelta(123, children_count=3, state=TaskState.POST_GENERATING, name='sdfa'))
        _do_check(TaskDelta(124,))
        _do_check(TaskDelta(125, parent_id=None, children_count=44, active_children_count=33, state=TaskState.GENERATING,
                            state_details=None, paused=True, node_id=932, node_input_name=None, node_output_name=None,
                            name='barfoo', split_level=5, work_data_invocation_attempt=13, progress=None,
                            split_origin_task_id=None, split_id=None, invocation_id=None, groups=set()))
        _do_check(TaskDelta(126, parent_id=23, children_count=44, active_children_count=33, state=TaskState.GENERATING,
                            state_details='shmetails', paused=True, node_id=932, node_input_name='lil', node_output_name='squeak',
                            name='barfoo', split_level=5, work_data_invocation_attempt=13, progress=45.7123,
                            split_origin_task_id=74, split_id=19, invocation_id=83712, groups={'foo', 'bar', 'car'}))

    def test_event_serialization(self):
        te0 = TasksChanged(146, [TaskDelta(9382, paused=True, node_id=74)])
        te1 = TasksUpdated(247754321, TaskBatchData(247754321,
                                                    {434: TaskData(434, 2, 1, 0, TaskState.ERROR, 'woof', True,
                                                                   412, 'moon', 'boom', 'falafel', 2, 1, 3.45264, 1,
                                                                   928122, 958285, {'foo', 'bar', 'wood'})}))
        te2 = TasksRemoved(83159, (2, 3, 4, 5, 912, 3495983, 838111, 943821))
        te3 = TaskFullState(91803834, TaskBatchData(91803834,
                                                    {379: TaskData(379, 1, 2, 2, TaskState.WAITING, '1woof', False,
                                                                   812, 'shamoon', 'boomash', 'falafel2', 5, 12, 13.46718, 0,
                                                                   64788, 9911, {'1foo', '1bar', 'woody'})}))

        te0.event_id = 123
        te1.event_id = 1234
        te2.event_id = 12345
        te3.event_id = 123456

        buffer = BytesIO()
        te0.serialize(buffer)
        te1.serialize(buffer)
        te2.serialize(buffer)
        te3.serialize(buffer)

        buffer.seek(0)
        reader = BufferedReaderWrapper(io.BufferedReader(buffer))
        e0 = TaskEvent.deserialize(reader)
        e1 = TaskEvent.deserialize(reader)
        e2 = TaskEvent.deserialize(reader)
        e3 = TaskEvent.deserialize(reader)

        self.assertEqual(te0, e0)
        self.assertEqual(te1, e1)
        self.assertEqual(te2, e2)
        self.assertEqual(te3, e3)
