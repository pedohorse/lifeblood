import io
from unittest import TestCase
from io import BytesIO
from lifeblood.enums import TaskState, WorkerType, WorkerState, TaskGroupArchivedState
from lifeblood.buffered_connection import BufferedReaderWrapper
from lifeblood.ui_protocol_data import UiData, NodeGraphStructureData, \
    NodeData, NodeConnectionData, TaskBatchData, TaskData, WorkerData, WorkerResources, WorkerBatchData, \
    TaskGroupData, TaskGroupStatisticsData, TaskGroupBatchData


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
