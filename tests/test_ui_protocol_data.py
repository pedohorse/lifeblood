from unittest import TestCase
from io import BytesIO
from lifeblood.ui_protocol_data import UiData, UIDataType, NodeGraphStructureData, \
    NodeData, NodeConnectionData, TaskBatchData


class Tests(TestCase):
    def test_trivial1(self):
        exp_uid = UiData(UIDataType.FULL,
                         123456,
                         NodeGraphStructureData({}, {}),
                         TaskBatchData({}), None, {})

        buffer = BytesIO()
        buffer.write(b'cat')
        exp_uid.serialize(buffer)
        print(buffer.tell())
        footer = b'qweasdzxc123qweasdzxc123qweasdzxc123qweasdzxc123@!@qweasdzxc123qweasdzxc123qweasdzxc123qweasdzxc123'
        buffer.write(footer)
        print(buffer.tell())

        buffer.seek(0)
        self.assertEqual(b'cat', buffer.read(3))
        test_uid = UiData.deserialize(buffer)

        self.assertEqual(exp_uid.event_type, test_uid.event_type)
        self.assertEqual(exp_uid.db_uid, test_uid.db_uid)
        self.assertEqual(exp_uid.graph_data, test_uid.graph_data)
        self.assertEqual(exp_uid.tasks, test_uid.tasks)
        self.assertEqual(exp_uid.workers, test_uid.workers)
        self.assertEqual(exp_uid.task_groups, test_uid.task_groups)
        print(buffer.tell())
        self.assertEqual(footer, buffer.read(len(footer)))
        print(buffer.tell())
