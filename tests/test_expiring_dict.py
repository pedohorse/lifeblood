import time
from unittest import TestCase
from lifeblood.expiring_collections import ExpiringSet, ExpiringValuesSetMap


class TestExpiringCollections(TestCase):
    def test_expiring_set(self):
        eset = ExpiringSet()
        self.assertFalse(eset.contains(123))

        eset.add(123, 1)
        eset.add(234, 2)
        self.assertTrue(eset.contains(123))
        self.assertTrue(eset.contains(234))
        self.assertSetEqual({123, 234}, set(eset.items()))
        time.sleep(1.1)
        self.assertFalse(eset.contains(123))
        self.assertTrue(eset.contains(234))
        self.assertSetEqual({234}, set(eset.items()))
        time.sleep(1.1)
        self.assertFalse(eset.contains(123))
        self.assertFalse(eset.contains(234))
        self.assertSetEqual(set(), set(eset.items()))

    def test_expiring_dict(self):
        d = ExpiringValuesSetMap()

        self.assertEqual(0, len(d))
        d.add_expiring_value(123, 'fff', 1)
        d.add_expiring_value(123, 'www', 2)
        d.add_expiring_value(123, 'ghj', 1)
        d.add_expiring_value(234, 'zxc', 1)
        self.assertEqual(2, len(d))
        self.assertEqual(3, len(tuple(d.get_values(123))))
        self.assertEqual(1, len(tuple(d.get_values(234))))
        time.sleep(1.1)
        self.assertEqual(1, len(d))
        self.assertEqual(1, len(tuple(d.get_values(123))))
        self.assertEqual(0, len(tuple(d.get_values(234))))
        time.sleep(1.1)
        self.assertEqual(0, len(d))
