import time
from unittest import TestCase
from lifeblood.expiring_dict import ExpiringSet


class TestExpiringDict(TestCase):
    def test_expiring_set(self):
        eset = ExpiringSet()
        self.assertFalse(eset.contains(123))

        eset.add(123, 1)
        eset.add(234, 2)
        self.assertTrue(eset.contains(123))
        self.assertTrue(eset.contains(234))
        self.assertSetEqual({123, 234}, set(eset.items()))
        time.sleep(1)
        self.assertFalse(eset.contains(123))
        self.assertTrue(eset.contains(234))
        self.assertSetEqual({234}, set(eset.items()))
        time.sleep(1)
        self.assertFalse(eset.contains(123))
        self.assertFalse(eset.contains(234))
        self.assertSetEqual(set(), set(eset.items()))
