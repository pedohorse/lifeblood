from unittest import TestCase
from lifeblood.hardware_resources import HardwareResources, HardwareResource


class TestHardwareResources(TestCase):
    def test_eq(self):
        res1 = HardwareResources(
            hwid=1357,
            qwe=22.33
        )
        res1a = HardwareResources(
            hwid=1357,
            qwe=22.33
        )
        res2 = HardwareResources(
            hwid=1357,
            qwe=22.33,
            asd=33,
        )
        res3 = HardwareResources(
            hwid=1358,
            qwe=22.33,
        )
        res4 = HardwareResources(
            hwid=1357,
            qwe=22.34,
        )
        res5 = HardwareResources(
            hwid=1357,
            asd=22.33,
        )

        self.assertEqual(res1, res1)
        self.assertEqual(res1a, res1a)
        self.assertEqual(res2, res2)
        self.assertEqual(res3, res3)
        self.assertEqual(res4, res4)
        self.assertEqual(res5, res5)

        self.assertEqual(res1, res1a)
        self.assertEqual(res1a, res1)
        self.assertNotEqual(res1, res2)
        self.assertNotEqual(res1, res3)
        self.assertNotEqual(res1, res4)
        self.assertNotEqual(res1, res5)
        self.assertNotEqual(res2, res1)
        self.assertNotEqual(res3, res1)
        self.assertNotEqual(res4, res1)
        self.assertNotEqual(res5, res1)

    def test_serde(self):
        res = HardwareResources(
            hwid=135246357,
            foo_bunny=55.6,
            bar_chicken=23,
        )

        act = HardwareResources.deserialize(res.serialize())

        self.assertEqual(res, act)

    def test_get_items(self):
        res = HardwareResources(
            hwid=135246357,
            foo_bunny=55.6,
            bar_chicken=23,
            car_cdr=1234567890123456
        )

        self.assertIsInstance(res['foo_bunny'], HardwareResource)
        self.assertIsInstance(res['bar_chicken'], HardwareResource)
        self.assertIsInstance(res['car_cdr'], HardwareResource)

        self.assertEqual(res.hwid, 135246357)
        self.assertEqual(res['foo_bunny'].value, 55.6)
        self.assertEqual(res['bar_chicken'].value, 23)
        self.assertEqual(res['car_cdr'].value, 1234567890123456)

        self.assertIn('foo_bunny', res)
        self.assertIn('bar_chicken', res)
        self.assertIn('car_cdr', res)
        self.assertNotIn('door_lore', res)
