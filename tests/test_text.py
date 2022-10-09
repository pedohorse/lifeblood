from unittest import TestCase
import random
from lifeblood import text


class TextTestCases(TestCase):

    def test_generate_name(self):
        rng = random.Random(69812)
        for _ in range(1000):
            length = rng.randint(-10, 1000)
            s = text.generate_name(length)
            self.assertIsInstance(s, str)
            self.assertEqual(max(1, length), len(s))

        for _ in range(1000):
            length = rng.randint(-10, 1000)
            max_length = rng.randint(-10, 1000)
            s = text.generate_name(length, max_length)
            self.assertIsInstance(s, str)
            self.assertLessEqual(max(1, length), len(s))
            self.assertGreaterEqual(max(1, max(length, max_length)), len(s))

    def test_match_pattern(self):
        self.assertTrue(text.match_pattern('asd*', 'asdqwezxc'))
        self.assertTrue(text.match_pattern('asd*', 'asd'))
        self.assertFalse(text.match_pattern('asd*', 'as'))
        self.assertFalse(text.match_pattern('asd*', 'ads'))
        self.assertFalse(text.match_pattern('asd*', 'gasd'))

        self.assertTrue(text.match_pattern('asв*qцe', 'asвqцe'))
        self.assertTrue(text.match_pattern('asв*qцe', 'asв.qцe'))
        self.assertTrue(text.match_pattern('asв*qцe', 'asвqцiu0aaqцe'))
        self.assertFalse(text.match_pattern('asв*qцe', 'asqwe'))
        self.assertFalse(text.match_pattern('asв*qцe', 'asвцe'))
        self.assertFalse(text.match_pattern('asв*qцe', 'asвqцeb'))
        self.assertFalse(text.match_pattern('asв*qцe', 'nasвqцe'))

        self.assertTrue(text.match_pattern('asd**qwe', 'asdqwe'))
        self.assertTrue(text.match_pattern('asd**qwe', 'asd.qwe'))
        self.assertTrue(text.match_pattern('asd**qwe', 'asdqwiu0aaqwe'))
        self.assertFalse(text.match_pattern('asd**qwe', 'asqwe'))
        self.assertFalse(text.match_pattern('asd**qwe', 'asdwe'))
        self.assertFalse(text.match_pattern('asd**qwe', 'asdqwer'))
        self.assertFalse(text.match_pattern('asd**qwe', 'zasdqwe'))

        self.assertTrue(text.match_pattern('asd*qwe*zxc', 'asdqwezxc'))
        self.assertTrue(text.match_pattern('asd*qwe*zxc', 'asd.qwezxc'))
        self.assertTrue(text.match_pattern('asd*qwe*zxc', 'asdqwiu0aaqwedqfzxc'))
        self.assertTrue(text.match_pattern('asd*qwe*zxc', 'asdqwedqfzxc'))
        self.assertFalse(text.match_pattern('asd*qwe*zxc', 'asqqwe'))
        self.assertFalse(text.match_pattern('asd*qwe*zxc', 'qwezxc'))
        self.assertFalse(text.match_pattern('asd*qwe*zxc', 'asdzxc'))
        self.assertFalse(text.match_pattern('asd*qwe*zxc', 'asdqwexc'))
        self.assertFalse(text.match_pattern('asd*qwe*zxc', 'asqwezxc'))

        # multiple patterns
        self.assertTrue(text.match_pattern('asd* ^*zxc', 'asd'))
        self.assertTrue(text.match_pattern('asd* ^*zxc', 'asdfkoq'))
        self.assertTrue(text.match_pattern('asd* ^*zxc', 'asdfkoqzx'))
        self.assertTrue(text.match_pattern('asd* ^*zxc', 'asdfko zxc qzx'))
        self.assertFalse(text.match_pattern('asd* ^*zxc', 'asdfko q zxc'))
        self.assertFalse(text.match_pattern('asd* ^*zxc', 'asdfkoqzxc'))
        self.assertFalse(text.match_pattern('asd* ^*zxc', 'asdzxc'))
        self.assertFalse(text.match_pattern('asd* ^*zxc', 'zxc'))

        self.assertTrue(text.match_pattern('asd* ^*zxc z?c', 'asd'))
        self.assertTrue(text.match_pattern('asd* ^*zxc z?c', 'asdfkoq'))
        self.assertTrue(text.match_pattern('asd* ^*zxc z?c', 'asdfkoqzx'))
        self.assertTrue(text.match_pattern('asd* ^*zxc z?c', 'asdfko zxc qzx'))
        self.assertTrue(text.match_pattern('asd* ^*zxc z?c', 'zxc'))
        self.assertTrue(text.match_pattern('asd* ^*zxc z?c', 'zhc'))
        self.assertFalse(text.match_pattern('asd* ^*zxc z?c', 'zxxc'))
        self.assertFalse(text.match_pattern('asd* ^*zxc z?c', 'asdfko q zxc'))
        self.assertFalse(text.match_pattern('asd* ^*zxc z?c', 'asdfkoqzxc'))
        self.assertFalse(text.match_pattern('asd* ^*zxc z?c', 'asdzxc'))

    def test_filter_by_pattern(self):
        tg = ['two', 'рыба', 'oner', 'wotoworm', 'рыба']
        self.assertListEqual(tg, text.filter_by_pattern('one? *t*wo* ^ры*ба рыба', ['one', 'two', 'рыба',
                                                                                    'oneman', 'laone', 'oner',
                                                                                    'wotoworm', 'рыtwwoба',
                                                                                    'рыба']))

    def test_memory_formatting(self):
        self.assertEqual('0B', text.nice_memory_formatting(0))
        for prefix, mult in (('', 1), ('-', -1)):
            self.assertEqual(prefix + '15B', text.nice_memory_formatting(15 * mult))
            self.assertEqual(prefix + '1KB', text.nice_memory_formatting(1500 * mult))
            self.assertEqual(prefix + '2MB', text.nice_memory_formatting(2500000 * mult))
            self.assertEqual(prefix + '3GB', text.nice_memory_formatting(3500000000 * mult))
            self.assertEqual(prefix + '4TB', text.nice_memory_formatting(4500000000000 * mult))
            self.assertEqual(prefix + '5PB', text.nice_memory_formatting(5500000000000000 * mult))
            self.assertEqual(prefix + '6EB', text.nice_memory_formatting(6500000000000000000 * mult))
            self.assertEqual(prefix + '6500EB', text.nice_memory_formatting(6500000000000000000000 * mult))
