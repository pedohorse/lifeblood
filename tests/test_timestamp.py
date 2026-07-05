from unittest import TestCase
from lifeblood import timestamp


class TimestampTestCases(TestCase):
    def test_has_tzinfo(self):
        ts = timestamp.global_timestamp_datetime()
        self.assertIsNotNone(ts.tzinfo)

    def test_basically_same_vals(self):
        ts_f = timestamp.global_timestamp_float()
        ts_i = timestamp.global_timestamp_int()
        ts_d = timestamp.global_timestamp_datetime()

        delta = 15 * 60  # no more 0.25h offset

        self.assertAlmostEqual(ts_f, ts_i, delta=delta)
        self.assertAlmostEqual(ts_d.timestamp(), ts_f, delta=delta)
        self.assertAlmostEqual(ts_d.timestamp(), ts_i, delta=delta)

    def test_to_from_timestamp(self):
        ts = timestamp.global_timestamp_datetime()
        self.assertEqual(
            ts,
            timestamp.global_timestamp_to_datetime(ts.timestamp()),
        )
