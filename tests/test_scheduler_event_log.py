from unittest import TestCase
import time
from lifeblood.scheduler_event_log import SchedulerEventLog
from lifeblood.ui_events import SchedulerEvent
from lifeblood.enums import UIEventType


class Tests(TestCase):
    def test_general(self):
        log = SchedulerEventLog(log_time_length_max=9999999, log_event_count_max=6)

        self.assertEqual(0, len(log))

        startstamp = time.time()
        log.add_event(SchedulerEvent(2, UIEventType.UPDATE))
        self.assertEqual(1, len(log))
        log.add_event(SchedulerEvent(3, UIEventType.UPDATE))
        self.assertEqual(2, len(log))
        log.add_event(SchedulerEvent(5, UIEventType.UPDATE))
        self.assertEqual(3, len(log))
        test_timestamp0 = time.time()       # test timestamp 0
        log.add_event(SchedulerEvent(6, UIEventType.UPDATE))
        self.assertEqual(4, len(log))
        missed_event0 = SchedulerEvent(10, UIEventType.UPDATE)
        log.add_event(SchedulerEvent(16, UIEventType.UPDATE))
        self.assertEqual(5, len(log))
        self.assertEqual((2, 3, 5, 6, 16), tuple(x.event_id for x in log.get_since_timestamp(startstamp)))
        self.assertEqual((6, 16), tuple(x.event_id for x in log.get_since_timestamp(test_timestamp0)))
        log.add_event(SchedulerEvent(22, UIEventType.UPDATE))
        self.assertEqual(6, len(log))
        test_timestamp1 = time.time()       # test timestamp 1
        log.add_event(SchedulerEvent(23, UIEventType.UPDATE))
        self.assertEqual(6, len(log))
        missed_event1 = SchedulerEvent(24, UIEventType.UPDATE)
        event = SchedulerEvent(25, UIEventType.UPDATE)
        log.add_event(event)
        self.assertEqual(6, len(log))
        self.assertEqual((5, 6, 16, 22, 23, 25), tuple(x.event_id for x in log.get_since_timestamp(startstamp)))
        self.assertEqual((6, 16, 22, 23, 25), tuple(x.event_id for x in log.get_since_timestamp(test_timestamp0)))
        self.assertEqual((23, 25), tuple(x.event_id for x in log.get_since_timestamp(test_timestamp1)))

        log.add_event(missed_event0)
        self.assertEqual(6, len(log))
        self.assertEqual((6, 10, 16, 22, 23, 25), tuple(x.event_id for x in log.get_since_timestamp(startstamp)))
        self.assertEqual((6, 10, 16, 22, 23, 25), tuple(x.event_id for x in log.get_since_timestamp(test_timestamp0)))
        self.assertEqual((23, 25), tuple(x.event_id for x in log.get_since_timestamp(test_timestamp1)))

        log.add_event(missed_event1)
        self.assertEqual(6, len(log))
        self.assertEqual((10, 16, 22, 23, 24, 25), tuple(x.event_id for x in log.get_since_timestamp(startstamp)))
        self.assertEqual((10, 16, 22, 23, 24, 25), tuple(x.event_id for x in log.get_since_timestamp(test_timestamp0)))
        self.assertEqual((23, 24, 25), tuple(x.event_id for x in log.get_since_timestamp(test_timestamp1)))

        log.add_event(event)  # should not raise, but give warning
        self.assertRaises(RuntimeError, log.add_event, SchedulerEvent(25, UIEventType.UPDATE))  # timestamp different - should raise

    def test_null_time(self):
        log = SchedulerEventLog(log_time_length_max=None, log_event_count_max=10)
        time.sleep(0.25)
        self.assertEqual(0, len(log))
        log.add_event(SchedulerEvent(0, UIEventType.UPDATE))
        time.sleep(0.3)
        self.assertEqual(1, len(log))
        log.add_event(SchedulerEvent(1, UIEventType.UPDATE))
        time.sleep(0.35)
        self.assertEqual(2, len(log))
        log.add_event(SchedulerEvent(2, UIEventType.UPDATE))
        time.sleep(0.4)
        self.assertEqual(3, len(log))
        log.add_event(SchedulerEvent(4, UIEventType.UPDATE))
        time.sleep(0.45)
        self.assertEqual(4, len(log))
        log.add_event(SchedulerEvent(10, UIEventType.UPDATE))
        time.sleep(0.5)
        self.assertEqual(5, len(log))
        log.trim()
        self.assertEqual(5, len(log))

    def test_null_count(self):
        log = SchedulerEventLog(log_time_length_max=0.5, log_event_count_max=None)
        self.assertEqual(0, len(log))
        log.add_event(SchedulerEvent(0, UIEventType.UPDATE))
        self.assertEqual(1, len(log))
        time.sleep(0.2)
        log.add_event(SchedulerEvent(1, UIEventType.UPDATE))
        self.assertEqual(2, len(log))
        time.sleep(0.2)
        log.add_event(SchedulerEvent(2, UIEventType.UPDATE))
        self.assertEqual(3, len(log))
        time.sleep(0.2)
        log.add_event(SchedulerEvent(4, UIEventType.UPDATE))
        self.assertEqual(3, len(log))
        time.sleep(0.6)
        log.add_event(SchedulerEvent(10, UIEventType.UPDATE))
        self.assertEqual(1, len(log))

    def test_benchmark1(self):
        # a sanity check more like
        log = SchedulerEventLog(log_time_length_max=10, log_event_count_max=9999999)

        perf0 = time.perf_counter()
        for i in range(1000):
            log.add_event(SchedulerEvent(i, UIEventType.UPDATE), do_trim=True)
        perf1 = time.perf_counter()
        delta = perf1 - perf0
        print(delta)

        self.assertLess(delta, 0.01)

    def test_benchmark2(self):
        # a sanity check more like
        log = SchedulerEventLog(log_time_length_max=10, log_event_count_max=100)

        perf0 = time.perf_counter()
        for i in range(1000):  # truncations will start after first 100 events
            log.add_event(SchedulerEvent(i, UIEventType.UPDATE), do_trim=True)
        perf1 = time.perf_counter()
        delta = perf1 - perf0
        print(delta)

        self.assertLess(delta, 0.01)
