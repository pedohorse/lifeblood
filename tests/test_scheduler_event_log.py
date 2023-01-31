from unittest import TestCase
import time
from lifeblood.scheduler_event_log import SchedulerEventLog, SchedulerEvent
from lifeblood.enums import UIEventType


class Tests(TestCase):
    def test_general(self):
        log = SchedulerEventLog(9999999, 6)

        self.assertEqual(0, len(log))

        startstamp = time.time()
        log.append(SchedulerEvent(2, UIEventType.UPDATE))
        self.assertEqual(1, len(log))
        log.append(SchedulerEvent(3, UIEventType.UPDATE))
        self.assertEqual(2, len(log))
        log.append(SchedulerEvent(5, UIEventType.UPDATE))
        self.assertEqual(3, len(log))
        test_timestamp0 = time.time()       # test timestamp 0
        log.append(SchedulerEvent(6, UIEventType.UPDATE))
        self.assertEqual(4, len(log))
        missed_event0 = SchedulerEvent(10, UIEventType.UPDATE)
        log.append(SchedulerEvent(16, UIEventType.UPDATE))
        self.assertEqual(5, len(log))
        self.assertEqual((2, 3, 5, 6, 16), tuple(x.event_id for x in log.get_since_timestamp(startstamp)))
        self.assertEqual((6, 16), tuple(x.event_id for x in log.get_since_timestamp(test_timestamp0)))
        log.append(SchedulerEvent(22, UIEventType.UPDATE))
        self.assertEqual(6, len(log))
        test_timestamp1 = time.time()       # test timestamp 1
        log.append(SchedulerEvent(23, UIEventType.UPDATE))
        self.assertEqual(6, len(log))
        missed_event1 = SchedulerEvent(24, UIEventType.UPDATE)
        event = SchedulerEvent(25, UIEventType.UPDATE)
        log.append(event)
        self.assertEqual(6, len(log))
        self.assertEqual((5, 6, 16, 22, 23, 25), tuple(x.event_id for x in log.get_since_timestamp(startstamp)))
        self.assertEqual((6, 16, 22, 23, 25), tuple(x.event_id for x in log.get_since_timestamp(test_timestamp0)))
        self.assertEqual((23, 25), tuple(x.event_id for x in log.get_since_timestamp(test_timestamp1)))

        log.append(missed_event0)
        self.assertEqual(6, len(log))
        self.assertEqual((6, 10, 16, 22, 23, 25), tuple(x.event_id for x in log.get_since_timestamp(startstamp)))
        self.assertEqual((6, 10, 16, 22, 23, 25), tuple(x.event_id for x in log.get_since_timestamp(test_timestamp0)))
        self.assertEqual((23, 25), tuple(x.event_id for x in log.get_since_timestamp(test_timestamp1)))

        log.append(missed_event1)
        self.assertEqual(6, len(log))
        self.assertEqual((10, 16, 22, 23, 24, 25), tuple(x.event_id for x in log.get_since_timestamp(startstamp)))
        self.assertEqual((10, 16, 22, 23, 24, 25), tuple(x.event_id for x in log.get_since_timestamp(test_timestamp0)))
        self.assertEqual((23, 24, 25), tuple(x.event_id for x in log.get_since_timestamp(test_timestamp1)))

        log.append(event)  # should not raise, but give warning
        self.assertRaises(RuntimeError, log.append, SchedulerEvent(25, UIEventType.UPDATE))  # timestamp different - should raise
