from unittest import TestCase
import time
from lifeblood.scheduler_event_log import SchedulerEventLog
from lifeblood.ui_events import SchedulerEvent
from lifeblood.enums import UIEventType

dbuid = 1234567


class Tests(TestCase):
    def test_general(self):
        log = SchedulerEventLog(log_time_length_max=9999999, log_event_count_max=6)

        self.assertEqual(0, len(log))

        startstamp = time.time_ns()
        time.sleep(0.000001)
        log.add_event(SchedulerEvent(2, UIEventType.UPDATE, dbuid))
        self.assertEqual(1, len(log))
        time.sleep(0.000001)  # cicd test machines seem to may provide a very coarse time resolution, so we ensure
        log.add_event(SchedulerEvent(3, UIEventType.UPDATE, dbuid))
        self.assertEqual(2, len(log))
        time.sleep(0.000001)
        log.add_event(SchedulerEvent(5, UIEventType.UPDATE, dbuid))
        self.assertEqual(3, len(log))
        time.sleep(0.000001)
        test_timestamp0 = time.time_ns()       # test timestamp 0
        time.sleep(0.000001)
        log.add_event(SchedulerEvent(6, UIEventType.UPDATE, dbuid))
        self.assertEqual(4, len(log))
        time.sleep(0.000001)
        missed_event0 = SchedulerEvent(10, UIEventType.UPDATE, dbuid)
        log.add_event(SchedulerEvent(16, UIEventType.UPDATE, dbuid))
        self.assertEqual(5, len(log))
        time.sleep(0.000001)

        evs = log.get_since_event(1)  # non existing events should work properly
        print(f'start:{startstamp} ev0:{evs[0].timestamp} ev-1:{evs[-1].timestamp} end:{time.time_ns()}')
        self.assertEqual(5, len(evs))
        time.sleep(0.000001)
        evs = log.get_since_event(2)
        self.assertEqual(4, len(evs))
        time.sleep(0.000001)
        evs = log.get_since_event(4)  # non existing events should work properly
        self.assertEqual(3, len(evs))
        time.sleep(0.000001)
        evs = log.get_since_event(9)  # non existing events should work properly
        self.assertEqual(1, len(evs))
        time.sleep(0.000001)

        self.assertEqual((2, 3, 5, 6, 16), tuple(x.event_id for x in log.get_since_timestamp_ns(startstamp)))
        self.assertEqual((6, 16), tuple(x.event_id for x in log.get_since_timestamp_ns(test_timestamp0)))
        log.add_event(SchedulerEvent(22, UIEventType.UPDATE, dbuid))
        self.assertEqual(6, len(log))
        time.sleep(0.000001)
        test_timestamp1 = time.time_ns()       # test timestamp 1
        time.sleep(0.000001)
        log.add_event(SchedulerEvent(23, UIEventType.UPDATE, dbuid))
        self.assertEqual(6, len(log))
        time.sleep(0.000001)
        missed_event1 = SchedulerEvent(24, UIEventType.UPDATE, dbuid)
        event = SchedulerEvent(25, UIEventType.UPDATE, dbuid)
        log.add_event(event)
        self.assertEqual(6, len(log))
        self.assertEqual((5, 6, 16, 22, 23, 25), tuple(x.event_id for x in log.get_since_timestamp_ns(startstamp)))
        self.assertEqual((6, 16, 22, 23, 25), tuple(x.event_id for x in log.get_since_timestamp_ns(test_timestamp0)))
        self.assertEqual((23, 25), tuple(x.event_id for x in log.get_since_timestamp_ns(test_timestamp1)))

        log.add_event(missed_event0)
        self.assertEqual(6, len(log))
        time.sleep(0.000001)
        self.assertEqual((6, 10, 16, 22, 23, 25), tuple(x.event_id for x in log.get_since_timestamp_ns(startstamp)))
        self.assertEqual((6, 10, 16, 22, 23, 25), tuple(x.event_id for x in log.get_since_timestamp_ns(test_timestamp0)))
        self.assertEqual((23, 25), tuple(x.event_id for x in log.get_since_timestamp_ns(test_timestamp1)))

        log.add_event(missed_event1)
        self.assertEqual(6, len(log))
        time.sleep(0.000001)
        self.assertEqual((10, 16, 22, 23, 24, 25), tuple(x.event_id for x in log.get_since_timestamp_ns(startstamp)))
        self.assertEqual((10, 16, 22, 23, 24, 25), tuple(x.event_id for x in log.get_since_timestamp_ns(test_timestamp0)))
        self.assertEqual((23, 24, 25), tuple(x.event_id for x in log.get_since_timestamp_ns(test_timestamp1)))

        log.add_event(event)  # should not raise, but give warning
        self.assertRaises(RuntimeError, log.add_event, SchedulerEvent(25, UIEventType.UPDATE, dbuid))  # timestamp different - should raise

    def test_null_time(self):
        log = SchedulerEventLog(log_time_length_max=None, log_event_count_max=10)
        time.sleep(0.25)
        self.assertEqual(0, len(log))
        log.add_event(SchedulerEvent(0, UIEventType.UPDATE, dbuid))
        time.sleep(0.3)
        self.assertEqual(1, len(log))
        log.add_event(SchedulerEvent(1, UIEventType.UPDATE, dbuid))
        time.sleep(0.35)
        self.assertEqual(2, len(log))
        log.add_event(SchedulerEvent(2, UIEventType.UPDATE, dbuid))
        time.sleep(0.4)
        self.assertEqual(3, len(log))
        log.add_event(SchedulerEvent(4, UIEventType.UPDATE, dbuid))
        time.sleep(0.45)
        self.assertEqual(4, len(log))
        log.add_event(SchedulerEvent(10, UIEventType.UPDATE, dbuid))
        time.sleep(0.5)
        self.assertEqual(5, len(log))
        log.trim()
        self.assertEqual(5, len(log))

    def test_null_count(self):
        log = SchedulerEventLog(log_time_length_max=0.5, log_event_count_max=None)
        self.assertEqual(0, len(log))
        log.add_event(SchedulerEvent(0, UIEventType.UPDATE, dbuid))
        self.assertEqual(1, len(log))
        time.sleep(0.2)
        log.add_event(SchedulerEvent(1, UIEventType.UPDATE, dbuid))
        self.assertEqual(2, len(log))
        time.sleep(0.2)
        log.add_event(SchedulerEvent(2, UIEventType.UPDATE, dbuid))
        self.assertEqual(3, len(log))
        time.sleep(0.2)
        log.add_event(SchedulerEvent(4, UIEventType.UPDATE, dbuid))
        self.assertEqual(3, len(log))
        time.sleep(0.6)
        log.add_event(SchedulerEvent(10, UIEventType.UPDATE, dbuid))
        self.assertEqual(1, len(log))

    def test_time_adjust(self):
        """
        it is possible that during work time may be adjusted (from network time for example)
        log sorting may suffer in that case
        """
        log = SchedulerEventLog(log_event_count_max=9999, log_time_length_max=None)
        evs = [
            SchedulerEvent(0, UIEventType.UPDATE, dbuid),
            SchedulerEvent(1, UIEventType.UPDATE, dbuid),
            SchedulerEvent(2, UIEventType.UPDATE, dbuid),
            SchedulerEvent(3, UIEventType.UPDATE, dbuid),
            SchedulerEvent(4, UIEventType.UPDATE, dbuid),
        ]
        evs[2].timestamp -= 100000  # time adjustment occurance 1
        evs[3].timestamp -= 100000  # time adjustment occurance 1 (same)
        evs[4].timestamp -= 200000  # time adjustment occurance 2

        for ev in evs:
            log.add_event(ev)

        # we rely on add_event adjusting timestamps, but not failing completely

        for i, ev in enumerate(evs):
            if i == 0:
                continue
            self.assertGreaterEqual(ev.timestamp, evs[i-1].timestamp)

    def _run_iters(self, count, log_time_length_max, log_event_count_max):
        log = SchedulerEventLog(log_time_length_max=log_time_length_max, log_event_count_max=log_event_count_max)
        # pre-create shit for higher precision of actual add_event
        events = []
        for i in range(count):
            events.append(SchedulerEvent(i, UIEventType.UPDATE, dbuid))

        # now measure
        perf0 = time.perf_counter()
        for ev in events:
            log.add_event(ev, do_trim=True)
        perf1 = time.perf_counter()
        return perf1 - perf0

    def _test_benchmark(self, log_time_length_max, log_event_count_max):
        # the point of this test is to ensure asymptotic complexity is no worse than linear for the common case
        def _help_time(callable):
            delta = callable()
            count = 1
            while delta < 0.1:
                delta += callable()
                count += 1
            return delta / count

        delta1 = _help_time(lambda: self._run_iters(1000, log_time_length_max, log_event_count_max))
        delta2 = _help_time(lambda: self._run_iters(10000, log_time_length_max, log_event_count_max))
        delta3 = _help_time(lambda: self._run_iters(100000, log_time_length_max, log_event_count_max))
        delta4 = _help_time(lambda: self._run_iters(1000000, log_time_length_max, log_event_count_max))
        print(f'{delta1} : {delta2} : {delta3} : {delta4}')
        self.assertGreater(delta1*(10+2), delta2, )  # we give 20% margin
        self.assertGreater(delta1*(100+20), delta3)
        self.assertGreater(delta1*(1000+200), delta4)

    def test_benchmark1(self):
        self._test_benchmark(100, 9999999)

    def test_benchmark2(self):
        self._test_benchmark(100, 100)

    def test_benchmark3(self):
        self._test_benchmark(100, None)

    def test_auto_ids(self):
        log = SchedulerEventLog(log_time_length_max=None, log_event_count_max=100)
        for _ in range(100):
            log.add_event(SchedulerEvent(-1, UIEventType.UPDATE, dbuid))
        evs = log.get_since_timestamp(0)
        self.assertEqual(100, len(evs))
        for i, ev in enumerate(evs):
            self.assertEqual(i, ev.event_id)
        for _ in range(100):
            log.add_event(SchedulerEvent(-1, UIEventType.UPDATE, dbuid))
        evs = log.get_since_timestamp(0)
        self.assertEqual(100, len(evs))
        for i, ev in enumerate(evs):
            self.assertEqual(100+i, ev.event_id)

    def test_full_state_fetching(self):
        log = SchedulerEventLog(log_event_count_max=11)
        log.add_event(SchedulerEvent(0, UIEventType.UPDATE, dbuid))
        log.add_event(SchedulerEvent(1, UIEventType.UPDATE, dbuid))
        log.add_event(SchedulerEvent(2, UIEventType.UPDATE, dbuid))
        log.add_event(SchedulerEvent(3, UIEventType.FULL_STATE, dbuid))
        log.add_event(SchedulerEvent(4, UIEventType.UPDATE, dbuid))
        log.add_event(SchedulerEvent(5, UIEventType.UPDATE, dbuid))
        log.add_event(SchedulerEvent(6, UIEventType.FULL_STATE, dbuid))
        log.add_event(SchedulerEvent(7, UIEventType.UPDATE, dbuid))
        log.add_event(SchedulerEvent(8, UIEventType.UPDATE, dbuid))
        log.add_event(SchedulerEvent(9, UIEventType.DELETE, dbuid))
        log.add_event(SchedulerEvent(10, UIEventType.UPDATE, dbuid))

        self.assertEqual(1, log.get_since_event(0)[0].event_id)
        self.assertEqual(3, log.get_since_event(1, truncate_before_full_state=True)[0].event_id)
        self.assertEqual(3, log.get_since_event(2, truncate_before_full_state=True)[0].event_id)

        self.assertEqual(6, log.get_since_event(3, truncate_before_full_state=True)[0].event_id)
        self.assertEqual(6, log.get_since_event(4, truncate_before_full_state=True)[0].event_id)
        self.assertEqual(6, log.get_since_event(5, truncate_before_full_state=True)[0].event_id)
        self.assertEqual(7, log.get_since_event(6, truncate_before_full_state=True)[0].event_id)
        self.assertEqual(8, log.get_since_event(7, truncate_before_full_state=True)[0].event_id)
        self.assertEqual(9, log.get_since_event(8, truncate_before_full_state=True)[0].event_id)
        self.assertEqual(10, log.get_since_event(9, truncate_before_full_state=True)[0].event_id)

        log.add_event(SchedulerEvent(11, UIEventType.UPDATE, dbuid))
        log.add_event(SchedulerEvent(12, UIEventType.UPDATE, dbuid))
        log.add_event(SchedulerEvent(13, UIEventType.UPDATE, dbuid))

        self.assertEqual(3, log.get_since_event(1, truncate_before_full_state=True)[0].event_id)
        self.assertEqual(3, log.get_since_event(2, truncate_before_full_state=True)[0].event_id)
        self.assertEqual(6, log.get_since_event(3, truncate_before_full_state=True)[0].event_id)

        log.add_event(SchedulerEvent(14, UIEventType.UPDATE, dbuid))

        self.assertEqual(6, log.get_since_event(1, truncate_before_full_state=True)[0].event_id)
        self.assertEqual(6, log.get_since_event(2, truncate_before_full_state=True)[0].event_id)
        self.assertEqual(6, log.get_since_event(3, truncate_before_full_state=True)[0].event_id)
        self.assertEqual(6, log.get_since_event(4, truncate_before_full_state=True)[0].event_id)
        self.assertEqual(6, log.get_since_event(5, truncate_before_full_state=True)[0].event_id)
        self.assertEqual(7, log.get_since_event(6, truncate_before_full_state=True)[0].event_id)