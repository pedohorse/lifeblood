import logging
from .ui_events import SchedulerEvent

from typing import Dict, List, Optional


logger = logging.getLogger(__name__)


class SchedulerEventLog:
    def __init__(self, *, log_time_length_max: Optional[float], log_event_count_max: Optional[int]):
        self.__max_time = log_time_length_max
        self.__max_count = log_event_count_max
        if log_event_count_max is None and log_time_length_max is None:
            raise ValueError('at least one of log_time_length_max or log_event_count_max must be not none')

        self.__events: List[SchedulerEvent] = []  # will be kept sorted
        self.__event_id_to_event: Dict[int, SchedulerEvent] = {}

        self.__internal_event_counter_next = 0

    def __find_index_below_timestamp(self, timestamp):
        if len(self.__events) < 8:
            for i, event in enumerate(self.__events):
                if event.timestamp > timestamp:
                    return i - 1
            return len(self.__events) - 1

        def _find_bin(l: List[SchedulerEvent], timestamp):
            if len(l) == 1:
                return -1 if timestamp < l[0].timestamp else 0
            if timestamp < l[0].timestamp:
                return -1
            elif timestamp > l[-1].timestamp:
                return len(l)-1
            si = len(l) // 2
            if (i := _find_bin(l[si:], timestamp)) > -1:
                return i + si
            return _find_bin(l[:si], timestamp)

        return _find_bin(self.__events, timestamp)

    def add_event(self, event: SchedulerEvent, do_trim=True):
        # first check the trivial case
        if len(self.__events) == 0 or event.event_id > self.__events[-1].event_id and event.timestamp > self.__events[-1].timestamp:
            i = len(self.__events)
        else:
            if event.event_id in self.__event_id_to_event:
                if event != self.__event_id_to_event[event.event_id]:
                    raise RuntimeError('attempting to add event with the same id, but different data')
                logger.warning(f'ignoring event as it is already in the log: {event}')
                return
            i = self.__find_index_below_timestamp(event.timestamp) + 1
            if i > 0 and self.__events[i-1].event_id >= event.event_id \
                    or i < len(self.__events) - 1 and self.__events[i+1].event_id <= event.event_id:  # sanity checks
                raise RuntimeError('event IDs are expected to be sorted the same way as their timestamps')
        if event.event_id < 0:
            event.event_id = self.__internal_event_counter_next
        self.__events.insert(i, event)
        self.__event_id_to_event[event.event_id] = event
        self.__internal_event_counter_next = max(self.__internal_event_counter_next, event.event_id) + 1
        if do_trim:
            self.trim()

    def trim(self):
        if len(self.__events) == 0:
            return
        if self.__max_time is not None:
            threshold = self.__events[-1].timestamp - self.__max_time
            if (self.__max_count is None or len(self.__events) < self.__max_count) and self.__events[0].timestamp > threshold:
                return
            trimpos = self.__find_index_below_timestamp(threshold)+1
            for event in self.__events[:trimpos]:
                self.__event_id_to_event.pop(event.event_id)
            self.__events = self.__events[trimpos:]

        if self.__max_count is not None:
            for event in self.__events[:-self.__max_count]:
                self.__event_id_to_event.pop(event.event_id)
            self.__events = self.__events[-self.__max_count:]

    def get_since_timestamp(self, timestamp):
        return tuple(self.__events[self.__find_index_below_timestamp(timestamp)+1:])

    def get_since_event(self, event_id: int):

        # elaborate way of just getting the id of the event
        e_index = self.__find_index_below_timestamp(self.__event_id_to_event[event_id].timestamp)
        assert self.__events[e_index].event_id == event_id
        # we will return all events since e_index, not since it's timestamp cuz we relay in incrementing event_ids
        # and want to avoid extra search by timestamp, since the result should be the exact same
        return tuple(self.__events[e_index+1:])

    def __len__(self):
        return len(self.__events)
