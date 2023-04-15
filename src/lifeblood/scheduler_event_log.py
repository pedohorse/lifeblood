import logging
from .ui_events import SchedulerEvent
from .enums import UIEventType

from typing import Dict, List, Optional, Set, Tuple


logger = logging.getLogger(__name__)


class SchedulerEventLog:
    def __init__(self, *, log_time_length_max: Optional[float] = None, log_event_count_max: Optional[int] = None):
        self.__max_time_ns = int(log_time_length_max * 10**9) if log_time_length_max is not None else None
        self.__max_count = log_event_count_max
        if log_event_count_max is None and log_time_length_max is None:
            raise ValueError('at least one of log_time_length_max or log_event_count_max must be not none')

        self.__events: List[SchedulerEvent] = []  # will be kept sorted
        self.__event_id_to_event: Dict[int, SchedulerEvent] = {}
        self.__full_state_events: List[SchedulerEvent] = []

        self.__internal_event_counter_next = 0

    def __find_index_below_timestamp(self, timestamp):
        return self.__find_index_below(self.__events, 'timestamp', timestamp)

    def __find_index_below_id(self, event_id):
        return self.__find_index_below(self.__events, 'event_id', event_id)

    def __find_full_state_index_below_id(self, event_id):
        return self.__find_index_below(self.__full_state_events, 'event_id', event_id)

    @classmethod
    def __find_index_below(cls, event_list, property_name, value):
        if len(event_list) < 8:
            for i, event in enumerate(event_list):
                if getattr(event, property_name) > value:
                    return i - 1
            return len(event_list) - 1

        def _find_bin(l: List[SchedulerEvent], value):
            if len(l) == 1:
                return -1 if value < getattr(l[0], property_name) else 0
            if value < getattr(l[0], property_name):
                return -1
            elif value > getattr(l[-1], property_name):
                return len(l)-1
            si = len(l) // 2
            if (i := _find_bin(l[si:], value)) > -1:
                return i + si
            return _find_bin(l[:si], value)

        return _find_bin(event_list, value)

    def add_event(self, event: SchedulerEvent, do_trim=True):
        # first check the trivial case
        if event.event_id < 0:
            event.event_id = self.__internal_event_counter_next
        if len(self.__events) == 0 or event.event_id > self.__events[-1].event_id and event.timestamp >= self.__events[-1].timestamp:
            i = len(self.__events)
        else:
            if event.event_id in self.__event_id_to_event:
                if event != self.__event_id_to_event[event.event_id]:
                    raise RuntimeError('attempting to add event with the same id, but different data')
                logger.warning(f'ignoring event as it is already in the log: {event}')
                return
            i = self.__find_index_below_id(event.event_id) + 1
            if i > 0 and (self.__events[i-1].event_id >= event.event_id or self.__events[i-1].timestamp > event.timestamp) \
                    or i < len(self.__events) and (self.__events[i].event_id <= event.event_id or self.__events[i].timestamp < event.timestamp):  # sanity checks
                raise RuntimeError(f'event IDs are expected to be sorted the same way as their timestamps: [{f"{self.__events[i-1].event_id}:{self.__events[i-1].timestamp}" if i>0 else "|"}, <{event.event_id}:{event.timestamp}>, {f"{self.__events[i].event_id}:{self.__events[i].timestamp}" if i<len(self.__events) else "|"}]')
        self.__events.insert(i, event)
        self.__event_id_to_event[event.event_id] = event
        if event.event_type == UIEventType.FULL_STATE:
            if len(self.__full_state_events) == 0 or i == len(self.__events) - 1:
                self.__full_state_events.append(event)
            else:
                self.__full_state_events.insert(self.__find_full_state_index_below_id(event.event_id) + 1, event)

        self.__internal_event_counter_next = max(self.__internal_event_counter_next, event.event_id) + 1
        if do_trim:
            self.trim()

    def trim(self):
        if len(self.__events) == 0:
            return
        if self.__max_time_ns is not None:
            threshold = self.__events[-1].timestamp - self.__max_time_ns
            if self.__events[0].timestamp <= threshold:
                trimpos = self.__find_index_below_timestamp(threshold)+1
                for event in self.__events[:trimpos]:
                    deleted_event = self.__event_id_to_event.pop(event.event_id)
                    if deleted_event.event_type == UIEventType.FULL_STATE:
                        # since we delete always last - we are guaranteed to have full events in the same order
                        assert deleted_event.event_id == self.__full_state_events.pop(0).event_id
                self.__events = self.__events[trimpos:]

        if self.__max_count is not None and len(self.__events) > self.__max_count:
            for event in self.__events[:-self.__max_count]:
                deleted_event = self.__event_id_to_event.pop(event.event_id)
                if deleted_event.event_type == UIEventType.FULL_STATE:
                    # since we delete always last - we are guaranteed to have full events in the same order
                    assert deleted_event.event_id == self.__full_state_events.pop(0).event_id
            self.__events = self.__events[-self.__max_count:]

    def get_since_timestamp(self, timestamp: float):
        """
        get events since given timestamp
        :param timestamp: timestamp is given as EPOCH time in floating seconds
        :return:
        """
        timestamp_ns = int(timestamp) * 10**9 + int(timestamp % 1.0 * 10**9)
        return self.get_since_timestamp_ns(timestamp_ns)

    def get_since_timestamp_ns(self, timestamp_ns: int):
        return tuple(self.__events[self.__find_index_below_timestamp(timestamp_ns)+1:])

    def get_since_event(self, event_id: int, truncate_before_full_state=False) -> Tuple[SchedulerEvent, ...]:

        # if event was already removed from the list (assume it was old)
        if truncate_before_full_state and len(self.__full_state_events) > 0:
            possible_full_state_i = self.__find_full_state_index_below_id(event_id) + 1
            if possible_full_state_i < len(self.__full_state_events):
                event_id = self.__full_state_events[possible_full_state_i].event_id - 1
        e_index = self.__find_index_below_id(event_id)
        if 0 <= e_index < len(self.__events):
            assert self.__events[e_index].event_id <= event_id  # in case event_id does not exist  - found event_id will be smaller
        # we will return all events since e_index, not since it's timestamp cuz we relay in incrementing event_ids
        # and want to avoid extra search by timestamp, since the result should be the exact same
        return tuple(self.__events[e_index+1:])

    def __len__(self):
        return len(self.__events)
