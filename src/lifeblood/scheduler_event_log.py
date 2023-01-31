from dataclasses import dataclass, field
import logging
import time
from .ui_protocol_data import TaskData
from .enums import UIEventType

from typing import Dict, List


logger = logging.getLogger(__name__)

@dataclass
class SchedulerEvent:
    timestamp: float = field(default_factory=lambda: time.time(), init=False)
    event_id: int
    event_type: UIEventType


@dataclass
class TaskEvent(SchedulerEvent):
    task_data: TaskData


class SchedulerEventLog:
    def __init__(self, log_time_length_max, log_event_count_max):
        self.__max_time = log_time_length_max
        self.__max_count = log_event_count_max

        self.__events: List[SchedulerEvent] = []  # will be kept sorted
        self.__event_id_to_event: Dict[int, SchedulerEvent] = {}

    def __find_index_below_timestamp(self, timestamp):
        if len(self.__events) < 8:
            for i, event in enumerate(self.__events):
                if event.timestamp > timestamp:
                    return i - 1
            return len(self.__events) - 1

        def _find_bin(l: List[SchedulerEvent], timestamp):
            if len(l) == 1:
                return -1 if timestamp < l[0].timestamp else 0
            si = len(l) // 2
            if (i := _find_bin(l[si:], timestamp)) > -1:
                return i + si
            return _find_bin(l[:si], timestamp)

        return _find_bin(self.__events, timestamp)

    def append(self, event: SchedulerEvent, do_trim=True):
        if event.event_id in self.__event_id_to_event:
            if event != self.__event_id_to_event[event.event_id]:
                raise RuntimeError('attempting to add event with the same id, but different data')
            logger.warning(f'ignoring event as it is already in the log: {event}')
            return
        i = self.__find_index_below_timestamp(event.timestamp) + 1
        if i > 0 and self.__events[i-1].event_id >= event.event_id \
                or i < len(self.__events) - 1 and self.__events[i+1].event_id <= event.event_id:  # sanity checks
            raise RuntimeError('event IDs are expected to be sorted the same way as their timestamps')
        self.__events.insert(i, event)
        self.__event_id_to_event[event.event_id] = event
        if do_trim:
            self.trim()

    def trim(self):
        if len(self.__events) == 0:
            return
        threshold = self.__events[-1].timestamp - self.__max_time
        trimpos = self.__find_index_below_timestamp(threshold)+1
        for event in self.__events[:trimpos]:
            self.__event_id_to_event.pop(event.event_id)
        self.__events = self.__events[trimpos:]

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
