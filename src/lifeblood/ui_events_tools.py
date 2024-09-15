from .ui_events import TaskFullState, TasksChanged, TasksRemoved, TasksUpdated, TaskEvent
from .ui_protocol_data import TaskBatchData, DataNotSet
from .logging import get_logger

from typing import List, Optional


def collapse_task_event_list(event_list: List[TaskEvent]) -> Optional[TaskBatchData]:
    if len(event_list) == 0:
        return None
    collapsed_data = TaskBatchData
    db_id = None
    event_id = None
    timestamp = None
    for event in event_list:
        if db_id is None:
            db_id = event.database_uid
            event_id = event.event_id
            timestamp = event.timestamp
        elif db_id != event.database_uid:
            raise RuntimeError('provided event list has events from different databases')
        event_id = max(event_id, event.event_id)
        timestamp = max(timestamp, event.timestamp)

        if isinstance(event, TaskFullState):
            collapsed_data = event.task_data
        elif isinstance(event, TasksRemoved):
            for task_id in event.task_ids:
                if task_id not in collapsed_data.tasks:
                    get_logger('lifeblood.utility').warning(f'event list inconsistency: task id {task_id} is not in tasks, cannot remove')
                    continue
                collapsed_data.tasks.pop(task_id)
        elif isinstance(event, TasksUpdated):
            for task_id, task_data in event.task_data.tasks.items():
                collapsed_data.tasks[task_id] = task_data
        elif isinstance(event, TasksChanged):
            for task_delta in event.task_deltas:
                task_id = task_delta.id
                if task_id not in collapsed_data.tasks:
                    get_logger('lifeblood.utility').warning(f'event list inconsistency: task id {task_id} is not in tasks, cannot apply delta')
                    continue
                for field in ('parent_id', 'children_count', 'active_children_count', 'state', 'state_details', 'paused', 'node_id', 'node_input_name',
                              'node_output_name', 'name', 'split_level', 'work_data_invocation_attempt', 'progress', 'split_origin_task_id', 'split_id',
                              'invocation_id', 'groups'):
                    if (val := getattr(task_delta, field)) is not DataNotSet:
                        setattr(collapsed_data.tasks[task_id], field, val)
        else:
            raise NotImplementedError(f'handling of event type "{type(event)}" is not implemented')

    return collapsed_data
