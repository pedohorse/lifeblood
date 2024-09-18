import copy

from .ui_events import TaskFullState, TasksChanged, TasksRemoved, TasksUpdated, TaskEvent
from .ui_protocol_data import TaskBatchData, DataNotSet, TaskData, TaskDelta

from typing import Dict, List, Optional


def collapse_task_event_list(event_list: List[TaskEvent]) -> Optional[TaskBatchData]:
    if len(event_list) == 0:
        return None
    collapsed_tasks: Dict[int, TaskData] = {}
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
            collapsed_tasks = {k: copy.copy(v) for k, v in event.task_data.tasks.items()}
        elif isinstance(event, TasksRemoved):
            for task_id in event.task_ids:
                if task_id not in collapsed_tasks:
                    raise RuntimeError(f'event list inconsistency: task id {task_id} is not in tasks, cannot remove')
                collapsed_tasks.pop(task_id)
        elif isinstance(event, TasksUpdated):
            for task_id, task_data in event.task_data.tasks.items():
                collapsed_tasks[task_id] = copy.copy(task_data)
        elif isinstance(event, TasksChanged):
            for task_delta in event.task_deltas:
                task_id = task_delta.id
                if task_id not in collapsed_tasks:
                    print(collapsed_tasks)
                    raise RuntimeError(f'event list inconsistency: task id {task_id} is not in tasks, cannot apply delta')
                for field in TaskDelta.__annotations__.keys():
                    if (val := getattr(task_delta, field)) is not DataNotSet:
                        if field == 'id':
                            assert collapsed_tasks[task_id].id == val
                        setattr(collapsed_tasks[task_id], field, val)
        else:
            raise NotImplementedError(f'handling of event type "{type(event)}" is not implemented')

    return TaskBatchData(
        db_id,
        collapsed_tasks
    )

