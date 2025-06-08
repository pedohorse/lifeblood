from lifeblood_viewer.task_group_actions import TaskGroupViewerAction
from dataclasses import dataclass


@dataclass
class TaskGroupViewerNoopAction(TaskGroupViewerAction):
    @classmethod
    def from_data_dict(cls, action_data: dict, *, original_user_data: bytes) -> "TaskGroupViewerAction":
        return TaskGroupViewerAction('noop')
