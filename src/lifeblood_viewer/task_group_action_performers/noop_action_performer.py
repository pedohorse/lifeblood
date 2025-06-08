from lifeblood_viewer.task_group_actions import TaskGroupViewerActionPerformerBase, TaskGroupViewerAction


class NoopViewerActionPerformer(TaskGroupViewerActionPerformerBase):
    def is_action_supported(self, action: TaskGroupViewerAction) -> bool:
        return action.type == 'noop'

    def perform_action(self, action: TaskGroupViewerAction):
        return
