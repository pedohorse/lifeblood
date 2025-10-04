from .undo_stack import UndoableOperation, OperationCompletionDetails
from .long_op import LongOperation, LongOperationData
from .ui_snippets import NodeSnippetData
from lifeblood.node_parameters import Parameter
from lifeblood.ui_protocol_data import InvocationLogData
from lifeblood.node_type_metadata import NodeTypeMetadata
from lifeblood.enums import TaskState, TaskGroupArchivedState
from lifeblood.taskspawn import NewTask
from PySide6.QtCore import QPointF

from types import MappingProxyType
from typing import Any, Callable, Iterable, List, Optional, Set, Union


class SceneDataController:
    def request_log(self, invocation_id: int, operation_data: Optional[LongOperationData] = None):
        raise NotImplementedError()

    def request_log_meta(self, task_id: int, operation_data: Optional[LongOperationData] = None):
        raise NotImplementedError()

    def request_attributes(self, task_id: int, operation_data: Optional[LongOperationData] = None):
        raise NotImplementedError()

    def request_invocation_job(self, task_id: int):
        raise NotImplementedError()

    def request_node_ui(self, node_id: int):
        raise NotImplementedError()

    def query_node_has_parameter(self, node_id: int, param_name: str, operation_data: Optional[LongOperationData] = None):
        raise NotImplementedError()

    def request_node_parameter_change(self, node_id: int, param: Parameter, operation_data: Optional[LongOperationData] = None):
        raise NotImplementedError()

    def request_node_parameter_expression_change(self, node_id: int, param: Parameter, operation_data: Optional[LongOperationData] = None):
        raise NotImplementedError()

    def request_node_parameters_change(self, node_id: int, params: Iterable[Parameter], operation_data: Optional[LongOperationData] = None):
        raise NotImplementedError()

    def request_apply_node_settings(self, node_id: int, settings_name: str, operation_data: Optional[LongOperationData] = None):
        raise NotImplementedError()

    def request_save_custom_settings(self, node_type_name: str, settings_name: str, settings: dict, operation_data: Optional[LongOperationData] = None):
        raise NotImplementedError()

    def request_set_settings_default(self, node_type_name: str, settings_name: Optional[str], operation_data: Optional[LongOperationData] = None):
        raise NotImplementedError()

    def request_node_types_update(self):
        raise NotImplementedError()

    def request_node_presets_update(self):
        raise NotImplementedError()

    def request_node_preset(self, packagename: str, presetname: str, operation_data: Optional[LongOperationData] = None):
        raise NotImplementedError()

    def request_set_node_name(self, node_id: int, name: str, operation_data: Optional[LongOperationData] = None):
        raise NotImplementedError()

    def request_node_connection_change(self, connection_id: int, outnode_id: Optional[int] = None, outname: Optional[str] = None, innode_id: Optional[int] = None, inname: Optional[str] = None):
        raise NotImplementedError()

    def request_node_connection_remove(self, connection_id: int, operation_data: Optional[LongOperationData] = None):
        raise NotImplementedError()

    def request_node_connection_add(self, outnode_id: int, outname: str, innode_id: int, inname: str, operation_data: Optional[LongOperationData] = None):
        raise NotImplementedError()

    def request_create_node(self, typename: str, nodename: str, pos: QPointF, operation_data: Optional[LongOperationData] = None):
        raise NotImplementedError()

    def request_create_nodes_from_snippet(self, snippet: NodeSnippetData, pos: QPointF, containing_long_op: Optional[LongOperation] = None):
        raise NotImplementedError()

    def request_remove_node(self, node_id: int, operation_data: Optional[LongOperationData] = None):
        raise NotImplementedError()

    def request_remove_nodes(self, node_ids: List[int], operation_data: Optional[LongOperationData] = None):
        raise NotImplementedError()

    def request_wipe_node(self, node_id: int):
        raise NotImplementedError()

    def request_duplicate_nodes(self, node_ids: List[int], shift: QPointF):
        raise NotImplementedError()

    def set_task_group_filter(self, groups):
        raise NotImplementedError()

    def set_task_state(self, task_ids: List[int], state: TaskState):
        raise NotImplementedError()

    def set_tasks_paused(self, task_ids_or_groups: List[Union[int, str]], paused: bool):
        raise NotImplementedError()

    def set_task_group_archived_state(self, group_names: List[str], state: TaskGroupArchivedState):
        raise NotImplementedError()

    def delete_task_groups(self, group_names: List[str]):
        raise NotImplementedError()

    def request_add_task_group(self, task_group_name: str, creator: str, *, allow_name_change_to_make_unique: bool = False, priority: float = 50.0, user_data: Optional[bytes] = None, operation_data: Optional[LongOperationData] = None) -> str:
        raise NotImplementedError()

    def request_task_group_user_data(self, group_name: str, operation_data: Optional[LongOperationData] = None):
        raise NotImplementedError()

    def request_task_cancel(self, task_id: int):
        raise NotImplementedError()

    def request_set_task_node(self, task_id: int, node_id: int):
        raise NotImplementedError()

    def request_add_task(self, new_task: NewTask):
        raise NotImplementedError()

    def request_rename_task(self, task_id: int, new_name: str):
        raise NotImplementedError()

    def request_set_task_groups(self, task_id: int, new_groups: Set[str]):
        raise NotImplementedError()

    def request_update_task_attributes(self, task_id: int, attribs_to_update: dict, attribs_to_delete: Set[str]):
        raise NotImplementedError()

    def set_skip_dead(self, do_skip: bool) -> None:
        raise NotImplementedError()

    def set_skip_archived_groups(self, do_skip: bool) -> None:
        raise NotImplementedError()

    def request_set_environment_resolver_arguments(self, task_id, env_args):
        raise NotImplementedError()

    def request_unset_environment_resolver_arguments(self, task_id):
        raise NotImplementedError()

    #

    def request_graph_and_tasks_update(self):
        """
        send a request to the scheduler to update node graph and tasks state immediately
        """
        raise NotImplementedError()

    def request_task_groups_update(self):
        """
        send a request to the scheduler to update task groups state immediately
        """
        raise NotImplementedError()

    def request_workers_update(self):
        """
        send a request to the scheduler to update workers state immediately
        """
        raise NotImplementedError()

    def add_connection(self, outnode_id: int, outname: str, innode_id: int, inname: str, *, callback: Optional[Callable[[UndoableOperation, OperationCompletionDetails], None]] = None):
        raise NotImplementedError()

    def cut_connection(self, outnode_id: int, outname: str, innode_id: int, inname: str, *, callback: Optional[Callable[[UndoableOperation, OperationCompletionDetails], None]] = None):
        raise NotImplementedError()

    def cut_connection_by_id(self, con_id, *, callback: Optional[Callable[[UndoableOperation, OperationCompletionDetails], None]] = None):
        raise NotImplementedError()

    def change_connection_by_id(self, con_id, *,
                                to_outnode_id: Optional[int] = None, to_outname: Optional[str] = None,
                                to_innode_id: Optional[int] = None, to_inname: Optional[str] = None,
                                callback: Optional[Callable[[UndoableOperation, OperationCompletionDetails], None]] = None):
        raise NotImplementedError()

    def change_node_parameter(self, node_id: int, item: Parameter, value: Any = ..., expression=...,
                              *, callback: Optional[Callable[[UndoableOperation, OperationCompletionDetails], None]] = None):
        """

        :param node_id:
        :param item:
        :param value: ... means no change
        :param expression: ... means no change
        :param callback: optional callback to call on successful completion of async operation
        :return:
        """
        raise NotImplementedError()

    def fetch_log_run_callback(self, invocation_id, callback: Callable[[InvocationLogData, Any], None], callback_data: Any = None):
        """
        fetch log for given invocation and run callback

        callback is run only in case of success
        """
        raise NotImplementedError()

    def node_types(self) -> MappingProxyType[str, NodeTypeMetadata]:
        raise NotImplementedError()
