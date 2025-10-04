import json

from enum import Enum
from types import MappingProxyType

from .network_item_watchers import NetworkItemWatcher, WatchableNetworkItem, WatchableNetworkItemProxy
from .scene_network_item import SceneNetworkItem, SceneNetworkItemWithUI
from .graphics_scene_base import GraphicsSceneBase

from lifeblood.node_ui import NodeUi
from lifeblood.ui_protocol_data import TaskData, TaskDelta, DataNotSet, IncompleteInvocationLogData, InvocationLogData
from lifeblood.basenode import BaseNode
from lifeblood.enums import TaskState
from lifeblood import logging
from lifeblood.environment_resolver import EnvironmentResolverArguments

from PySide6.QtWidgets import QGraphicsScene, QGraphicsItem

from typing import FrozenSet, Optional, List, Tuple, Dict, Set, Callable, Iterable, Union


logger = logging.get_logger('viewer')


class Node(SceneNetworkItemWithUI, WatchableNetworkItemProxy):
    class TaskSortOrder(Enum):
        ID = 0
        ID_REV = 1
        FRAMES = 2
        FRAMES_REV = 3
        NAME = 4
        NAME_REV = 5
        TOTAL_RUNTIME = 6
        TOTAL_RUNTIME_REV = 7

    # cache node type-2-inputs/outputs names, not to ask a million times for every node
    # actually this can be dynamic, and this cache is not used anyway, so TODO: get rid of it?
    _node_inputs_outputs_cached: Dict[str, Tuple[List[str], List[str]]] = {}

    # cache node shapes
    #_node_shapes

    class PseudoNode(BaseNode):
        def __init__(self, my_node: "Node"):
            super().__init__('_noname_')
            self.__my_node = my_node

        def _ui_changed(self, definition_changed=False):
            if definition_changed:
                self.__my_node.reanalyze_nodeui()

    def __init__(self, scene: GraphicsSceneBase, id: int, type: str, name: str):
        super().__init__(scene, id)

        self.__name = name
        self.__tasks: Set["Task"] = set()
        self.__tasks_sorted_cached: Optional[Dict[Node.TaskSortOrder, List["Task"]]] = None
        self.__node_type = type

        self.__nodeui: Optional[NodeUi] = None
        self.__connections: Set[NodeConnection] = set()

        self.__inputs, self.__outputs = None, None
        if self.__node_type in Node._node_inputs_outputs_cached:
            self.__inputs, self.__outputs = Node._node_inputs_outputs_cached[self.__node_type]

    def get_session_id(self):
        """
        session id is local id that should be preserved within a session even after undo/redo operations,
        unlike simple id, that will change on undo/redo
        """
        return self.graphics_scene().session_node_id_from_id(self.get_id())

    def node_type(self) -> str:
        return self.__node_type

    def node_name(self) -> str:
        return self.__name

    def set_name(self, new_name: str):
        if new_name == self.__name:
            return
        self.__name = new_name
        self.item_updated()

    def set_selected(self, selected: bool, *, unselect_others=False):
        scene: QGraphicsScene = self.graphics_scene()
        if unselect_others:
            scene.clearSelection()
        self.setSelected(selected)

    def update_nodeui(self, nodeui: NodeUi):
        self.__nodeui = nodeui
        self.__nodeui.set_ui_change_callback_receiver(Node.PseudoNode(self))
        self.reanalyze_nodeui()

    def reanalyze_nodeui(self):
        Node._node_inputs_outputs_cached[self.__node_type] = (list(self.__nodeui.inputs_names()), list(self.__nodeui.outputs_names()))
        self.__inputs, self.__outputs = Node._node_inputs_outputs_cached[self.__node_type]
        self.item_updated()

    def get_nodeui(self) -> Optional[NodeUi]:
        return self.__nodeui

    def all_connections(self) -> FrozenSet["NodeConnection"]:
        return frozenset(self.__connections)

    def input_connections(self, inname) -> Set["NodeConnection"]:
        if self.__inputs is not None and inname not in self.__inputs:
            raise RuntimeError(f'nodetype {self.__node_type} does not have input {inname}')
        return {x for x in self.__connections if x.input() == (self, inname)}

    def output_connections(self, outname) -> Set["NodeConnection"]:
        if self.__outputs is not None and outname not in self.__outputs:
            raise RuntimeError(f'nodetype {self.__node_type} does not have output {outname}')
        return {x for x in self.__connections if x.output() == (self, outname)}

    def input_names(self) -> Tuple[str, ...]:
        return tuple(self.__inputs) if self.__inputs else ()

    def output_names(self) -> Tuple[str, ...]:
        return tuple(self.__outputs) if self.__outputs else ()

    def input_nodes(self, inname: Optional[str] = None) -> Set["Node"]:
        """
        get input nodes as viewer (not scheduler) sees them

        :param inname: intput name to follow or None for all intputs
        """
        if inname is None:
            con_names = list(self.input_names())
        else:
            con_names = [inname]

        nodes = set()
        for con_name in con_names:
            nodes.update(con.output()[0] for con in self.input_connections(con_name))
        return nodes

    def output_nodes(self, outname: Optional[str] = None) -> Set["Node"]:
        """
        get output nodes as viewer (not scheduler) sees them

        :param outname: output name to follow or None for all outputs
        """
        if outname is None:
            con_names = list(self.output_names())
        else:
            con_names = [outname]

        nodes = set()
        for con_name in con_names:
            nodes.update(con.input()[0] for con in self.output_connections(con_name))
        return nodes

    def add_item_watcher(self, watcher: "NetworkItemWatcher"):
        super().add_item_watcher(watcher)
        if len(self.item_watchers()) == 1:  # first watcher
            for task in self.__tasks:
                task.add_item_watcher(self)

    def remove_item_watcher(self, watcher: "NetworkItemWatcher"):
        super().remove_item_watcher(watcher)
        if len(self.item_watchers()) == 0:  # removed last watcher
            for task in self.__tasks:
                task.remove_item_watcher(self)

    def add_task(self, task: "Task"):
        if task in self.__tasks:
            return
        logger.debug(f"adding task {task.get_id()} to node {self.get_id()}")

        if task.node() and task.node() != self:
            task.node().remove_task(task)
        task._set_parent_node(self)
        self.__tasks.add(task)

        # invalidate sorted cache
        self.__tasks_sorted_cached = None

        self.item_updated()

        if len(self.item_watchers()) > 0:
            task.add_item_watcher(self)

    def remove_tasks(self, tasks_to_remove: Iterable["Task"]):
        """
        this should cause much less animation overhead compared to
        if u would call remove-task for each task individually
        """
        logger.debug(f"removeing task {[x.get_id() for x in tasks_to_remove]} from node {self.get_id()}")
        tasks_to_remove = set(tasks_to_remove)
        for task in tasks_to_remove:
            task._set_parent_node(None)
            if len(self.item_watchers()) > 0:
                task.remove_item_watcher(self)
        for task in tasks_to_remove:
            self.__tasks.remove(task)

        # invalidate sorted cache
        self.__tasks_sorted_cached = None

        self.item_updated()

    def remove_task(self, task_to_remove: "Task"):
        logger.debug(f"removing task {task_to_remove.get_id()} from node {self.get_id()}")
        task_to_remove._set_parent_node(None)
        if len(self.item_watchers()) > 0:
            task_to_remove.remove_item_watcher(self)

        # invalidate sorted cache
        self.__tasks_sorted_cached = None

        self.__tasks.remove(task_to_remove)
        self.item_updated()

    def _sorted_tasks(self, order: TaskSortOrder) -> List["Task"]:
        if self.__tasks_sorted_cached is None:
            self.__tasks_sorted_cached = {}
        if order not in self.__tasks_sorted_cached:
            if order in (Node.TaskSortOrder.ID, Node.TaskSortOrder.ID_REV):
                tasks = sorted(self.__tasks, key=lambda x: x.get_id(), reverse=order == Node.TaskSortOrder.ID_REV)
            elif order in (Node.TaskSortOrder.FRAMES, Node.TaskSortOrder.FRAMES_REV):
                tasks = sorted(
                    self.__tasks,
                    key=lambda x: foo[0] if (foo := x.attributes().get('frames', ())) and isinstance(foo, list) and len(foo) and isinstance(foo[0], (int, float)) else 0,
                    reverse=order == Node.TaskSortOrder.FRAMES_REV,
                )
            elif order in (Node.TaskSortOrder.NAME, Node.TaskSortOrder.NAME_REV):
                tasks = sorted(self.__tasks, key=lambda x: x.name(), reverse=order == Node.TaskSortOrder.NAME_REV)
            elif order in (Node.TaskSortOrder.TOTAL_RUNTIME, Node.TaskSortOrder.TOTAL_RUNTIME_REV):
                tasks = sorted(self.__tasks, key=lambda x: x.invocations_total_time(only_last_per_node=True), reverse=order == Node.TaskSortOrder.TOTAL_RUNTIME_REV)
            else:
                raise NotImplementedError(f'sort order {order} is not implemented')
            self.__tasks_sorted_cached[order] = tasks
        return self.__tasks_sorted_cached[order]

    def tasks_iter(self, *, order: Optional[TaskSortOrder] = None) -> Iterable["Task"]:
        if order is None:
            return (x for x in self.__tasks)
        return self._sorted_tasks(order)

    def tasks(self) -> FrozenSet["Task"]:
        return frozenset(self.__tasks)

    def task_state_changed(self, task):
        """
        here node might decide to highlight the task that changed state one way or another
        """
        pass

    def task_name_changed(self, task):
        """
        called by child task when it's name were changed
        """
        if self.__tasks_sorted_cached:
            self.__tasks_sorted_cached.pop(Node.TaskSortOrder.NAME, None)
            self.__tasks_sorted_cached.pop(Node.TaskSortOrder.NAME_REV, None)

    def task_attributes_changed(self, task):
        """
        called by child task when it's attributes were changed
        """
        if self.__tasks_sorted_cached:
            self.__tasks_sorted_cached.pop(Node.TaskSortOrder.FRAMES, None)
            self.__tasks_sorted_cached.pop(Node.TaskSortOrder.FRAMES_REV, None)

    def task_logs_updated(self, task):
        """
        called by child task when it's logs are changed
        """
        if self.__tasks_sorted_cached:
            self.__tasks_sorted_cached.pop(Node.TaskSortOrder.TOTAL_RUNTIME, None)
            self.__tasks_sorted_cached.pop(Node.TaskSortOrder.TOTAL_RUNTIME_REV, None)

    def draw_imgui_elements(self, drawing_widget):
        pass  # base item doesn't draw anything

    def add_connection(self, new_connection: "NodeConnection"):
        self.__connections.add(new_connection)

        # if node ui has not yet been updated - we temporary add in/out names to lists
        # it will get overriden by nodeui update
        conno = new_connection.output()
        if conno[0] == self and (self.__outputs is None or conno[1] not in self.__outputs):
            if self.__outputs is None:
                self.__outputs = []
            self.__outputs.append(conno[1])
        conni = new_connection.input()
        if conni[0] == self and (self.__inputs is None or conni[1] not in self.__inputs):
            if self.__inputs is None:
                self.__inputs = []
            self.__inputs.append(conni[1])

    def remove_connection(self, connection: "NodeConnection"):
        self.__connections.remove(connection)

    def itemChange(self, change, value):
        if change == QGraphicsItem.GraphicsItemChange.ItemSceneChange:  # just before scene change
            conns = self.__connections.copy()
            if len(self.__tasks):
                logger.warning(f'node {self.get_id()}({self.node_name()}) has tasks at the moment of deletion, orphaning the tasks')
                self.remove_tasks(self.__tasks)
            for connection in conns:
                if self.scene() is not None and value != self.scene():
                    logger.debug('removing connections...')
                    assert connection.scene() is not None
                    connection.scene().removeItem(connection)
            assert len(self.__connections) == 0

        return super().itemChange(change, value)


class NodeConnection(SceneNetworkItem):
    def __init__(self, scene: GraphicsSceneBase, id: int, nodeout: Node, nodein: Node, outname: str, inname: str):
        super().__init__(scene, id)

        self.__nodeout = nodeout
        self.__nodein = nodein
        self.__outname = outname
        self.__inname = inname

        nodein.add_connection(self)
        nodeout.add_connection(self)

    def output(self) -> Tuple[Node, str]:
        return self.__nodeout, self.__outname

    def input(self) -> Tuple[Node, str]:
        return self.__nodein, self.__inname

    def set_output(self, node: Node, output_name: str = 'main'):
        logger.debug(f'reassigning NodeConnection output to {node.get_id()}, {output_name}')
        assert node is not None
        self.prepareGeometryChange()
        if node != self.__nodeout:
            self.__nodeout.remove_connection(self)
            self.__nodeout = node
            self.__outname = output_name
            self.__nodeout.add_connection(self)
        else:
            self.__outname = output_name

    def set_input(self, node: Node, input_name: str = 'main'):
        logger.debug(f'reassigning NodeConnection input to {node.get_id()}, {input_name}')
        assert node is not None
        self.prepareGeometryChange()
        if node != self.__nodein:
            self.__nodein.remove_connection(self)
            self.__nodein = node
            self.__inname = input_name
            self.__nodein.add_connection(self)
        else:
            self.__inname = input_name

    def itemChange(self, change: QGraphicsItem.GraphicsItemChange, value):
        if change == QGraphicsItem.GraphicsItemChange.ItemSceneChange:  # just before scene change
            if value == self.__nodein.scene():
                self.__nodein.add_connection(self)
            else:
                self.__nodein.remove_connection(self)
            if value == self.__nodeout.scene():
                self.__nodeout.add_connection(self)
            else:
                self.__nodeout.remove_connection(self)
        return super().itemChange(change, value)


class Task(SceneNetworkItemWithUI, WatchableNetworkItem):
    def __init__(self, scene: GraphicsSceneBase, task_data: TaskData):
        super().__init__(scene, task_data.id)

        # self.__state_details_raw = None
        self.__state_details_cached = None
        self.__raw_data: TaskData = task_data

        # self.__groups = set() if groups is None else set(groups)
        self.__log: Dict[int, Dict[int, Union[IncompleteInvocationLogData, InvocationLogData]]] = {}
        self.__inv_log: Optional[List[Tuple[int, int, Union[IncompleteInvocationLogData, InvocationLogData]]]] = None  # for presentation - inv_id -> (node_id, log)
        self.__inv_stat_total_time: Optional[Tuple[float, float]] = None
        self.__ui_attributes: dict = {}
        self.__ui_env_res_attributes: Optional[EnvironmentResolverArguments] = None

        self.__node: Optional[Node] = None

    def set_selected(self, selected: bool):
        scene: QGraphicsScene = self.scene()
        scene.clearSelection()
        if selected:
            self.setFlag(QGraphicsItem.GraphicsItemFlag.ItemIsSelectable, True)
        self.setSelected(selected)

    def parent_task_id(self) -> Optional[int]:
        return self.__raw_data.parent_id

    def children_tasks_count(self) -> int:
        return self.__raw_data.children_count

    def active_children_tasks_count(self) -> int:
        return self.__raw_data.active_children_count

    def split_level(self) -> int:
        return self.__raw_data.split_level

    def latest_invocation_attempt(self) -> int:
        return self.__raw_data.work_data_invocation_attempt

    def name(self) -> str:
        return self.__raw_data.name

    def set_name(self, name: str):
        if name == self.__raw_data.name:
            return
        self.__raw_data.name = name

        if self.__node:
            self.__node.task_name_changed(self)
        self.item_updated()

    def state(self) -> TaskState:
        return self.__raw_data.state

    def state_details(self) -> Optional[dict]:
        if self.__state_details_cached is None and self.__raw_data.state_details is not None:
            self.__state_details_cached = json.loads(self.__raw_data.state_details)
        return self.__state_details_cached

    def paused(self):
        return self.__raw_data.paused

    def groups(self) -> Set[str]:
        return self.__raw_data.groups

    def set_groups(self, groups: Set[str]):
        if self.__raw_data.groups == groups:
            return
        self.__raw_data.groups = groups
        self.item_updated()

    def attributes(self):
        return MappingProxyType(self.__ui_attributes)

    def in_group(self, group_name):
        return group_name in self.__raw_data.groups

    def node(self) -> Optional[Node]:
        return self.__node

    def set_state_details(self, state_details: Optional[str] = None):
        if self.__raw_data.state_details == state_details:
            return
        self.__raw_data.state_details = state_details
        self.__state_details_cached = None
        self.item_updated()

    def set_state(self, state: Optional[TaskState], paused: Optional[bool]):
        if (state is None or state == self.__raw_data.state) and (paused is None or self.__raw_data.paused == paused):
            return
        if state is not None:
            self.__raw_data.state = state
            self.set_state_details(None)
            if state != TaskState.IN_PROGRESS:
                self.__raw_data.progress = None
        if paused is not None:
            self.__raw_data.paused = paused
        if self.__node:
            self.__node.task_state_changed(self)
        self.item_updated()

    def set_task_data(self, raw_data: TaskData):
        self.__state_details_cached = None
        state_changed = self.__raw_data.state != raw_data.state
        self.__raw_data = raw_data
        if state_changed and self.__node:
            self.__node.task_state_changed(self)
            self.item_updated()

    def apply_task_delta(self, task_delta: TaskDelta, get_node: Callable[[int], Node]):
        if task_delta.paused is not DataNotSet:
            self.set_state(None, task_delta.paused)
        if task_delta.state is not DataNotSet:
            self.set_state(task_delta.state, None)
        if task_delta.name is not DataNotSet:
            self.set_name(task_delta.name)
        if task_delta.node_id is not DataNotSet:
            node: Optional[Node] = get_node(task_delta.node_id)
            if node is not None:
                node.add_task(self)
        if task_delta.work_data_invocation_attempt is not DataNotSet:
            self.__raw_data.work_data_invocation_attempt = task_delta.work_data_invocation_attempt
        if task_delta.node_output_name is not DataNotSet:
            self.__raw_data.node_output_name = task_delta.node_output_name
        if task_delta.node_input_name is not DataNotSet:
            self.__raw_data.node_input_name = task_delta.node_input_name
        if task_delta.invocation_id is not DataNotSet:
            self.__raw_data.invocation_id = task_delta.invocation_id
        if task_delta.split_id is not DataNotSet:
            self.__raw_data.split_id = task_delta.split_id
        if task_delta.children_count is not DataNotSet:
            self.__raw_data.children_count = task_delta.children_count
        if task_delta.active_children_count is not DataNotSet:
            self.__raw_data.active_children_count = task_delta.active_children_count
        if task_delta.groups is not DataNotSet:
            self.set_groups(task_delta.groups)
        if task_delta.split_origin_task_id is not DataNotSet:
            self.__raw_data.split_origin_task_id = task_delta.split_origin_task_id
        if task_delta.split_level is not DataNotSet:
            self.__raw_data.split_level = task_delta.split_level
        if task_delta.progress is not DataNotSet:
            self.__raw_data.progress = task_delta.progress
        if task_delta.parent_id is not DataNotSet:
            self.__raw_data.parent_id = task_delta.parent_id
        if task_delta.state_details is not DataNotSet:
            self.set_state_details(task_delta.state_details)
        self.item_updated()

    def set_progress(self, progress: float):
        self.__raw_data.progress = progress
        # logger.debug('progress %d', progress)
        self.item_updated()

    def get_progress(self) -> Optional[float]:
        return self.__raw_data.progress if self.__raw_data else None

    def item_updated(self):
        super().item_updated()
        for watcher in self.item_watchers():
            watcher.item_was_updated(self)

    def __reset_cached_invocation_data(self):
        self.__inv_log = None
        self.__inv_stat_total_time = None

    def update_log(self, alllog: Dict[int, Dict[int, Union[IncompleteInvocationLogData, InvocationLogData]]], full_update: bool):
        """
        This function gets called by scene with new shit from worker. Maybe there's more sense to make it "_protected"
        :param alllog: is expected to be a dict of node_id -> (dict of invocation_id -> (invocation dict) )
        :param full_update: is true, if log dict covers all invocations.
            otherwise update is considered partial, so only updated information counts, no removes are to be done
        :return:
        """
        logger.debug('log updated (full=%s) with %d entries', full_update, sum(len(x.values()) for _, x in alllog.items()))
        # Note that we assume log deletion is not possible
        updated_invocations = set()
        for node_id, invocs in alllog.items():
            updated_invocations.update(invocs.keys())
            if node_id not in self.__log:
                self.__log[node_id] = invocs
                continue
            for inv_id, logs in invocs.items():
                if inv_id in self.__log[node_id]:
                    assert logs is not None
                    if isinstance(logs, IncompleteInvocationLogData):
                        self.__log[node_id][inv_id].copy_from(logs)
                        continue
                self.__log[node_id][inv_id] = logs
        # if it's full update - we clear invocations that are present in task, but not in updated info
        if full_update:
            for node_id, invocs in self.__log.items():
                for inv_id in list(invocs.keys()):
                    if inv_id not in updated_invocations:
                        logger.debug('removing %d invocation from task %d', inv_id, self.get_id())
                        invocs.pop(inv_id)

        # clear cached inverted dict, it will be rebuilt on next access
        self.__reset_cached_invocation_data()

        if self.__node:
            self.__node.task_logs_updated(self)
        self.item_updated()

    def remove_invocations_log(self, invocation_ids: List[int]):
        logger.debug('removing invocations for %s', invocation_ids)
        for _, invocs in self.__log:
            for invocation_id in invocation_ids:
                if invocation_id in invocs:
                    invocs.pop(invocation_id)

        # clear cached inverted dict, it will be rebuilt on next access
        self.__reset_cached_invocation_data()

        if self.__node:
            self.__node.task_logs_updated(self)
        self.item_updated()

    def invocations_total_time(self, only_last_per_node: bool = True) -> float:
        """
        calculate and get statistics on all invocations belonging to this task
        :return: if only_last_per_node - returns runtime sum of only last invocations per node
                 otherwise returns sum of ALL invocation times, including errors and re-runs.
        """
        if self.__inv_stat_total_time is None:
            total_time = 0.0
            max_inv_logs = {}
            for inv_id, node_id, log in self.invocation_logs():
                if log.invocation_runtime is None:
                    continue
                if node_id not in max_inv_logs or max_inv_logs[node_id][0] < inv_id: # looking for invocation with biggest inv_id, as it will be the latest
                    max_inv_logs[node_id] = (inv_id, log)
                total_time += log.invocation_runtime

            total_latest_time = sum(x.invocation_runtime for _, x in max_inv_logs.values())
            self.__inv_stat_total_time = (total_latest_time, total_time)

        if only_last_per_node:
            return self.__inv_stat_total_time[0]
        else:
            return self.__inv_stat_total_time[1]

    def invocation_logs_mapping(self) -> MappingProxyType[int, Dict[int, Union[IncompleteInvocationLogData, InvocationLogData]]]:
        return MappingProxyType(self.__log)

    def invocation_logs(self) -> List[Tuple[int, int, Union[IncompleteInvocationLogData, InvocationLogData]]]:
        """
        return tuples of (invocation_id, node_id, Log Data)
        Entries will be grouped by node_id
        """
        if self.__inv_log is None:
            self.__inv_log = []
            for node_id, logdict in self.__log.items():
                for inv_id, log in logdict.items():
                    self.__inv_log.append((inv_id, node_id, log))
        return self.__inv_log

    def update_attributes(self, attributes: dict):
        logger.debug('attrs updated with %s', attributes)
        self.__ui_attributes = attributes

        if self.__node:
            self.__node.task_attributes_changed(self)
        self.item_updated()

    def set_environment_attributes(self, env_attrs: Optional[EnvironmentResolverArguments]):
        self.__ui_env_res_attributes = env_attrs
        self.item_updated()

    def environment_attributes(self) -> Optional[EnvironmentResolverArguments]:
        return self.__ui_env_res_attributes

    def _set_parent_node(self, node: Optional[Node]):
        """
        only to be called by Node class
        """
        self.__node = node

    def setParentItem(self, item):
        """
        use node.add_task if you want to set node for this task
        :param item:
        :return:
        """
        super().setParentItem(item)
