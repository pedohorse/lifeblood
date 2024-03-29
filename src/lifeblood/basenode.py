import asyncio
import re
from copy import deepcopy
from typing import Dict, Optional, Any
from .nodethings import ProcessingResult
from .uidata import NodeUi, ParameterNotFound, Parameter
from .processingcontext import ProcessingContext
from .logging import get_logger
from .enums import NodeParameterType, WorkerType
from .plugin_info import PluginInfo, empty_plugin_info
from .nodegraph_holder_base import NodeGraphHolderBase

# reexport
from .nodethings import ProcessingError

from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:
    from logging import Logger


class BaseNode:
    _plugin_data = None  # To be set on module level by loader, set to empty_plugin_info by default

    @classmethod
    def label(cls) -> str:
        raise NotImplementedError()

    @classmethod
    def tags(cls) -> Iterable[str]:
        raise NotImplementedError()

    @classmethod
    def type_name(cls) -> str:
        raise NotImplementedError()

    @classmethod
    def description(cls) -> str:
        return 'this node type does not have a description'

    def __init__(self, name: str):
        if BaseNode._plugin_data is None:
            BaseNode._plugin_data = empty_plugin_info
        self.__parent: NodeGraphHolderBase = None
        self.__parent_nid: int = None
        self._parameters: NodeUi = NodeUi(self)
        self.__name = name
        try:
            mytype = self.type_name()
        except NotImplementedError:
            mytype = None
        self.__logger = get_logger(f'BaseNode.{mytype}' if mytype is not None else 'BaseNode')
        # subclass is expected to add parameters at this point

    def set_parent(self, graph_holder: NodeGraphHolderBase, node_id_in_graph: int):
        self.__parent = graph_holder
        self.__parent_nid = node_id_in_graph

    def logger(self) -> "Logger":
        return self.__logger

    def name(self):
        return self.__name

    def set_name(self, name: str):
        self.__name = name

    def id(self):
        return self.__parent_nid

    # # MAYBE UNCOMMENT THESE WHEN ALL NODES ARE REFACTORED TO USE CONTEXT?
    # def param_value(self, param_name, context: Optional[ProcessingContext] = None) -> Any:
    #     """
    #     shortcut to node.get_ui().parameter_value
    #     :param param_name:
    #     :param context: context in which to evaluate expression
    #     :return:
    #     """
    #     return self._parameters.parameter(param_name).value(context)
    #
    def param(self, param_name) -> Parameter:
        """
        shortcut to node.get_ui().parameter

        :param param_name:
        :return:
        """
        return self._parameters.parameter(param_name)

    def set_param_value(self, param_name, param_value) -> None:
        """
        shortcut to node.get_ui().set_parameter
        :param param_name: parameter name
        :param param_value: value to set
        :return:
        """
        return self._parameters.parameter(param_name).set_value(param_value)

    def get_ui(self) -> NodeUi:
        return self._parameters

    def is_input_connected(self, input_name: str) -> bool:
        """
        returns wether or not specified input is connected to the node
        note that these methods are supposed to be called both from main thread AND from within executor pool thread
        so creating tasks becomes tricky.
        :param input_name:
        :return:
        """
        try:
            thread_loop = asyncio.get_running_loop()
        except RuntimeError:  # no loop
            thread_loop = None
        fut = asyncio.run_coroutine_threadsafe(self.__parent.get_node_input_connections(self.__parent_nid, input_name), self.__parent.get_event_loop())
        if thread_loop is self.__parent.get_event_loop():  # we are in scheduler's main loop
            self.__logger.error('this method cannot be called from the main scheduler thread')
            raise RuntimeError('this method cannot be called from the main scheduler thread')
        else:
            conns = fut.result(60)
        return len(conns) > 0

    def is_output_connected(self, output_name: str):
        """
        returns wether or not specified output is connected to the node
        :param output_name:
        :return:
        """
        try:
            thread_loop = asyncio.get_running_loop()
        except RuntimeError:  # no loop
            thread_loop = None
        fut = asyncio.run_coroutine_threadsafe(self.__parent.get_node_output_connections(self.__parent_nid, output_name), self.__parent.get_event_loop())
        if thread_loop is self.__parent.get_event_loop():  # we are in scheduler's main loop
            self.__logger.error('this method cannot be called from the main scheduler thread')
            raise RuntimeError('this method cannot be called from the main scheduler thread')
        else:
            conns = fut.result(60)
        return len(conns) > 0

    def _ui_changed(self, definition_changed=False):
        """
        this methods gets called by self and NodeUi when a parameter changes to trigger node's database update
        :return:
        """
        if self.__parent is not None:
            # it is important to create task in main scheduler's event loop
            try:
                thread_loop = asyncio.get_running_loop()
            except RuntimeError:  # no loop
                thread_loop = None
            fut = asyncio.run_coroutine_threadsafe(self.__parent.node_reports_changes_needs_saving(self.__parent_nid), self.__parent.get_event_loop())
            if thread_loop is self.__parent.get_event_loop():  # we are in scheduler's main loop
                self.__logger.warning('this method probably should not be called from the main scheduler thread')
            else:  # we are not in main thread
                fut.result(60)  # ensure callback is completed before continuing. not sure how much it's needed, but it feels safer
                # TODO: even though timeout even here is impossible in any sane situation - still we should do something about it
            # and this is a nono: asyncio.get_event_loop().create_task(self.__parent.node_reports_changes_needs_saving(self.__parent_nid))

    # def _state_changed(self):
    #     """
    #     this methods should be called when important node state was changes to trigger node's database update
    #     :return:
    #     """
    #     if self.__parent is not None:
    #         # TODO: note that this may happen at any point in processing,
    #         #  so IF node subclass has internal state - it has to deal with ensuring state is consistent at time or writing
    #         #  this may also apply to _ui_changed above, but nodes really SHOULD NOT change their own parameters during processing
    #         asyncio.get_event_loop().create_task(self.__parent.node_reports_changes_needs_saving(self.__parent_nid))

    def _process_task_wrapper(self, task_dict) -> ProcessingResult:
        # with self.get_ui().lock_interface_readonly():  # TODO: this is bad, RETHINK!
        #  TODO: , in case threads do l1---r1    - release2 WILL leave lock in locked state forever, as it remembered it at l2
        #  TODO:                         l2---r2
        return self.process_task(ProcessingContext(self, task_dict))

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        """
        custom node classes subclassing this are supposed to override this method

        :param context:
        :return:
        """
        raise NotImplementedError()

    def _postprocess_task_wrapper(self, task_dict) -> ProcessingResult:
        # with self.get_ui().lock_interface_readonly():  #TODO: read comment for _process_task_wrapper
        return self.postprocess_task(ProcessingContext(self, task_dict))

    def postprocess_task(self, context: ProcessingContext) -> ProcessingResult:
        """
        custom node classes subclassing this are supposed to override this method

        :param context:
        :return:
        """
        return ProcessingResult()

    def copy_ui_to(self, to_node: "BaseNode"):
        newui = deepcopy(self._parameters)  # nodeUI redefines deepcopy to detach new copy from node
        to_node._parameters = newui
        newui.attach_to_node(to_node)

    def apply_settings(self, settings: Dict[str, Dict[str, Any]]) -> None:
        with self.get_ui().postpone_ui_callbacks():
            for param_name, value in settings.items():
                try:
                    param = self.param(param_name)
                    if isinstance(value, dict):
                        if 'value' in value:
                            param.set_value(value['value'])
                        if 'expression' in value:
                            param.set_expression(value['expression'])
                    else:
                        if param.has_expression():
                            param.remove_expression()
                        param.set_value(value)
                except ParameterNotFound:
                    self.logger().warning(f'applying settings: skipping unrecognized parameter "{param_name}"')
                    continue
                except ValueError as e:
                    self.logger().warning(f'applying settings: skipping parameter "{param_name}": bad value type: {str(e)}')
                    continue

    # # some helpers
    # def _get_task_attributes(self, task_row):
    #     return json.loads(task_row.get('attributes', '{}'))

    #
    # Plugin info
    #
    @classmethod
    def my_plugin(cls) -> PluginInfo:
        # this case was for nodetypes that are present in DB, but not loaded cuz of configuration errors
        #  but it doesn't make sense in current form - if node is created - plugin info will be present
        #  this needs to be rethought
        # # if type_name not in pluginloader.plugins:
        # #     return None
        return cls._plugin_data

    #
    # Serialize and back
    #

    def get_state(self) -> Optional[dict]:
        """
        override this to be able to save node's unique state if it has one
        None means node does not and will not have an internal state
        if node CAN have an internal state and it's just empty - return empty dict instead

        note: state will only be saved on normal exit, it won't be saved on crash, it's not part of any transaction
        """
        return None

    def set_state(self, state: dict):
        """
        restore state as given by get_state
        """
        pass


class BaseNodeWithTaskRequirements(BaseNode):
    def __init__(self, name: str):
        super(BaseNodeWithTaskRequirements, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            with ui.collapsable_group_block('main worker requirements', 'worker requirements'):
                ui.add_parameter('priority adjustment', 'priority adjustment', NodeParameterType.FLOAT, 0).set_slider_visualization(-100, 100)
                with ui.parameters_on_same_line_block():
                    ui.add_parameter('worker cpu cost', 'min <cpu (cores)> preferred', NodeParameterType.FLOAT, 1.0).set_value_limits(value_min=0)
                    ui.add_parameter('worker cpu cost preferred', None, NodeParameterType.FLOAT, 0.0).set_value_limits(value_min=0)
                with ui.parameters_on_same_line_block():
                    ui.add_parameter('worker mem cost', 'min <memory (GBs)> preferred', NodeParameterType.FLOAT, 0.5).set_value_limits(value_min=0)
                    ui.add_parameter('worker mem cost preferred', None, NodeParameterType.FLOAT, 0.0).set_value_limits(value_min=0)
                ui.add_parameter('worker groups', 'groups (space or comma separated)', NodeParameterType.STRING, '')
                ui.add_parameter('worker type', 'worker type', NodeParameterType.INT, WorkerType.STANDARD.value)\
                    .add_menu((('standard', WorkerType.STANDARD.value),
                               ('scheduler helper', WorkerType.SCHEDULER_HELPER.value)))
                with ui.collapsable_group_block('gpu main worker requirements', 'gpu requirements'):
                    with ui.parameters_on_same_line_block():
                        ui.add_parameter('worker gpu cost', 'min <gpus> preferred', NodeParameterType.FLOAT, 0.0).set_value_limits(value_min=0)
                        ui.add_parameter('worker gpu cost preferred', None, NodeParameterType.FLOAT, 0.0).set_value_limits(value_min=0)
                    with ui.parameters_on_same_line_block():
                        ui.add_parameter('worker gpu mem cost', 'min <memory (GBs)> preferred', NodeParameterType.FLOAT, 0.0).set_value_limits(value_min=0)
                        ui.add_parameter('worker gpu mem cost preferred', None, NodeParameterType.FLOAT, 0.0).set_value_limits(value_min=0)

    def __apply_requirements(self, task_dict: dict, result: ProcessingResult):
        if result.invocation_job is not None:
            context = ProcessingContext(self, task_dict)
            raw_groups = context.param_value('worker groups').strip()
            reqs = result.invocation_job.requirements()
            if raw_groups != '':
                reqs.add_groups(re.split(r'[ ,]+', raw_groups))
            reqs.set_min_cpu_count(context.param_value('worker cpu cost'))
            reqs.set_min_memory_bytes(context.param_value('worker mem cost') * 10**9)
            reqs.set_min_gpu_count(context.param_value('worker gpu cost'))
            reqs.set_min_gpu_memory_bytes(context.param_value('worker gpu mem cost') * 10**9)
            # preferred
            pref_cpu_count = context.param_value('worker cpu cost preferred')
            pref_mem_bytes = context.param_value('worker mem cost preferred') * 10**9
            pref_gpu_count = context.param_value('worker gpu cost preferred')
            pref_gpu_mem_bytes = context.param_value('worker gpu mem cost preferred') * 10**9
            if pref_cpu_count > 0:
                reqs.set_preferred_cpu_count(pref_cpu_count)
            if pref_mem_bytes > 0:
                reqs.set_preferred_memory_bytes(pref_mem_bytes)
            if pref_gpu_count > 0:
                reqs.set_preferred_gpu_count(pref_gpu_count)
            if pref_gpu_mem_bytes > 0:
                reqs.set_preferred_gpu_memory_bytes(pref_gpu_mem_bytes)

            reqs.set_worker_type(WorkerType(context.param_value('worker type')))
            result.invocation_job.set_requirements(reqs)
            result.invocation_job.set_priority(context.param_value('priority adjustment'))
        return result

    def _process_task_wrapper(self, task_dict) -> ProcessingResult:
        result = super(BaseNodeWithTaskRequirements, self)._process_task_wrapper(task_dict)
        return self.__apply_requirements(task_dict, result)

    def _postprocess_task_wrapper(self, task_dict) -> ProcessingResult:
        result = super(BaseNodeWithTaskRequirements, self)._postprocess_task_wrapper(task_dict)
        return self.__apply_requirements(task_dict, result)


# class BaseNodeWithEnvironmentRequirements(BaseNode):
#     def __init__(self, name: str):
#         super(BaseNodeWithEnvironmentRequirements, self).__init__(name)
#         ui = self.get_ui()
#         with ui.initializing_interface_lock():
#             with ui.collapsable_group_block('main environment resolver', 'task environment resolver additional requirements'):
#                 ui.add_parameter('main env resolver name', 'resolver name', NodeParameterType.STRING, 'StandardEnvironmentResolver')
#                 with ui.multigroup_parameter_block('main env resolver arguments'):
#                     with ui.parameters_on_same_line_block():
#                         type_param = ui.add_parameter('main env resolver arg type', '', NodeParameterType.INT, 0)
#                         type_param.add_menu((('int', NodeParameterType.INT.value),
#                                              ('bool', NodeParameterType.BOOL.value),
#                                              ('float', NodeParameterType.FLOAT.value),
#                                              ('string', NodeParameterType.STRING.value),
#                                              ('json', -1)
#                                              ))
#
#                         ui.add_parameter('main env resolver arg svalue', 'val', NodeParameterType.STRING, '').append_visibility_condition(type_param, '==', NodeParameterType.STRING.value)
#                         ui.add_parameter('main env resolver arg ivalue', 'val', NodeParameterType.INT, 0).append_visibility_condition(type_param, '==', NodeParameterType.INT.value)
#                         ui.add_parameter('main env resolver arg fvalue', 'val', NodeParameterType.FLOAT, 0.0).append_visibility_condition(type_param, '==', NodeParameterType.FLOAT.value)
#                         ui.add_parameter('main env resolver arg bvalue', 'val', NodeParameterType.BOOL, False).append_visibility_condition(type_param, '==', NodeParameterType.BOOL.value)
#                         ui.add_parameter('main env resolver arg jvalue', 'val', NodeParameterType.STRING, '').append_visibility_condition(type_param, '==', -1)
#
#     def _process_task_wrapper(self, task_dict) -> ProcessingResult:
#         result = super(BaseNodeWithEnvironmentRequirements, self)._process_task_wrapper(task_dict)
#         result.invocation_job.environment_resolver_arguments()
#         return result
