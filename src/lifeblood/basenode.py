import asyncio
from copy import deepcopy
from typing import Dict, Optional, Any
from .nodethings import ProcessingResult
from .uidata import NodeUi, ParameterNotFound, Parameter
from .processingcontext import ProcessingContext
from .logging import get_logger
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

    def parent(self) -> Optional[NodeGraphHolderBase]:
        return self.__parent

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

    def _process_task_wrapper(self, task_dict, node_config) -> ProcessingResult:
        # with self.get_ui().lock_interface_readonly():  # TODO: this is bad, RETHINK!
        #  TODO: , in case threads do l1---r1    - release2 WILL leave lock in locked state forever, as it remembered it at l2
        #  TODO:                         l2---r2
        return self.process_task(ProcessingContext(self, task_dict, node_config))

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        """
        custom node classes subclassing this are supposed to override this method

        :param context:
        :return:
        """
        raise NotImplementedError()

    def _postprocess_task_wrapper(self, task_dict, node_config) -> ProcessingResult:
        # with self.get_ui().lock_interface_readonly():  #TODO: read comment for _process_task_wrapper
        return self.postprocess_task(ProcessingContext(self, task_dict, node_config))

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
