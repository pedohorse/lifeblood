import asyncio
from enum import Enum
import pickle
import json
import re
from copy import copy
from typing import Dict, Optional, List, Any
from .nodethings import ProcessingResult, ProcessingError
from .uidata import NodeUi, ParameterNotFound, ParameterReadonly, Parameter
from .pluginloader import create_node, plugin_hash
from .processingcontext import ProcessingContext
from .logging import get_logger
from .enums import NodeParameterType

from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:
    from .scheduler import Scheduler


class BaseNode:
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
        self.__parent: Scheduler = None
        self.__parent_nid: int = None
        self._parameters: NodeUi = NodeUi(self)
        self.__name = name
        # subclass is expected to add parameters at this point

    def _set_parent(self, parent_scheduler, node_id):
        self.__parent = parent_scheduler
        self.__parent_nid = node_id

    def name(self):
        return self.__name

    def set_name(self, name: str):
        self.__name = name

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
        fut = asyncio.run_coroutine_threadsafe(self.__parent.get_node_input_connections(self.__parent_nid, input_name), self.__parent.get_event_loop())
        conns = fut.result(60)
        return len(conns) > 0

    def is_output_connected(self, output_name: str):
        """
        returns wether or not specified output is connected to the node
        :param output_name:
        :return:
        """
        fut = asyncio.run_coroutine_threadsafe(self.__parent.get_node_output_connections(self.__parent_nid, output_name), self.__parent.get_event_loop())
        conns = fut.result(60)
        return len(conns) > 0

    def _ui_changed(self):
        """
        this methods gets called by self and NodeUi when a parameter changes to trigger node's database update
        :return:
        """
        if self.__parent is not None:
            asyncio.get_event_loop().create_task(self.__parent.node_reports_changes_needs_saving(self.__parent_nid))

    # def _state_changed(self):
    #     """
    #     this methods should be called when important node state was changes to trigger node's database update
    #     :return:
    #     """
    #     if self.__parent is not None:
    #         # TODO: note that this may happen at any point in processing,
    #         #  so IF node subclass has internal state - it has to deal with ensuring state is consistent at time or writing
    #         asyncio.get_event_loop().create_task(self.__parent.node_reports_changes_needs_saving(self.__parent_nid))

    def ready_to_process_task(self, task_dict) -> bool:
        """
        must be VERY fast - this will be run in main thread
        there is no context wrapper just to skip unnessesary overhead as not many nodes will ever override this
        """
        return True

    def ready_to_postprocess_task(self, task_dict) -> bool:
        """
        must be VERY fast - this will be run in main thread
        there is no context wrapper just to skip unnessesary overhead as not many nodes will ever override this
        """
        return True

    def _process_task_wrapper(self, task_dict) -> ProcessingResult:
        return self.process_task(ProcessingContext(self, task_dict))

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        """
        custom node classes subclassing this are supposed to override this method

        :param context:
        :return:
        """
        raise NotImplementedError()

    def _postprocess_task_wrapper(self, task_dict) -> ProcessingResult:
        return self.postprocess_task(ProcessingContext(self, task_dict))

    def postprocess_task(self, context: ProcessingContext) -> ProcessingResult:
        """
        custom node classes subclassing this are supposed to override this method

        :param context:
        :return:
        """
        return ProcessingResult()

    # # some helpers
    # def _get_task_attributes(self, task_row):
    #     return json.loads(task_row.get('attributes', '{}'))

    #
    # Serialize and back
    #
    def __reduce__(self):
        # typename = type(self).__module__
        # if '.' in typename:
        #     typename = typename.rsplit('.', 1)[-1]
        return create_node, (self.type_name(), '', None, None), self.__getstate__()

    def __getstate__(self):
        # TODO: if u ever implement parameter expressions - be VERY careful with pickling expressions referencing across nodes
        d = copy(self.__dict__)
        assert '_BaseNode__parent' in d
        d['_BaseNode__parent'] = None
        d['_BaseNode__parent_nid'] = None
        d['_BaseNode__saved_plugin_hash'] = plugin_hash(self.type_name())  # we will use this hash to detect plugin module changes on load
        return d

    def __setstate__(self, state):
        # the idea here is to update node's class instance IF plugin hash is different from the saved one
        # the hash being different means that node's definition was updated - we don't know how
        # so what we do is save all parameter values, merge old state values with new
        # and hope for the best...

        hash = plugin_hash(self.type_name())
        if hash != state.get('_BaseNode__saved_plugin_hash', None):
            self.__init__(state.get('name', ''))
            # update all except ui
            try:
                if '_parameters' in state:
                    old_ui: NodeUi = state['_parameters']
                    del state['_parameters']
                    self.__dict__.update(state)
                    new_ui = self.get_ui()
                    for param in old_ui.parameters():
                        try:
                            newparam = new_ui.parameter(param.name())
                        except ParameterNotFound:
                            continue
                        try:
                            newparam.set_value(param.unexpanded_value())
                        except ParameterReadonly:
                            newparam._Parameter__value = param.unexpanded_value()
            except AttributeError:
                # something changed so much that some core attrs are different
                get_logger('BaseNode').exception(f'could not update interface for some node of type {self.type_name()}. resetting node\'s inrerface')


            # TODO: if and whenever expressions are introduced - u need to take care of expressions here too!
        else:
            self.__dict__.update(state)

    def serialize(self) -> bytes:
        """
        by default we just serialize
        :return:
        """
        return pickle.dumps(self)

    async def serialize_async(self) -> bytes:
        return await asyncio.get_event_loop().run_in_executor(None, self.serialize)

    @classmethod
    def deserialize(cls, data: bytes, parent_scheduler, node_id):
        newobj = pickle.loads(data)
        newobj.__parent = parent_scheduler
        newobj.__parent_nid = node_id
        return newobj

    @classmethod
    async def deserialize_async(cls, data: bytes, parent_scheduler, node_id):
        return await asyncio.get_event_loop().run_in_executor(None, cls.deserialize, data, parent_scheduler, node_id)


class BaseNodeWithTaskRequirements(BaseNode):
    def __init__(self, name: str):
        super(BaseNodeWithTaskRequirements, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            with ui.collapsable_group_block('main worker requirements', 'worker requirements'):
                ui.add_parameter('worker cpu cost', 'cpu cost (cores)', NodeParameterType.FLOAT, 0).set_value_limits(value_min=0)
                ui.add_parameter('worker mem cost', 'memory cost (GBs)', NodeParameterType.FLOAT, 0).set_value_limits(value_min=0)
                ui.add_parameter('worker groups', 'groups (space or comma separated)', NodeParameterType.STRING, '')
    
    def _process_task_wrapper(self, task_dict) -> ProcessingResult:
        result = super(BaseNodeWithTaskRequirements, self)._process_task_wrapper(task_dict)
        if result.invocation_job is not None:
            context = ProcessingContext(self, task_dict)
            reqs = result.invocation_job.requirements()
            reqs.add_groups(re.split(r'[ ,]+', context.param_value('worker groups').strip()))
            result.invocation_job.set_requirements(reqs)
        return result

    def _postprocess_task_wrapper(self, task_dict) -> ProcessingResult:
        result = super(BaseNodeWithTaskRequirements, self)._postprocess_task_wrapper(task_dict)
        if result.invocation_job is not None:
            context = ProcessingContext(self, task_dict)
            reqs = result.invocation_job.requirements()
            reqs.add_groups(re.split(r'[ ,]+', context.param_value('worker groups').strip()))
            result.invocation_job.set_requirements(reqs)
        return result


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
#                         ui.add_parameter('main env resolver arg svalue', 'val', NodeParameterType.STRING, '').add_visibility_condition(type_param, '==', NodeParameterType.STRING.value)
#                         ui.add_parameter('main env resolver arg ivalue', 'val', NodeParameterType.INT, 0).add_visibility_condition(type_param, '==', NodeParameterType.INT.value)
#                         ui.add_parameter('main env resolver arg fvalue', 'val', NodeParameterType.FLOAT, 0.0).add_visibility_condition(type_param, '==', NodeParameterType.FLOAT.value)
#                         ui.add_parameter('main env resolver arg bvalue', 'val', NodeParameterType.BOOL, False).add_visibility_condition(type_param, '==', NodeParameterType.BOOL.value)
#                         ui.add_parameter('main env resolver arg jvalue', 'val', NodeParameterType.STRING, '').add_visibility_condition(type_param, '==', -1)
#
#     def _process_task_wrapper(self, task_dict) -> ProcessingResult:
#         result = super(BaseNodeWithEnvironmentRequirements, self)._process_task_wrapper(task_dict)
#         result.invocation_job.environment_resolver_arguments()
#         return result
