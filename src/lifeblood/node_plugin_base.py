"""
This high-level module has base classes to be used by plugin nodes instead of directly using BaseNode from lifeblood.basenode
"""

import re
from .basenode import BaseNode
from .enums import NodeParameterType, WorkerType
from .nodegraph_holder_base import NodeGraphHolderBase
from .processingcontext import ProcessingContext
from .nodethings import ProcessingResult, ProcessingError  # unused import - for easy reexport to plugins
from .worker_resource_definition import WorkerResourceDefinition, WorkerResourceDataType
from .uidata import NodeUi
from .scheduler.scheduler import Scheduler

from typing import Dict


class BaseNodeWithTaskRequirements(BaseNode):
    def __init__(self, name: str):
        super(BaseNodeWithTaskRequirements, self).__init__(name)
        self.__res_definitions: Dict[str, WorkerResourceDefinition] = {}

        ui = self.get_ui()
        with ui.initializing_interface_lock():
            with ui.collapsable_group_block('__requirements__', 'worker requirements'):
                ui.add_parameter('__requirements__.priority_adjustment', 'priority adjustment', NodeParameterType.FLOAT, 0).set_slider_visualization(-100, 100)
                with ui.multigroup_parameter_block('__requirements__.res', 'Resources'):
                    with ui.parameters_on_same_line_block():
                        name_param = ui.add_parameter('__requirements__.name_res', None, NodeParameterType.STRING, 'res')
                        ui.add_parameter('__requirements__.label_res', None, NodeParameterType.STRING, 'label')
                        type_param = ui.add_parameter('__requirements__.type_res', None, NodeParameterType.INT, 0, can_have_expressions=False)
                        name_param.set_hidden(True)
                        type_param.set_hidden(True)
                        ui.add_parameter('__requirements__.f_min_res', 'min <> preferred', NodeParameterType.FLOAT, 0.0) \
                            .set_value_limits(value_min=0) \
                            .append_visibility_condition(type_param, '==', 0)
                        ui.add_parameter('__requirements__.f_pref_res', None, NodeParameterType.FLOAT, 0.0) \
                            .set_value_limits(value_min=0) \
                            .append_visibility_condition(type_param, '==', 0)
                        ui.add_parameter('__requirements__.i_min_res', 'min <> preferred', NodeParameterType.INT, 0) \
                            .set_value_limits(value_min=0) \
                            .append_visibility_condition(type_param, '==', 1)
                        ui.add_parameter('__requirements__.i_pref_res', None, NodeParameterType.INT, 0) \
                            .set_value_limits(value_min=0) \
                            .append_visibility_condition(type_param, '==', 1)
                ui.parameter('__requirements__.res').set_hidden(True)
                ui.add_parameter('__requirements__.worker_groups', 'groups (space or comma separated)', NodeParameterType.STRING, '')
                ui.add_parameter('__requirements__.worker_type', 'worker type', NodeParameterType.INT, WorkerType.STANDARD.value)\
                    .add_menu((('standard', WorkerType.STANDARD.value),
                               ('scheduler helper', WorkerType.SCHEDULER_HELPER.value)))

    def __check_set(self, param_name: str, value):
        # NOTE: no context here, expressions are NOT expected (must be forbidden) in these parameters
        param = self.param(param_name)
        if param.value() == value and param.is_locked():
            return
        param.set_locked(False)
        param.set_value(value)
        param.set_locked(True)

    def set_parent(self, graph_holder: NodeGraphHolderBase, node_id_in_graph: int):
        # inherit resource definitions from newly attached parent
        self.__res_definitions = {}
        if isinstance(graph_holder, Scheduler):
            config_provider = graph_holder.config_provider
            # TODO: store existing values and set them after to preserve when defs chane
            resource_defs = config_provider.hardware_resource_definitions()
            self.__res_definitions = {x.name: x for x in resource_defs}

            self.__check_set('__requirements__.res', len(resource_defs))
            for i, res_def in enumerate(resource_defs):
                self.__check_set(f'__requirements__.name_res_{i}', res_def.name)
                self.__check_set(f'__requirements__.label_res_{i}', res_def.label or res_def.name)
                if res_def.type in (WorkerResourceDataType.GENERIC_FLOAT, WorkerResourceDataType.SHARABLE_COMPUTATIONAL_UNIT, WorkerResourceDataType.MEMORY_BYTES):
                    self.__check_set(f'__requirements__.type_res_{i}', 0)
                elif res_def.type in (WorkerResourceDataType.GENERIC_INT,):
                    self.__check_set(f'__requirements__.type_res_{i}', 1)
                else:
                    raise NotImplementedError(f'unknown resource data type "{res_def.type}"')
        #
        super().set_parent(graph_holder, node_id_in_graph)

    def __apply_requirements(self, task_dict: dict, node_config: dict, result: ProcessingResult):
        if result.invocation_job is not None:
            context = ProcessingContext(self, task_dict, node_config)
            raw_groups = context.param_value('__requirements__.worker_groups').strip()
            reqs = result.invocation_job.requirements()
            if raw_groups != '':
                reqs.add_groups(re.split(r'[ ,]+', raw_groups))

            for res_i in range(context.param_value('__requirements__.res')):
                res_name = context.param_value(f'__requirements__.name_res_{res_i}')
                res_type = context.param_value(f'__requirements__.type_res_{res_i}')
                if res_type == 0:  # Float
                    res_min = context.param_value(f'__requirements__.f_min_res_{res_i}')
                    res_pref = context.param_value(f'__requirements__.f_pref_res_{res_i}')
                elif res_type == 1:  # Int
                    res_min = context.param_value(f'__requirements__.i_min_res_{res_i}')
                    res_pref = context.param_value(f'__requirements__.i_pref_res_{res_i}')
                else:
                    raise NotImplementedError(f'unknown resource type value: {res_type}')
                if res_def := self.__res_definitions.get(res_name):
                    if res_def.type == WorkerResourceDataType.MEMORY_BYTES:
                        # user inputs memory in float GBs
                        res_min = int(res_min * 10**9)
                        res_pref = int(res_pref * 10 ** 9)
                reqs.set_min_resource(
                    res_name,
                    res_min
                )
                reqs.set_preferred_resource(
                    res_name,
                    res_pref
                )
            reqs.set_worker_type(WorkerType(context.param_value('__requirements__.worker_type')))
            result.invocation_job.set_requirements(reqs)
            result.invocation_job.set_priority(context.param_value('__requirements__.priority_adjustment'))
        return result

    def _process_task_wrapper(self, task_dict, node_config) -> ProcessingResult:
        result = super(BaseNodeWithTaskRequirements, self)._process_task_wrapper(task_dict, node_config)
        return self.__apply_requirements(task_dict, node_config, result)

    def _postprocess_task_wrapper(self, task_dict, node_config) -> ProcessingResult:
        result = super(BaseNodeWithTaskRequirements, self)._postprocess_task_wrapper(task_dict, node_config)
        return self.__apply_requirements(task_dict, node_config, result)
