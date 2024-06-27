"""
This high-level module has base classes to be used by plugin nodes instead of directly using BaseNode from lifeblood.basenode
"""

import re
from .basenode import BaseNode
from .enums import NodeParameterType, WorkerType
from .processingcontext import ProcessingContext
from .nodethings import ProcessingResult, ProcessingError  # unused import - for easy reexport to plugins
from .scheduler.scheduler import Scheduler


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

    def __apply_requirements(self, task_dict: dict, node_config: dict, result: ProcessingResult):
        if result.invocation_job is not None:
            context = ProcessingContext(self, task_dict, node_config)
            raw_groups = context.param_value('worker groups').strip()
            reqs = result.invocation_job.requirements()
            if raw_groups != '':
                reqs.add_groups(re.split(r'[ ,]+', raw_groups))

            reqs.set_min_resource('cpu_count', context.param_value('worker cpu cost'))
            reqs.set_min_resource('cpu_mem', context.param_value('worker mem cost') * 10**9)
            reqs.set_min_resource('gpu_count', context.param_value('worker gpu cost'))
            reqs.set_min_resource('gpu_mem', context.param_value('worker gpu mem cost') * 10**9)
            # preferred
            reqs.set_preferred_resource('cpu_count', context.param_value('worker cpu cost preferred'))
            reqs.set_preferred_resource('cpu_mem', context.param_value('worker mem cost preferred') * 10**9)
            reqs.set_preferred_resource('gpu_count', context.param_value('worker gpu cost preferred'))
            reqs.set_preferred_resource('gpu_mem', context.param_value('worker gpu mem cost preferred') * 10**9)

            reqs.set_worker_type(WorkerType(context.param_value('worker type')))
            result.invocation_job.set_requirements(reqs)
            result.invocation_job.set_priority(context.param_value('priority adjustment'))
        return result

    def _process_task_wrapper(self, task_dict, node_config) -> ProcessingResult:
        result = super(BaseNodeWithTaskRequirements, self)._process_task_wrapper(task_dict, node_config)
        return self.__apply_requirements(task_dict, node_config, result)

    def _postprocess_task_wrapper(self, task_dict, node_config) -> ProcessingResult:
        result = super(BaseNodeWithTaskRequirements, self)._postprocess_task_wrapper(task_dict, node_config)
        return self.__apply_requirements(task_dict, node_config, result)


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
