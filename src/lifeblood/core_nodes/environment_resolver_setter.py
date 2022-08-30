from lifeblood.basenode import BaseNode, ProcessingError
from lifeblood.nodethings import ProcessingResult
from lifeblood.processingcontext import ProcessingContext
from lifeblood.enums import NodeParameterType
from lifeblood.environment_resolver import EnvironmentResolverArguments
from lifeblood.uidata import NodeUi, MultiGroupLayout, Parameter
from lifeblood.node_visualization_classes import NodeColorScheme

from typing import Iterable


def node_class():
    return EnvironmentResolverArgumentsSetter


class EnvironmentResolverArgumentsSetter(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'modify environment resolver arguments'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'modify', 'environment', 'resolver'

    @classmethod
    def type_name(cls) -> str:
        return 'environment_resolver_arguments_setter'

    def __init__(self, name: str):
        super(EnvironmentResolverArgumentsSetter, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            mode_param = ui.add_parameter('mode', 'mode', NodeParameterType.STRING, 'modify')\
                                .add_menu((('Set', 'set'),
                                           ('Modify', 'modify')))
            with ui.parameters_on_same_line_block():
                ui.add_parameter('set resolver name', 'set resolver', NodeParameterType.BOOL, False).append_visibility_condition(mode_param, '==', 'modify')
                ui.add_parameter('resolver name', 'resolver', NodeParameterType.STRING, '')
            ui.add_separator()
            with ui.multigroup_parameter_block('arguments', 'arguments to set'):
                with ui.parameters_on_same_line_block():
                    ui.add_parameter('arg name', '<name', NodeParameterType.STRING, 'arg name')
                    type_param = ui.add_parameter('type', 'value>', NodeParameterType.INT, 0)
                    type_param.add_menu((('int', NodeParameterType.INT.value),
                                         ('bool', NodeParameterType.BOOL.value),
                                         ('float', NodeParameterType.FLOAT.value),
                                         ('string', NodeParameterType.STRING.value)))

                    ui.add_parameter('svalue', 'val', NodeParameterType.STRING, '').append_visibility_condition(type_param, '==', NodeParameterType.STRING.value)
                    ui.add_parameter('ivalue', 'val', NodeParameterType.INT, 0).append_visibility_condition(type_param, '==', NodeParameterType.INT.value)
                    ui.add_parameter('fvalue', 'val', NodeParameterType.FLOAT, 0.0).append_visibility_condition(type_param, '==', NodeParameterType.FLOAT.value)
                    ui.add_parameter('bvalue', 'val', NodeParameterType.BOOL, False).append_visibility_condition(type_param, '==', NodeParameterType.BOOL.value)

            ui.add_separator()
            with ui.multigroup_parameter_block('arguments to delete', 'arguments to delete'):
                ui.add_parameter('arg name to delete', 'argument to delete', NodeParameterType.STRING, '')

            ui.parameter('arguments to delete').append_visibility_condition(mode_param, '==', 'modify')

        # now initialize default values
        ui.parameter('arguments').set_value(1)
        ui.parameter('arg name_0').set_value('user')
        ui.parameter('type_0').set_value(NodeParameterType.STRING.value)
        ui.parameter('svalue_0').set_value('someusername')

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        resolver_name = context.param_value('resolver name').strip()
        arguments = {}
        for i in range(context.param_value('arguments')):
            # we are being sane with names, though name can technically be any unicode string
            name = context.param_value(f'arg name_{i}').strip()
            if name == '':
                continue

            argtype = context.param_value(f'type_{i}')
            if argtype == NodeParameterType.INT.value:
                val = context.param_value(f'ivalue_{i}')
            elif argtype == NodeParameterType.BOOL.value:
                val = context.param_value(f'bvalue_{i}')
            elif argtype == NodeParameterType.FLOAT.value:
                val = context.param_value(f'fvalue_{i}')
            elif argtype == NodeParameterType.STRING.value:
                val = context.param_value(f'svalue_{i}')
            else:
                raise ProcessingError(f'unknown argument type: {argtype}')
            arguments[name] = val

        if context.param_value('mode') == 'set':
            res = ProcessingResult()
            res.set_environment_resolver_arguments(EnvironmentResolverArguments(resolver_name, arguments))

            return res
        else:  # mode is modify
            if not context.param_value('set resolver name'):
                resolver_name = None

        # else mode is modify (no other modes exist)
        env_arguments = context.task_environment_resolver_arguments()
        if resolver_name is not None:
            env_arguments.set_name(resolver_name)
        for arg, val in arguments.items():
            env_arguments.add_argument(arg, val)

        for i in range(context.param_value('arguments to delete')):
            name = context.param_value(f'arg name to delete_{i}').strip()
            if name == '':
                continue
            if name in env_arguments.arguments():
                env_arguments.remove_argument(name)

        res = ProcessingResult()
        res.set_environment_resolver_arguments(env_arguments)

        return res

