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
            ui.add_parameter('mode', 'mode', NodeParameterType.STRING, 'set')\
                .add_menu((('set', 'set'),))
            ui.add_parameter('resolver name', 'resolver', NodeParameterType.STRING, 'StandardEnvironmentResolver')
            with ui.multigroup_parameter_block('arguments'):
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

        # now initialize default values
        ui.parameter('arguments').set_value(1)
        ui.parameter('arg name_0').set_value('user')
        ui.parameter('type_0').set_value(NodeParameterType.STRING.value)
        ui.parameter('svalue_0').set_value('someusername')

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        resolver_name = context.param_value('resolver name')
        arguments = {}
        for i in range(context.param_value('arguments')):
            name = context.param_value(f'arg name_{i}')
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

        res = ProcessingResult()
        res.set_environment_resolver_arguments(EnvironmentResolverArguments(resolver_name, arguments))

        return res
