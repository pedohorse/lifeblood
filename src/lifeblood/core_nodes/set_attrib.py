from lifeblood.basenode import BaseNode
from lifeblood.nodethings import ProcessingResult
from lifeblood.enums import NodeParameterType

from typing import Iterable


def node_class():
    return SetAttributes


class SetAttributes(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'set attributes'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'set', 'attribute', 'core'

    @classmethod
    def type_name(cls) -> str:
        return 'set_attrib'

    def __init__(self, name: str):
        super(SetAttributes, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            with ui.multigroup_parameter_block('attr_count'):
                with ui.parameters_on_same_line_block():
                    ui.add_parameter('name', 'Name', NodeParameterType.STRING, '')
                    type_param = ui.add_parameter('type', '', NodeParameterType.INT, 0)
                    type_param.add_menu((('int', NodeParameterType.INT.value),
                                         ('bool', NodeParameterType.BOOL.value),
                                         ('float', NodeParameterType.FLOAT.value),
                                         ('string', NodeParameterType.STRING.value),
                                         ('integer range', -1)))

                    ui.add_parameter('svalue', 'val', NodeParameterType.STRING, '').append_visibility_condition(type_param, '==', NodeParameterType.STRING.value)
                    ui.add_parameter('ivalue', 'val', NodeParameterType.INT, 0).append_visibility_condition(type_param, '==', NodeParameterType.INT.value)
                    ui.add_parameter('fvalue', 'val', NodeParameterType.FLOAT, 0.0).append_visibility_condition(type_param, '==', NodeParameterType.FLOAT.value)
                    ui.add_parameter('bvalue', 'val', NodeParameterType.BOOL, False).append_visibility_condition(type_param, '==', NodeParameterType.BOOL.value)
                    # range
                    ui.add_parameter('rf1value', 'to', NodeParameterType.INT, 0).append_visibility_condition(type_param, '==', -1)
                    ui.add_parameter('rf2value', 'step', NodeParameterType.INT, 9).append_visibility_condition(type_param, '==', -1)
                    ui.add_parameter('rf3value', '', NodeParameterType.INT, 1).append_visibility_condition(type_param, '==', -1)
            ui.color_scheme().set_main_color(0.15, 0.24, 0.25)
        # # Example how one would initialize multiblock to have initial nonzero value
        # multiblock = list(ui.items())[-1]
        # assert isinstance(multiblock, MultiGroupLayout)  # TODO: find a better workflow of getting that last layout added
        # multiblock.add_template_instance()
        # multiblock.add_template_instance()

        # def _printone(level, i=0):
        #     if isinstance(level, Parameter):
        #         print('\t'*(i-1), f'====parameter {level.name()}')
        #         return
        #     for item in level.items(recursive=False):
        #         print('\t'*i, item)
        #         _printone(item, i+1)

        #_printone(ui.main_parameter_layout())

    def process_task(self, context) -> ProcessingResult:
        attr_count = context.param_value('attr_count')
        res = ProcessingResult()
        for i in range(attr_count):
            attr_name = context.param_value(f'name_{i}')
            if attr_name == '':  # however some spaces - is a valid parameter name
                continue
            attr_type = context.param_value(f'type_{i}')
            if attr_type == NodeParameterType.INT.value:
                attr_val = context.param_value(f'ivalue_{i}')
            elif attr_type == NodeParameterType.BOOL.value:
                attr_val = context.param_value(f'bvalue_{i}')
            elif attr_type == NodeParameterType.FLOAT.value:
                attr_val = context.param_value(f'fvalue_{i}')
            elif attr_type == NodeParameterType.STRING.value:
                attr_val = context.param_value(f'svalue_{i}')
            elif attr_type == -1:
                attr_val = list(range(context.param_value(f'rf1value_{i}'), context.param_value(f'rf2value_{i}') + 1, context.param_value(f'rf3value_{i}')))
            else:
                raise NotImplementedError('unexpected type')
            res.set_attribute(attr_name, attr_val)
        return res

    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()
