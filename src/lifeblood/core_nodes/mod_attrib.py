from lifeblood.basenode import BaseNode
from lifeblood.nodethings import ProcessingResult
from lifeblood.taskspawn import TaskSpawn
from lifeblood.exceptions import NodeNotReadyToProcess
from lifeblood.enums import NodeParameterType
from lifeblood.uidata import NodeUi, MultiGroupLayout, Parameter
from lifeblood.node_visualization_classes import NodeColorScheme

from typing import Iterable


def node_class():
    return ModifyAttributes


class ModifyAttributes(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'modify attributes'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'modify', 'attribute', 'core'

    @classmethod
    def type_name(cls) -> str:
        return 'mod_attrib'

    def __init__(self, name: str):
        super(ModifyAttributes, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.15, 0.24, 0.25)
            with ui.multigroup_parameter_block('attr_count'):
                with ui.parameters_on_same_line_block():
                    ui.add_parameter('name', 'Name', NodeParameterType.STRING, '')
                    optype_param = ui.add_parameter('optype', '', NodeParameterType.INT, 0)
                    optype_param.add_menu((('add', 0),
                                           ('sub', 1),
                                           ('mul', 2),
                                           ('div', 3),
                                           ('concat', 100),
                                           ('append', 200),
                                           ('pop', 201),
                                           ('slice', 202),
                                           ))

                    valtype_param = ui.add_parameter('valtype', '', NodeParameterType.INT, NodeParameterType.INT.value)
                    valtype_param.append_visibility_condition(optype_param, '<', 100)
                    valtype_param.add_menu((('int', NodeParameterType.INT.value),
                                            ('float', NodeParameterType.FLOAT.value)))

                    ui.add_parameter('ivalue', 'val', NodeParameterType.INT, 0)\
                        .append_visibility_condition(optype_param, '<', 100)\
                        .append_visibility_condition(valtype_param, '==', NodeParameterType.INT.value)
                    ui.add_parameter('fvalue', 'val', NodeParameterType.FLOAT, 0.0)\
                        .append_visibility_condition(optype_param, '<', 100)\
                        .append_visibility_condition(valtype_param, '==', NodeParameterType.FLOAT.value)

                    ui.add_parameter('svalue', 'val', NodeParameterType.STRING, '')\
                        .append_visibility_condition(optype_param, '==', 100)

                    listvaltype_param = ui.add_parameter('lvaltype', '', NodeParameterType.INT, NodeParameterType.INT.value)\
                        .add_menu((('int', NodeParameterType.INT.value),
                                   ('float', NodeParameterType.FLOAT.value),
                                   ('string', NodeParameterType.STRING.value)))\
                        .append_visibility_condition(optype_param, '==', 200)
                    ui.add_parameter('lfvalue', 'val', NodeParameterType.FLOAT, 0.0) \
                        .append_visibility_condition(optype_param, '==', 200)\
                        .append_visibility_condition(listvaltype_param, '==', NodeParameterType.FLOAT.value)
                    ui.add_parameter('livalue', 'val', NodeParameterType.INT, 0) \
                        .append_visibility_condition(optype_param, '==', 200)\
                        .append_visibility_condition(listvaltype_param, '==', NodeParameterType.INT.value)
                    ui.add_parameter('lsvalue', 'val', NodeParameterType.STRING, '') \
                        .append_visibility_condition(optype_param, '==', 200)\
                        .append_visibility_condition(listvaltype_param, '==', NodeParameterType.STRING.value)

                    # slice
                    ui.add_parameter('lsl0value', '', NodeParameterType.INT, 0) \
                        .append_visibility_condition(optype_param, '==', 202)
                    ui.add_parameter('lsl1value', '', NodeParameterType.INT, 0) \
                        .append_visibility_condition(optype_param, '==', 202)
                    ui.add_parameter('lsl2value', 'start/end/step', NodeParameterType.INT, 1) \
                        .append_visibility_condition(optype_param, '==', 202)

    def process_task(self, context) -> ProcessingResult:
        attr_count = context.param_value('attr_count')
        res = ProcessingResult()
        to_set = {}
        for i in range(attr_count):
            attrs = context.task_attributes()
            attr_name = context.param_value(f'name_{i}')
            if attr_name == '':  # however some spaces - is a valid parameter name
                continue
            if attr_name in attrs:
                to_set[attr_name] = attrs[attr_name]

            op_type = context.param_value(f'optype_{i}')
            if op_type < 100:
                attr_type = context.param_value(f'valtype_{i}')
                if attr_type == NodeParameterType.INT.value:
                    attr_val = context.param_value(f'ivalue_{i}')
                    if attr_name not in to_set:
                        to_set[attr_name] = 0
                    # remember - sets should be independent, cuz if u do them dependant on the order -
                    #  the expression eval will still be independent and it's gonna be a confusing mess

                elif attr_type == NodeParameterType.FLOAT.value:
                    attr_val = context.param_value(f'fvalue_{i}')
                    if attr_name not in to_set:
                        to_set[attr_name] = 0.0
                else:
                    raise NotImplementedError()
                if op_type == 0:
                    to_set[attr_name] += attr_val
                elif op_type == 1:
                    to_set[attr_name] -= attr_val
                elif op_type == 2:
                    to_set[attr_name] *= attr_val
                elif op_type == 3:
                    to_set[attr_name] /= attr_val
                else:
                    raise NotImplementedError()
            elif op_type == 100:
                # then attrib type is supposed to be string
                if attr_name not in to_set:
                    to_set[attr_name] = ''
                to_set[attr_name] += context.param_value(f'svalue_{i}')
            elif op_type == 200:  # append
                attr_type = context.param_value(f'lvaltype_{i}')
                if attr_name not in to_set:
                    to_set[attr_name] = []
                if attr_type == NodeParameterType.INT.value:
                    to_set[attr_name].append(context.param_value(f'livalue_{i}'))
                elif attr_type == NodeParameterType.FLOAT.value:
                    to_set[attr_name].append(context.param_value(f'lfvalue_{i}'))
                elif attr_type == NodeParameterType.STRING.value:
                    to_set[attr_name].append(context.param_value(f'lsvalue_{i}'))
                else:
                    raise NotImplementedError()
            elif op_type == 201:  # pop
                if attr_name in to_set:
                    to_set[attr_name].pop()
            elif op_type == 202:  # slice
                if attr_name not in to_set:
                    to_set[attr_name] = []
                else:
                    start = context.param_value(f'lsl0value_{i}')
                    end = context.param_value(f'lsl1value_{i}')
                    step = context.param_value(f'lsl2value_{i}')
                    to_set[attr_name] = to_set[attr_name][start:end:step]
            else:
                raise NotImplementedError()

        for attr_name, attr_val in to_set.items():
            res.set_attribute(attr_name, attr_val)
        return res

    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()
