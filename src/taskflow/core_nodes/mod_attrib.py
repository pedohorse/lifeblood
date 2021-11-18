from taskflow.basenode import BaseNode
from taskflow.nodethings import ProcessingResult
from taskflow.taskspawn import TaskSpawn
from taskflow.exceptions import NodeNotReadyToProcess
from taskflow.enums import NodeParameterType
from taskflow.uidata import NodeUi, MultiGroupLayout, Parameter
from taskflow.node_visualization_classes import NodeColorScheme

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
                               ('string', NodeParameterType.STRING)))\
                    .append_visibility_condition(optype_param, '>=', 200)
                ui.add_parameter('lfvalue', 'val', NodeParameterType.FLOAT, 0.0) \
                    .append_visibility_condition(optype_param, '>=', 200)\
                    .append_visibility_condition(listvaltype_param, '==', NodeParameterType.FLOAT.value)
                ui.add_parameter('livalue', 'val', NodeParameterType.INT, 0) \
                    .append_visibility_condition(optype_param, '>=', 200)\
                    .append_visibility_condition(listvaltype_param, '==', NodeParameterType.INT.value)
                ui.add_parameter('lsvalue', 'val', NodeParameterType.STRING, '') \
                    .append_visibility_condition(optype_param, '>=', 200)\
                    .append_visibility_condition(listvaltype_param, '==', NodeParameterType.STRING.value)

    def process_task(self, context) -> ProcessingResult:
        attr_count = context.param_value('attr_count')
        res = ProcessingResult()
        for i in range(attr_count):
            attrs = context.task_attributes()
            attr_name = context.param_value(f'name_{i}')
            if attr_name == '':  # however some spaces - is a valid parameter name
                continue
            op_type = context.param_value(f'optype_{i}')
            if op_type < 100:
                attr_type = context.param_value(f'valtype_{i}')
                if attr_type == NodeParameterType.INT.value:
                    attr_val = context.param_value(f'ivalue_{i}')
                    # remember - sets should be independent, cuz if u do them dependant on the order -
                    #  the expression eval will still be independent and it's gonna be a confusing mess

                elif attr_type == NodeParameterType.FLOAT.value:
                    attr_val = context.param_value(f'fvalue_{i}')
            else:
                attr_type = context.param_value(f'lvaltype_{i}')
                if attr_type == NodeParameterType.INT.value:
                    attr_val
            res.set_attribute(attr_name, attr_val)
        return res

    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()
