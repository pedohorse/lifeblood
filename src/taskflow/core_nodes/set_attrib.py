from taskflow.basenode import BaseNode
from taskflow.nodethings import ProcessingResult
from taskflow.taskspawn import TaskSpawn
from taskflow.exceptions import NodeNotReadyToProcess
from taskflow.enums import NodeParameterType
from taskflow.uidata import NodeUi, MultiGroupLayout, Parameter

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

    def __init__(self, name: str):
        super(SetAttributes, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            with ui.multigroup_parameter_block():
                with ui.parameters_on_same_line_block():
                    ui.add_parameter('name', 'Name', NodeParameterType.STRING, '')
                    type_param = ui.add_parameter('type', '', NodeParameterType.INT, 0)
                    type_param.add_menu((('int', NodeParameterType.INT.value),
                                         ('bool', NodeParameterType.BOOL.value),
                                         ('float', NodeParameterType.FLOAT.value),
                                         ('string', NodeParameterType.STRING.value)))

                    ui.add_parameter('svalue', 'val', NodeParameterType.STRING, '').add_visibility_condition(type_param, '==', NodeParameterType.STRING.value)
                    ui.add_parameter('ivalue', 'val', NodeParameterType.INT, 0).add_visibility_condition(type_param, '==', NodeParameterType.INT.value)
                    ui.add_parameter('fvalue', 'val', NodeParameterType.FLOAT, 0.0).add_visibility_condition(type_param, '==', NodeParameterType.FLOAT.value)
                    ui.add_parameter('bvalue', 'val', NodeParameterType.BOOL, False).add_visibility_condition(type_param, '==', NodeParameterType.BOOL.value)
        multiblock = list(ui.items())[-1]
        assert isinstance(multiblock, MultiGroupLayout)  # TODO: find a better workflow of getting that last layout added
        multiblock.add_template_instance()
        multiblock.add_template_instance()

        # def _printone(level, i=0):
        #     if isinstance(level, Parameter):
        #         print('\t'*(i-1), f'====parameter {level.name()}')
        #         return
        #     for item in level.items(recursive=False):
        #         print('\t'*i, item)
        #         _printone(item, i+1)

        _printone(ui.main_parameter_layout())

    def process_task(self, task_dict) -> ProcessingResult:
        attr_name = self.param_value('name')
        attr_type = self.param_value('type')
        if attr_type == NodeParameterType.INT.value:
            attr_val = self.param_value('ivalue')
        elif attr_type == NodeParameterType.BOOL.value:
            attr_val = self.param_value('bvalue')
        elif attr_type == NodeParameterType.FLOAT.value:
            attr_val = self.param_value('fvalue')
        elif attr_type == NodeParameterType.STRING.value:
            attr_val = self.param_value('svalue')
        else:
            raise NotImplementedError('unexpected type')
        res = ProcessingResult()
        res.set_attribute(attr_name, attr_val)
        return res

    def postprocess_task(self, task_dict) -> ProcessingResult:
        return ProcessingResult()
