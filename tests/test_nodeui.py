from unittest import TestCase
from lifeblood import basenode
from lifeblood.uidata import NodeParameterType, ParameterNameCollisionError

from typing import Iterable

from lifeblood.nodethings import ProcessingResult


class ParamNode(basenode.BaseNode):
    @classmethod
    def tags(cls) -> Iterable[str]:
        return ()

    def postprocess_task(self, task_dict) -> ProcessingResult:
        return ProcessingResult()

    def process_task(self, task_dict) -> ProcessingResult:
        return ProcessingResult()

    @classmethod
    def label(cls) -> str:
        return 'bleh'


class GoodParamNode1(ParamNode):
    def __init__(self, name):
        super(GoodParamNode1, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('abcd', None, param_type=NodeParameterType.STRING, param_val='13qe')
            with ui.multigroup_parameter_block('vigo'):
                ui.add_parameter('efgh', None, param_type=NodeParameterType.BOOL, param_val=True)
                ui.add_parameter('ijkl', None, param_type=NodeParameterType.INT, param_val=-2)
            ui.add_parameter('mnop', None, param_type=NodeParameterType.FLOAT, param_val=9.1)
        ui.parameter('vigo').set_value(1)


class GoodParamNode2(ParamNode):
    def __init__(self, name):
        super(GoodParamNode2, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('abcd', None, param_type=NodeParameterType.STRING, param_val='13qe')
            with ui.multigroup_parameter_block('vigo'):
                ui.add_parameter('efgh', None, param_type=NodeParameterType.BOOL, param_val=True)
                ui.add_parameter('ijkl', None, param_type=NodeParameterType.INT, param_val=-2)
            ui.add_parameter('efgh', None, param_type=NodeParameterType.FLOAT, param_val=9.1)
        ui.parameter('vigo').set_value(1)


class BadParamNode1(ParamNode):
    def __init__(self, name):
        super(BadParamNode1, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('abcd', None, param_type=NodeParameterType.STRING, param_val='13qe')
            with ui.multigroup_parameter_block('vigo'):
                ui.add_parameter('efgh', None, param_type=NodeParameterType.BOOL, param_val=True)
                ui.add_parameter('mnop', None, param_type=NodeParameterType.INT, param_val=-2)
            ui.add_parameter('vigo', None, param_type=NodeParameterType.FLOAT, param_val=9.1)
        ui.parameter('vigo').set_value(1)


class BadParamNode2(ParamNode):
    def __init__(self, name):
        super(BadParamNode2, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('abcd', None, param_type=NodeParameterType.STRING, param_val='13qe')
            with ui.multigroup_parameter_block('vigo'):
                ui.add_parameter('efgh', None, param_type=NodeParameterType.BOOL, param_val=True)
                ui.add_parameter('ijkl', None, param_type=NodeParameterType.INT, param_val=-2)
            ui.add_parameter('ijkl_0', None, param_type=NodeParameterType.FLOAT, param_val=9.1)
        ui.parameter('vigo').set_value(2)

        print([x.name() for x in ui.parameters()])


class UniqueUiParametersCheck(TestCase):
    def runTest(self):
        self.assertRaises(ParameterNameCollisionError, BadParamNode1, 'name1')
        self.assertRaises(ParameterNameCollisionError, BadParamNode2, 'name2')
        GoodParamNode1('name1')
        GoodParamNode2('name2')
