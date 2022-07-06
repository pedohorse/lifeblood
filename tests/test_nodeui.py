from unittest import TestCase
import random
from itertools import chain
from lifeblood import basenode
from lifeblood.uidata import NodeParameterType, ParameterNameCollisionError, ParameterNotFound, ParameterError

from typing import Iterable

from lifeblood.nodethings import ProcessingResult


def get_random_unicode(rng, length):
    """
    thanks to https://stackoverflow.com/questions/1477294/generate-random-utf-8-string-in-python
    """
    # Update this to include code point ranges to be sampled
    include_ranges = [
        (0x0021, 0x0021),
        (0x0023, 0x0026),
        (0x0028, 0x007E),
        (0x00A1, 0x00AC),
        (0x00AE, 0x00FF),
        (0x0100, 0x017F),
        (0x0180, 0x024F),
        (0x2C60, 0x2C7F),
        (0x16A0, 0x16F0),
        (0x0370, 0x0377),
        (0x037A, 0x037E),
        (0x0384, 0x038A),
        (0x038C, 0x038C),
        (0x0400, 0x04FF),
    ]
    alphabet = [
        chr(code_point) for current_range in include_ranges
        for code_point in range(current_range[0], current_range[1] + 1)
    ]
    return ''.join(rng.choice(alphabet) for _ in range(length))


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
    def test_parameter_name_collision(self):
        self.assertRaises(ParameterNameCollisionError, BadParamNode1, 'name1')
        self.assertRaises(ParameterNameCollisionError, BadParamNode2, 'name2')

    def test_basic1(self):
        p1 = GoodParamNode1('name1')
        self.assertEqual('13qe', p1.param('abcd').value())
        self.assertEqual(0, p1.param('vigo').value())
        self.assertRaises(ParameterNotFound, p1.param, 'efgh')
        self.assertRaises(ParameterNotFound, p1.param, 'ijkl')
        self.assertRaises(ParameterNotFound, p1.param, 'efgh_0')
        self.assertRaises(ParameterNotFound, p1.param, 'ijkl_0')
        self.assertRaises(ParameterNotFound, p1.param, 'efgh_1')
        self.assertRaises(ParameterNotFound, p1.param, 'ijkl_1')
        self.assertRaises(ParameterNotFound, p1.param, 'efgh_2')
        self.assertRaises(ParameterNotFound, p1.param, 'ijkl_2')
        self.assertEqual(9.1, p1.param('mnop').value())

        rng = random.Random(666)
        p1.param('abcd').set_value('bobo baba')
        self.assertEqual('bobo baba', p1.param('abcd').value())
        p1.param('abcd').set_value('')
        self.assertEqual('', p1.param('abcd').value())
        for _ in range(100):
            val = get_random_unicode(rng, rng.randint(0, 2**17))
            val = val.replace('`', ' ')
            p1.param('abcd').set_value(val)
            self.assertEqual(val, p1.param('abcd').value())

        p1.param('mnop').set_value(0)
        self.assertEqual(0.0, p1.param('mnop').value())
        for _ in range(1000):
            val = rng.uniform(-1e32, 1e32)
            p1.param('mnop').set_value(val)
            self.assertEqual(val, p1.param('mnop').value())

    def test_set_value_casting(self):
        p1 = GoodParamNode2('name2')
        testvals = ((1, '1'),
                    (2.3, '2.3'),
                    (True, 'True'),
                    ('wow cat', 'wow cat'))
        for val, expected_val in testvals:
            p1.param('abcd').set_value(val)
            self.assertEqual(expected_val, p1.param('abcd').value())

        testvals = ((True, True),
                    (False, False),
                    ('', False),
                    ('ass', True),
                    (0, False),
                    (-9, True),
                    (91, True),
                    (0.0, False),
                    (12.45, True))
        for val, expected_val in testvals:
            p1.param('efgh_0').set_value(val)
            self.assertEqual(expected_val, p1.param('efgh_0').value())

        testvals = ((1.234, 1.234),
                    ('1.414', 1.414),
                    ('5', 5.0),
                    (True, 1.0),
                    (False, 0.0))
        for val, expected_val in testvals:
            p1.param('efgh').set_value(val)
            self.assertEqual(expected_val, p1.param('efgh').value())

        testvals = ((1.234, 1),
                    ('13', 13),
                    (True, 1),
                    (False, 0))
        for val, expected_val in testvals:
            p1.param('ijkl_0').set_value(val)
            self.assertEqual(expected_val, p1.param('ijkl_0').value())

    def test_multiparm(self):
        node = GoodParamNode2('name2')
        rng = random.Random(9173712)

        # big multiparam value changes are slow currently, test takes a long time
        for val in chain(range(0, 100), (rng.randint(100, 1000) for _ in range(50))):
            node.param('vigo').set_value(val)
            for i in range(val):
                self.assertIsNotNone(node.param(f'efgh_{i}'))
                self.assertIsNotNone(node.param(f'ijkl_{i}'))
                self.assertEqual(True, node.param(f'efgh_{i}').value())
                self.assertEqual(-2, node.param(f'ijkl_{i}').value())
            for i in range(val, val + 5):
                self.assertRaises(ParameterNotFound, node.param, f'efgh_{i}')
                self.assertRaises(ParameterNotFound, node.param, f'ijkl_{i}')


    def test_expression_set_remove(self):
        raise NotImplementedError()

    def test_bad_expression(self):
        raise NotImplementedError()

    def test_expression_bad_return_type_castable(self):
        raise NotImplementedError()

    def test_expression_bad_return_type_uncastable(self):
        raise NotImplementedError()

    def test_expression_non_expressionable_parameters(self):
        raise NotImplementedError()

    def test_expression_multiparam_value(self):
        raise NotImplementedError()

    def test_string_expansion(self):
        raise NotImplementedError()

    def test_string_expansion_escape_backtick(self):
        raise NotImplementedError()

    def test_limits(self):
        raise NotImplementedError()

    def test_limits_str(self):
        raise NotImplementedError()

    def test_expression_limits(self):
        raise NotImplementedError()

    def test_readonly(self):
        raise NotImplementedError()

    def test_locked(self):
        raise NotImplementedError()

    def test_add_parameter_outside_of_lock(self):
        raise NotImplementedError()

    def test_serialization(self):
        raise NotImplementedError()

    def test_parameter_change_callback(self):
        raise NotImplementedError()

    def test_layout_change_callback(self):
        raise NotImplementedError()

    def test_visibility_condition_triggers_appearance_callback(self):
        raise NotImplementedError()
