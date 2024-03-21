from unittest import TestCase
import random
from itertools import chain
from lifeblood import basenode
from lifeblood.uidata import NodeUi, NodeParameterType, ParameterNameCollisionError, ParameterNotFound, ParameterError, \
    ParameterExpressionError, ParameterExpressionCastError, ParameterReadonly, ParameterLocked, LayoutError, \
    ParameterDefinitionError, NodeUiDefinitionError, ParameterCannotHaveExpressions

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

    @classmethod
    def type_name(cls) -> str:
        return 'debug testing type and stuff'


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


class ListParamTypesNode(ParamNode):
    def __init__(self, name):
        super(ListParamTypesNode, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('int_param', None, param_type=NodeParameterType.INT, param_val=12)
            ui.add_parameter('float_param', None, param_type=NodeParameterType.FLOAT, param_val=2.3)
            ui.add_parameter('bool_param', None, param_type=NodeParameterType.BOOL, param_val=True)
            ui.add_parameter('string_param', None, param_type=NodeParameterType.STRING, param_val='wowcat')


class LockedReadonlyParamsNode(ParamNode):
    def __init__(self, name):
        super(LockedReadonlyParamsNode, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('locked_param', None, param_type=NodeParameterType.INT, param_val=123).set_locked(True)
            ui.add_parameter('readonly_param', None, param_type=NodeParameterType.INT, param_val=321, readonly=True)
            ui.add_parameter('noexpr_param', None, param_type=NodeParameterType.INT, param_val=321, can_have_expressions=False)
            ui.add_parameter('int_param', None, param_type=NodeParameterType.INT, param_val=614)


class ParametersWithLimitsNode(ParamNode):
    def __init__(self, name):
        super(ParametersWithLimitsNode, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('int_param', None, param_type=NodeParameterType.INT, param_val=123).set_value_limits(-2, 144)
            ui.add_parameter('int_param_default_exceeds', None, param_type=NodeParameterType.INT, param_val=155).set_value_limits(-2, 144)
            ui.add_parameter('float_param', None, param_type=NodeParameterType.FLOAT, param_val=12.3).set_value_limits(-1.5, 43.2)
            ui.add_parameter('float_param_default_exceeds', None, param_type=NodeParameterType.FLOAT, param_val=-2.1).set_value_limits(-1.5, 43.2)


class NestedMultiGroupLayoutNode1(ParamNode):
    def __init__(self, name):
        super(NestedMultiGroupLayoutNode1, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            with ui.multigroup_parameter_block('outer block'):
                ui.add_parameter('outer param', None, NodeParameterType.INT, -102)
                with ui.multigroup_parameter_block('middle block'):
                    ui.add_parameter('middle param', None, NodeParameterType.INT, -12)
                    with ui.multigroup_parameter_block('inner block'):
                        ui.add_parameter('inner param', None, NodeParameterType.INT, -2)

class UniqueUiParametersCheck(TestCase):
    def test_parameter_name_collision(self):
        self.assertRaises(ParameterNameCollisionError, BadParamNode1, 'name1')
        self.assertRaises(ParameterNameCollisionError, BadParamNode2, 'name2')

    def test_basic1(self):
        p1 = GoodParamNode1('name1337')
        self.assertEqual('name1337', p1.name())
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
        for val in chain(range(0, 100), (rng.randint(100, 1000) for _ in range(8))):  # TODO: improve performance, then inc test data range
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
        node = ListParamTypesNode('lolname')

        for param, val, expr, expr_result, alt_val in ((node.param('int_param'), 23, '2+3*4', 14, -23),
                                                       (node.param('float_param'), -4.21, '2+5/10', 2.5, -9.111),
                                                       (node.param('bool_param'), True, '2==5', False, True),
                                                       (node.param('bool_param'), False, '4==4', True, False),
                                                       (node.param('string_param'), 'asdfghj', '" ".join(("wow","cat"))', 'wow cat', 'crocodile')):
            self.assertFalse(param.has_expression())
            param.set_value(val)
            self.assertEqual(val, param.value())
            param.set_expression(expr)
            self.assertTrue(param.has_expression())
            self.assertEqual(expr_result, param.value())

            # TODO: should we allow setting value under expression? for now we do de-facto
            param.set_value(alt_val)
            self.assertEqual(expr_result, param.value())

            param.remove_expression()
            self.assertFalse(param.has_expression())
            self.assertEqual(alt_val, param.value())

    def test_bad_expression(self):
        node = ListParamTypesNode('lolname')

        node.param('int_param').set_expression('bad+wolf')
        self.assertRaises(ParameterExpressionError, node.param('int_param').value)

    def test_expression_bad_return_type_castable(self):
        node = ListParamTypesNode('lolname')
        for param, expr, casted_val in ((node.param('int_param'), '23.45+12.34+3.4', 39),
                                        (node.param('int_param'), 'True or False', 1),
                                        (node.param('int_param'), '"-2"+"4"', -24),
                                        (node.param('float_param'), '4-10', -6.0),
                                        (node.param('float_param'), 'False and False or True', 1.0),
                                        (node.param('float_param'), '"12"+ "." + "33"', 12.33),
                                        (node.param('bool_param'), '10-10', False),
                                        (node.param('bool_param'), '10-11', True),
                                        (node.param('bool_param'), '4.5-4', True),
                                        (node.param('bool_param'), '44.66/2-22.33', False),
                                        (node.param('bool_param'), 'True', True),
                                        (node.param('bool_param'), 'False', False),
                                        (node.param('string_param'), '3+12*2', '27'),
                                        (node.param('string_param'), '5.5*3', '16.5'),
                                        (node.param('string_param'), 'True and 15==(10+5)', 'True'),):
            param.set_expression(expr)
            self.assertEqual(casted_val, param.value())

    def test_expression_bad_return_type_uncastable(self):
        node = ListParamTypesNode('lolname')
        for param, expr in ((node.param('int_param'), '"off"+"woof"'),
                            (node.param('int_param'), '"2.3"'),
                            (node.param('float_param'), '"oof"')):
            param.set_expression(expr)
            self.assertRaises(ParameterExpressionCastError, param.value)

    def test_expression_non_expressionable_parameters(self):
        node = LockedReadonlyParamsNode('ohno')
        self.assertRaises(ParameterCannotHaveExpressions, node.param('noexpr_param').set_expression, '1+2+3')

    def test_expression_multiparam_value(self):
        # currently multiparam cannot have expressions. this may change in the future
        node = GoodParamNode1('woof')
        self.assertRaises(ParameterCannotHaveExpressions, node.param('vigo').set_expression, '1+3')

    def test_string_expansion(self):
        node = ListParamTypesNode('lename')
        node.param('string_param').set_value('cat `".".join(("dog", "bark"))` and sleeps')
        self.assertEqual('cat dog.bark and sleeps', node.param('string_param').value())

        node.param('string_param').set_value('cat')
        self.assertEqual('cat', node.param('string_param').value())

        node.param('string_param').set_value('`".".join(("dog", "bark"))`')
        self.assertEqual('dog.bark', node.param('string_param').value())

        # bad expr
        node.param('string_param').set_value('`".".join(("dog, "bark"))`')
        self.assertRaises(ParameterExpressionError, node.param('string_param').value)

    def test_string_expansion_escape_backtick(self):
        node = ListParamTypesNode('lename')
        node.param('string_param').set_value(r'cat `".".join(("dog", "ba\`rk"))` and sleeps')
        self.assertEqual('cat dog.ba`rk and sleeps', node.param('string_param').value())

        node.param('string_param').set_value(r'cat `".".join(("dog", "ba\`rk"))` and sleeps')
        self.assertEqual('cat dog.ba`rk and sleeps', node.param('string_param').value())

        node.param('string_param').set_value(r'cat `".".join(("dog", "bark"))`\` and sleeps')
        self.assertEqual('cat dog.bark` and sleeps', node.param('string_param').value())

        node.param('string_param').set_value(r'cat \``".".join(("dog", "bark"))` and sleeps')
        self.assertEqual('cat `dog.bark and sleeps', node.param('string_param').value())

        node.param('string_param').set_value(r'cat \``".".join(("dog", "bark"))`\` and sleeps')
        self.assertEqual('cat `dog.bark` and sleeps', node.param('string_param').value())

        node.param('string_param').set_value(r'c\`at `".".join(("dog", "ba\`rk"))` and sleeps')
        self.assertEqual('c`at dog.ba`rk and sleeps', node.param('string_param').value())
        node.param('string_param').set_value(r'cat `".".join(("dog", "ba\`rk"))` and sl\`eeps')
        self.assertEqual('cat dog.ba`rk and sl`eeps', node.param('string_param').value())
        node.param('string_param').set_value(r'cat `".".join(("dog", "bark"))` and sl\`eeps')
        self.assertEqual('cat dog.bark and sl`eeps', node.param('string_param').value())

        # but \` created as result of expansion must stay
        node.param('string_param').set_value(r'cat `".".join(("dog", "\\" +"\`", "bark"))` and sleeps')
        self.assertEqual(r'cat dog.\`.bark and sleeps', node.param('string_param').value())

    def test_expand_to_python_convert_trivial(self):
        node = ListParamTypesNode('lename')
        val = 'sometext_`".".join(("wild","hog"))`)"('
        expected_expr = '\'sometext_\' + (".".join(("wild","hog"))) + \')"(\''

        node.param('string_param').set_value(val)
        expand_val = node.param('string_param').value()
        actual_expr = node.param('string_param').python_from_expandable_string(val)
        node.param('string_param').set_expression(actual_expr)
        expr_val = node.param('string_param').value()

        self.assertEqual(expand_val, expr_val)
        self.assertEqual(expected_expr, actual_expr)

    def test_expand_to_python_convert(self):
        for expandable_string in (
                'qwerty',
                '`1+3`',
                r'\`-`55*3` ',
                '`1 if False else 33`ass',
                r'a"e`"bla\`b"+"wab"`""" \" "\'',
                '`"vi" + "va"` la `re.sub("qwe", "vol", "qweution")`!!'):
            node = ListParamTypesNode('lename')
            param = node.param('string_param')
            param.set_value(expandable_string)
            expand_val = param.value()
            expr = param.python_from_expandable_string(expandable_string)
            param.set_value('')  # just in case
            param.set_expression(expr)
            self.assertTrue(param.has_expression())
            expr_val = param.value()
            self.assertEqual(expand_val, expr_val)

    def test_expand_to_python_convert_bad(self):
        for expandable_string, expected_expr in (
                ('`1+3', "'`1+3'"),
                ('`-`55*3` ', "\"\" + '55*3` '"),
                ('test `a + b` tset', "'test ' + (a + b) + ' tset'"),  # non syntax errors are allowed
                ('`1 if flaso else 33``1.2.3.`ass', "(1 if flaso else 33) + \"\" + 'ass'"),
                (r'a"e`""blab"+"wab"`""" \" "\'', "'a\"e' + \"\" + '\"\"\" \\\\\" \"\\\\\\''"),
                ('q`1+-=2` fe `1lofa`!', "'q' + \"\" + ' fe ' + \"\" + '!'")):
            node = ListParamTypesNode('lename')
            param = node.param('string_param')
            expr = param.python_from_expandable_string(expandable_string)
            self.assertEqual(expected_expr, expr)

    def test_limits(self):
        node = ParametersWithLimitsNode('iamnode')
        self.assertEqual(123, node.param('int_param').value())
        node.param('int_param').set_value(555)
        self.assertEqual(144, node.param('int_param').value())
        node.param('int_param').set_value(-3)
        self.assertEqual(-2, node.param('int_param').value())

        self.assertEqual(144, node.param('int_param_default_exceeds').value())

        self.assertEqual(12.3, node.param('float_param').value())
        node.param('float_param').set_value(55.5)
        self.assertEqual(43.2, node.param('float_param').value())
        node.param('float_param').set_value(-1.7)
        self.assertEqual(-1.5, node.param('float_param').value())

        self.assertEqual(-1.5, node.param('float_param_default_exceeds').value())

    def test_limits_str(self):
        class BadParametersWithLimitsNode1(ParamNode):
            def __init__(self, name):
                super(BadParametersWithLimitsNode1, self).__init__(name)
                ui = self.get_ui()
                with ui.initializing_interface_lock():
                    ui.add_parameter('string_param', None, param_type=NodeParameterType.STRING, param_val='boo').set_value_limits('a', 'z')

        class BadParametersWithLimitsNode2(ParamNode):
            def __init__(self, name):
                super(BadParametersWithLimitsNode2, self).__init__(name)
                ui = self.get_ui()
                with ui.initializing_interface_lock():
                    ui.add_parameter('string_param', None, param_type=NodeParameterType.STRING, param_val='boo').set_value_limits('a', 'z')

        self.assertRaises(ParameterDefinitionError, BadParametersWithLimitsNode1, 'succ')
        self.assertRaises(ParameterDefinitionError, BadParametersWithLimitsNode2, 'succ')

    def test_expression_limits(self):
        node = ParametersWithLimitsNode('iamnode')
        node.param('int_param').set_expression('123+234')
        self.assertEqual(144, node.param('int_param').value())
        node.param('int_param').set_expression('-12-23')
        self.assertEqual(-2, node.param('int_param').value())

        node.param('float_param').set_expression('12.3+234')
        self.assertEqual(43.2, node.param('float_param').value())
        node.param('float_param').set_expression('-12-2.3')
        self.assertEqual(-1.5, node.param('float_param').value())

    def test_readonly(self):
        node = LockedReadonlyParamsNode('ohno')
        self.assertEqual(321, node.param('readonly_param').value())
        self.assertRaises(ParameterReadonly, node.param('readonly_param').set_value, 666)
        self.assertRaises(ParameterReadonly, node.param('readonly_param').set_expression, '1+2')
        node.param('readonly_param').set_locked(True)
        # after locking still should raise ParameterReadonly, not ParameterLocked
        self.assertRaises(ParameterReadonly, node.param('readonly_param').set_value, 666)
        self.assertRaises(ParameterReadonly, node.param('readonly_param').set_expression, '1+2')

    def test_locked(self):
        node = LockedReadonlyParamsNode('ohno')
        for _ in range(5):
            self.assertEqual(123, node.param('locked_param').value())
            self.assertRaises(ParameterLocked, node.param('locked_param').set_value, 666)
            self.assertRaises(ParameterLocked, node.param('locked_param').set_expression, '1+2+3')
            self.assertEqual(123, node.param('locked_param').value())
            node.param('locked_param').set_locked(False)
            node.param('locked_param').set_value(666)
            self.assertEqual(666, node.param('locked_param').value())
            node.param('locked_param').set_locked(True)

            self.assertEqual(666, node.param('locked_param').value())
            self.assertRaises(ParameterLocked, node.param('locked_param').set_value, 123)
            self.assertRaises(ParameterLocked, node.param('locked_param').set_expression, '1+2+3')
            self.assertEqual(666, node.param('locked_param').value())
            node.param('locked_param').set_locked(False)
            node.param('locked_param').set_value(123)
            self.assertEqual(123, node.param('locked_param').value())
            node.param('locked_param').set_locked(True)

    def test_add_parameter_outside_of_lock(self):
        node = LockedReadonlyParamsNode('ohno')
        ui = node.get_ui()
        self.assertRaises(NodeUiDefinitionError, ui.add_parameter, 'woofffffff', None, NodeParameterType.INT, -1)

        # TODO: should methods like set_value_limits, add_menu also require lock block?
        # self.assertRaises(NodeUiDefinitionError, node.param('int_param').set_value_limits, -9999, 9999)

    def test_serialization(self):
        node1 = GoodParamNode1('src')
        node1ui = node1.get_ui()
        node2ui = NodeUi.deserialize(node1ui.serialize())  # type: NodeUi

        self.assertSetEqual(set(x.name() for x in node1ui.parameters()), set(x.name() for x in node2ui.parameters()))

        for param1 in node1ui.parameters():
            param2 = node2ui.parameter(param1.name())
            self.assertEqual(param1.value(), param2.value())
            self.assertEqual(param1.is_locked(), param2.is_locked())
            self.assertEqual(param1.is_readonly(), param2.is_readonly())
            self.assertEqual(param1.can_have_expressions(), param2.can_have_expressions())
            self.assertEqual(param1.value_limits(), param2.value_limits())
            self.assertEqual(param1.expression(), param2.expression())

    def test_parameter_change_callback(self):
        class NodeWithParamCallbacks(ParamNode):
            def __init__(self, name):
                super(NodeWithParamCallbacks, self).__init__(name)
                self.callbacks = []
                ui = self.get_ui()
                with ui.initializing_interface_lock():
                    ui.add_parameter('int_param', None, NodeParameterType.INT, 12)
                    with ui.multigroup_parameter_block('fooface'):
                        ui.add_parameter('inblock', None, NodeParameterType.STRING, 'bar')

            def _ui_changed(self, definition_changed=False):
                print(f'callback! definition:{definition_changed}')
                self.callbacks.append(definition_changed)

        node = NodeWithParamCallbacks('foo')
        self.assertEqual(0, len(node.callbacks))
        node.param('int_param').set_value(123)
        self.assertEqual(1, len(node.callbacks))
        self.assertEqual(False, node.callbacks[-1])
        ui = node.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('float_param', None, NodeParameterType.FLOAT, 3.45)
        self.assertEqual(1, len(node.callbacks))

        node.param('float_param').set_value(4.56)
        self.assertEqual(2, len(node.callbacks))
        self.assertEqual(False, node.callbacks[-1])

        # note - changing multiparam value currently causes 1 value callback,
        #  but there is a TODO: to change this behaviour: https://trello.com/c/BxwJM9W5/314-nodeui-currently-childadded-childabouttoberemoved-do-not-cause-any-event-propagation-like-definitionchanged-but-i-guess-it-shoul
        node.param('fooface').set_value(2)
        self.assertEqual(3, len(node.callbacks))
        self.assertEqual(False, node.callbacks[-1])

        node.param('inblock_1').set_value('vlad')
        self.assertEqual(4, len(node.callbacks))
        self.assertEqual(False, node.callbacks[-1])

        node.param('float_param').set_expression('fee')
        self.assertEqual(5, len(node.callbacks))
        self.assertEqual(True, node.callbacks[-1])

        node.param('float_param').set_locked(True)
        self.assertEqual(6, len(node.callbacks))
        self.assertEqual(True, node.callbacks[-1])

    # nothing triggers layout change callback outside of initialization currently
    # def test_layout_change_callback(self):
    #     raise NotImplementedError()

    def test_visibility_condition_triggers_appearance_callback(self):
        class NodeWithVisCondCallbacks(ParamNode):
            def __init__(self, name):
                super(NodeWithVisCondCallbacks, self).__init__(name)
                self.callbacks = []
                ui = self.get_ui()
                with ui.initializing_interface_lock():
                    p1 = ui.add_parameter('int_param2', None, NodeParameterType.INT, 12)
                    p2 = ui.add_parameter('int_param3', None, NodeParameterType.INT, 55)
                    ui.add_parameter('int_param', None, NodeParameterType.INT, 12).append_visibility_condition(p1, '==', 5) \
                                                                                  .append_visibility_condition(p2, '<', 30)

        node = NodeWithVisCondCallbacks('foo')
        self.assertFalse(node.param('int_param').visible())
        node.param('int_param2').set_value(5)
        self.assertFalse(node.param('int_param').visible())
        node.param('int_param3').set_value(25)
        self.assertTrue(node.param('int_param').visible())
        node.param('int_param3').set_value(29)
        self.assertTrue(node.param('int_param').visible())
        node.param('int_param3').set_value(31)
        self.assertFalse(node.param('int_param').visible())
        node.param('int_param3').set_value(1)
        self.assertTrue(node.param('int_param').visible())
        node.param('int_param2').set_value(6)
        self.assertFalse(node.param('int_param').visible())

    def test_bad_multigroup_visibility_reference(self):
        class NodeWithBadVisReference(ParamNode):
            def __init__(self, name):
                super(NodeWithBadVisReference, self).__init__(name)
                ui = self.get_ui()
                with ui.initializing_interface_lock():
                    p1 = ui.add_parameter('outer param', None, NodeParameterType.STRING, 'wow')
                    with ui.multigroup_parameter_block('count'):
                        p2 = ui.add_parameter('inner param', None, NodeParameterType.FLOAT, -1.2)
                        p2.append_visibility_condition(p1, '==', 'wow')

        self.assertRaises(AssertionError, NodeWithBadVisReference, 'oof')  # a bit general... we expect specifically "p2.append_visi..." line to raise
        # and this next check just can never occur currently
        #self.assertRaises(ParameterDefinitionError, node.param('count').set_value, 1)

    def test_nested_multiparameters1(self):
        node = NestedMultiGroupLayoutNode1('foo')

        node.param('outer block').set_value(1)
        self.assertEqual(-102, node.param('outer param_0').value())
        node.param('outer param_0').set_value(-103)
        self.assertEqual(-103, node.param('outer param_0').value())
        self.assertEqual(0, node.param('middle block_0').value())

        node.param('middle block_0').set_value(1)
        self.assertEqual(-12, node.param('middle param_0.0').value())
        node.param('middle param_0.0').set_value(-13)
        self.assertEqual(-13, node.param('middle param_0.0').value())
        self.assertEqual(0, node.param('inner block_0.0').value())

        node.param('inner block_0.0').set_value(1)
        self.assertEqual(-2, node.param('inner param_0.0.0').value())
        node.param('inner param_0.0.0').set_value(-3)
        self.assertEqual(-3, node.param('inner param_0.0.0').value())

    def test_nested_multiparameters2(self):
        node = NestedMultiGroupLayoutNode1('foo')

        node.param('outer block').set_value(1)
        node.param('middle block_0').set_value(1)
        node.param('inner block_0.0').set_value(1)

        node.param('outer param_0').set_value(123)
        node.param('middle param_0.0').set_value(1234)
        node.param('inner param_0.0.0').set_value(12345)

        node.param('inner block_0.0').set_value(2)
        node.param('middle block_0').set_value(3)
        node.param('outer block').set_value(4)
        node.param('middle block_2').set_value(3)
        node.param('inner block_2.2').set_value(2)
        node.param('inner block_2.1').set_value(2)
        node.param('inner block_2.0').set_value(2)
        node.param('middle block_1').set_value(3)
        node.param('inner block_1.0').set_value(2)
        node.param('inner block_1.1').set_value(2)
        node.param('inner block_1.2').set_value(2)
        node.param('inner block_0.1').set_value(2)
        node.param('inner block_0.2').set_value(2)
        node.param('middle block_3').set_value(3)
        node.param('inner block_3.2').set_value(2)
        node.param('inner block_3.1').set_value(2)
        node.param('inner block_3.0').set_value(2)

        self.assertEqual(123, node.param('outer param_0').value())
        self.assertEqual(1234, node.param('middle param_0.0').value())
        self.assertEqual(12345, node.param('inner param_0.0.0').value())

        node.param('outer param_3').set_value(234)
        node.param('middle param_2.2').set_value(2345)
        node.param('inner param_1.0.1').set_value(23456)

        self.assertEqual(234, node.param('outer param_3').value())
        self.assertEqual(2345, node.param('middle param_2.2').value())
        self.assertEqual(23456, node.param('inner param_1.0.1').value())

        rng = random.Random(666131313)
        for _ in range(1000):
            outer_i = rng.randint(0, 3)
            middle_i = rng.randint(0, 2)
            inner_i = rng.randint(0, 1)
            outer_v = rng.randint(-9999, 9999)
            middle_v = rng.randint(-9999, 9999)
            inner_v = rng.randint(-9999, 9999)
            node.param(f'outer param_{outer_i}').set_value(outer_v)
            node.param(f'middle param_{outer_i}.{middle_i}').set_value(middle_v)
            node.param(f'inner param_{outer_i}.{middle_i}.{inner_i}').set_value(inner_v)

            self.assertEqual(outer_v, node.param(f'outer param_{outer_i}').value())
            self.assertEqual(middle_v, node.param(f'middle param_{outer_i}.{middle_i}').value())
            self.assertEqual(inner_v, node.param(f'inner param_{outer_i}.{middle_i}.{inner_i}').value())
