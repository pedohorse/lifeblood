from dataclasses import dataclass
import os
import pathlib
import math
from copy import deepcopy
from .enums import NodeParameterType
from .expression_locals_provider_base import ExpressionLocalsProviderBase
import re

from typing import Dict, Any, List, Set, Optional, Tuple, Union, Iterable, FrozenSet, Type


class ParameterExpressionError(Exception):
    def __init__(self, inner_exception):
        self.__inner_exception = inner_exception

    def __str__(self):
        return f'ParameterExpressionError: {str(self.__inner_exception)}'

    def inner_expection(self):
        return self.__inner_exception


class ParameterExpressionCastError(ParameterExpressionError):
    """
    represents error with type casting of the expression result
    """
    pass


class LayoutError(RuntimeError):
    pass


class LayoutReadonlyError(LayoutError):
    pass


class ParameterHierarchyItem:
    def __init__(self):
        self.__parent: Optional["ParameterHierarchyItem"] = None
        self.__children: Set["ParameterHierarchyItem"] = set()

    def parent(self) -> Optional["ParameterHierarchyItem"]:
        return self.__parent

    def set_parent(self, item: Optional["ParameterHierarchyItem"]):
        if self.__parent == item:
            return
        if self.__parent is not None:
            assert self in self.__parent.__children
            self.__parent._child_about_to_be_removed(self)
            self.__parent.__children.remove(self)
        self.__parent = item
        if self.__parent is not None:
            self.__parent.__children.add(self)
            self.__parent._child_added(self)

    def _child_about_to_be_removed(self, child: "ParameterHierarchyItem"):
        """
        callback for just before a child is removed
        :param child:
        :return:
        """
        pass

    def _child_added(self, child: "ParameterHierarchyItem"):
        """
        callback for just after child is added
        :param child:
        :return:
        """
        pass

    def children(self) -> FrozenSet["ParameterHierarchyItem"]:
        return frozenset(self.__children)

    def _children_definition_changed(self, children: Iterable["ParameterHierarchyItem"]):
        if self.__parent is not None:
            self.__parent._children_definition_changed([self])

    def _children_appearance_changed(self, children: Iterable["ParameterHierarchyItem"]):
        if self.__parent is not None:
            self.__parent._children_appearance_changed([self])

    def _children_value_changed(self, children: Iterable["ParameterHierarchyItem"]):
        if self.__parent is not None:
            self.__parent._children_value_changed([self])

    def visible(self) -> bool:
        return False


class ParameterHierarchyLeaf(ParameterHierarchyItem):
    def _children_definition_changed(self, children: Iterable["ParameterHierarchyItem"]):
        return

    def _children_value_changed(self, children: Iterable["ParameterHierarchyItem"]):
        return

    def _children_appearance_changed(self, children: Iterable["ParameterHierarchyItem"]):
        return

    def _child_added(self, child: "ParameterHierarchyItem"):
        raise RuntimeError('cannot add children to ParameterHierarchyLeaf')

    def _child_about_to_be_removed(self, child: "ParameterHierarchyItem"):
        raise RuntimeError('cannot remove children from ParameterHierarchyLeaf')


def evaluate_expression(expression, context: Optional[ExpressionLocalsProviderBase]):
    try:
        return eval(expression,
                    {'os': os, 're': re, 'pathlib': pathlib, 'Path': pathlib.Path, **{k: getattr(math, k) for k in dir(math) if not k.startswith('_')}},
                    context.locals() if context is not None else {})
    except Exception as e:
        raise ParameterExpressionError(e) from None


class Separator(ParameterHierarchyLeaf):
    pass


class Parameter(ParameterHierarchyLeaf):
    __re_expand_pattern = None
    __re_escape_backticks_pattern = None

    class DontChange:
        pass

    def __init__(self, param_name: str, param_label: Optional[str], param_type: NodeParameterType, param_val: Any, can_have_expression: bool = True, readonly: bool = False, default_value=None):
        super(Parameter, self).__init__()
        self.__name = param_name
        self.__label = param_label
        self.__type = param_type
        self.__value = None
        self.__menu_items: Optional[Dict[str, str]] = None
        self.__menu_items_order: List[str] = []
        self.__vis_when = []
        self.__force_hidden = False
        self.__is_readonly = False  # set it False until the end of constructor
        self.__locked = False  # same as readonly, but is settable by user

        self.__expression = None
        self.__can_have_expressions = can_have_expression

        if Parameter.__re_expand_pattern is None:
            Parameter.__re_expand_pattern = re.compile(r'((?<!\\)`.*?(?<!\\)`)')  # TODO: this does NOT cover escaped slash in front of ` (or escaped escaped slash and so on)
        if Parameter.__re_escape_backticks_pattern is None:
            Parameter.__re_escape_backticks_pattern = re.compile(r'\\`')

        self.__hard_borders: Tuple[Optional[Union[int, float]], Optional[Union[int, float]]] = (None, None)
        self.__display_borders: Tuple[Optional[Union[int, float]], Optional[Union[int, float]]] = (None, None)

        self.__string_multiline = False
        self.__string_multiline_syntax_hint: Optional[str] = None

        # links
        self.__params_referencing_me: Set["Parameter"] = set()

        # caches
        self.__vis_cache = None

        assert default_value is None or type(default_value) == type(param_val)
        self.set_value(param_val)
        self.__default_value = default_value or param_val
        self.__is_readonly = readonly

    def name(self) -> str:
        return self.__name

    def _set_name(self, name: str):
        """
        this should only be called by layout classes
        """
        self.__name = name
        if self.parent() is not None:
            self.parent()._children_definition_changed([self])

    def label(self) -> Optional[str]:
        return self.__label

    def type(self) -> NodeParameterType:
        return self.__type

    def unexpanded_value(self, context: Optional[ExpressionLocalsProviderBase] = None):  # TODO: why context parameter here?
        return self.__value

    def default_value(self):
        """
        note that this value will be unexpanded

        :return:
        """
        return self.__default_value

    def value(self, context: Optional[ExpressionLocalsProviderBase] = None) -> Any:
        """
        returns value of this parameter
        :param context: optional dict like locals, for expression evaluations
        """

        if self.__expression is not None:
            result = evaluate_expression(self.__expression, context)
            # check type and cast
            try:
                if self.__type == NodeParameterType.INT:
                    result = int(result)
                elif self.__type == NodeParameterType.FLOAT:
                    result = float(result)
                elif self.__type == NodeParameterType.STRING and not isinstance(result, str):
                    result = str(result)
                elif self.__type == NodeParameterType.BOOL:
                    result = bool(result)
            except ValueError:
                raise ParameterExpressionCastError(f'could not cast {result} to {self.__type.name}') from None
            # check limits
            if self.__type in (NodeParameterType.INT, NodeParameterType.FLOAT):
                if self.__hard_borders[0] is not None and result < self.__hard_borders[0]:
                    result = self.__hard_borders[0]
                if self.__hard_borders[1] is not None and result > self.__hard_borders[1]:
                    result = self.__hard_borders[1]
            return result

        if self.__type != NodeParameterType.STRING:
            return self.__value

        # for string parameters we expand expressions in ``, kinda like bash
        parts = self.__re_expand_pattern.split(self.__value)
        for i, part in enumerate(parts):
            if part.startswith('`') and part.endswith('`'):  # expression
                parts[i] = str(evaluate_expression(self.__re_escape_backticks_pattern.sub('`', part[1:-1]), context))
            else:
                parts[i] = self.__re_escape_backticks_pattern.sub('`', part)
        return ''.join(parts)
        # return self.__re_expand_pattern.sub(lambda m: str(evaluate_expression(m.group(1), context)), self.__value)

    def set_slider_visualization(self, value_min=DontChange, value_max=DontChange):  # type: (Union[int, float], Union[int, float]) -> Parameter
        """
        set a visual slider's minimum and maximum
        this does nothing to the parameter itself, and it's up to parameter renderer to interpret this data

        :return: self to be chained
        """
        if self.__type not in (NodeParameterType.INT, NodeParameterType.FLOAT):
            raise ParameterDefinitionError('cannot set limits for parameters of types other than INT and FLOAT')

        if self.__type == NodeParameterType.INT:
            value_min = int(value_min)
        elif self.__type == NodeParameterType.FLOAT:
            value_min = float(value_min)

        if self.__type == NodeParameterType.INT:
            value_max = int(value_max)
        elif self.__type == NodeParameterType.FLOAT:
            value_max = float(value_max)

        self.__display_borders = (value_min, value_max)
        return self

    def set_value_limits(self, value_min=DontChange, value_max=DontChange):  # type: (Union[int, float, None, Type[DontChange]], Union[int, float, None, Type[DontChange]]) -> Parameter
        """
        set minimum and maximum values that parameter will enforce
        None means no limit (unset limit)

        :return: self to be chained
        """
        if self.__type not in (NodeParameterType.INT, NodeParameterType.FLOAT):
            raise ParameterDefinitionError('cannot set limits for parameters of types other than INT and FLOAT')
        if value_min == self.DontChange:
            value_min = self.__hard_borders[0]
        elif value_min is not None:
            if self.__type == NodeParameterType.INT:
                value_min = int(value_min)
            elif self.__type == NodeParameterType.FLOAT:
                value_min = float(value_min)
        if value_max == self.DontChange:
            value_max = self.__hard_borders[1]
        elif value_max is not None:
            if self.__type == NodeParameterType.INT:
                value_max = int(value_max)
            elif self.__type == NodeParameterType.FLOAT:
                value_max = float(value_max)
        assert value_min != self.DontChange
        assert value_max != self.DontChange

        self.__hard_borders = (value_min, value_max)
        if value_min is not None and self.__value < value_min:
            self.__value = value_min
        if value_max is not None and self.__value > value_max:
            self.__value = value_max
        return self

    def set_text_multiline(self, syntax_hint: Optional[str] = None):
        if self.__type != NodeParameterType.STRING:
            raise ParameterDefinitionError('multiline can be only set for string parameters')
        self.__string_multiline = True
        self.__string_multiline_syntax_hint = syntax_hint
        return self

    def is_text_multiline(self):
        return self.__string_multiline

    def syntax_hint(self) -> Optional[str]:
        """
        may hint an arbitrary string hint to the renderer
        it's up to renderer to decide what to do.
        common conception is to use language name lowercase, like: python
        None means no hint
        """
        return self.__string_multiline_syntax_hint

    def display_value_limits(self) -> Tuple[Union[int, float, None], Union[int, float, None]]:
        """
        returns a tuple of limits for display purposes.
        parameter itself ignores this totally.
        it's up to parameter renderer to interpret this info
        """
        return self.__display_borders

    def value_limits(self) -> Tuple[Union[int, float, None], Union[int, float, None]]:
        """
        returns a tuple of hard limits.
        these limits are enforced by the parameter itself
        """
        return self.__hard_borders

    def is_readonly(self):
        return self.__is_readonly

    def is_locked(self):
        return self.__locked

    def set_locked(self, locked: bool):
        if locked == self.__locked:
            return
        self.__locked = locked
        if self.parent() is not None:
            self.parent()._children_definition_changed([self])

    def set_value(self, value: Any):
        if self.__is_readonly:
            raise ParameterReadonly()
        if self.__locked:
            raise ParameterLocked()
        if self.__type == NodeParameterType.FLOAT:
            param_value = float(value)
            if self.__hard_borders[0] is not None:
                param_value = max(param_value, self.__hard_borders[0])
            if self.__hard_borders[1] is not None:
                param_value = min(param_value, self.__hard_borders[1])
        elif self.__type == NodeParameterType.INT:
            param_value = int(value)
            if self.__hard_borders[0] is not None:
                param_value = max(param_value, self.__hard_borders[0])
            if self.__hard_borders[1] is not None:
                param_value = min(param_value, self.__hard_borders[1])
        elif self.__type == NodeParameterType.BOOL:
            param_value = bool(value)
        elif self.__type == NodeParameterType.STRING:
            param_value = str(value)
        else:
            raise NotImplementedError()
        self.__value = param_value
        for other_param in self.__params_referencing_me:
            other_param._referencing_param_value_changed(self)

        if self.parent() is not None:
            self.parent()._children_value_changed([self])

    def can_have_expressions(self):
        return self.__can_have_expressions

    def has_expression(self):
        return self.__expression is not None

    def expression(self):
        return self.__expression

    def set_expression(self, expression: Union[str, None]):
        """
        sets or removes expression from a parameter
        :param expression: either expression code or None means removing expression
        :return:
        """
        if self.__is_readonly:
            raise ParameterReadonly()
        if self.__locked:
            raise ParameterLocked()
        if not self.__can_have_expressions:
            raise ParameterCannotHaveExpressions()
        if expression != self.__expression:
            self.__expression = expression
            if self.parent() is not None:
                self.parent()._children_definition_changed([self])

    def remove_expression(self):
        self.set_expression(None)

    @classmethod
    def python_from_expandable_string(cls, expandable_string, context: Optional[ExpressionLocalsProviderBase] = None) -> str:
        """
        given string value that may contain backtick expressions return python equivalent
        """
        expression_parts = []
        parts = cls.__re_expand_pattern.split(expandable_string)
        for i, part in enumerate(parts):
            if part.startswith('`') and part.endswith('`'):  # expression
                maybe_expr = f'({cls.__re_escape_backticks_pattern.sub("`", part[1:-1])})'
                try:
                    val = evaluate_expression(maybe_expr, context)
                    if not isinstance(val, str):
                        maybe_expr = f'str{maybe_expr}'  # note, maybe_expr is already enclosed in parentheses
                except ParameterExpressionError as e:
                    # we just catch syntax errors, other runtime errors are allowed as real context is set per task
                    if isinstance(e.inner_expection(), SyntaxError):
                        maybe_expr = '""'
                expression_parts.append(maybe_expr)
            else:
                val = cls.__re_escape_backticks_pattern.sub('`', part)
                if not val:
                    continue
                expression_parts.append(repr(val))

        return ' + '.join(expression_parts)

    def _referencing_param_value_changed(self, other_parameter):
        """
        when a parameter that we are referencing changes - it will report here
        :param other_parameter:
        """
        # TODO: this now only works with referencing param in visibility condition
        # TODO: butt we want general references, including from parameter expressions
        # TODO: OOOORR will i need references for expressions at all?
        # TODO: references between node bring SOOOO much pain when serializing them separately
        if self.__vis_when:
            self.__vis_cache = None
            if self.parent() is not None and isinstance(self.parent(), ParametersLayoutBase):
                self.parent()._children_appearance_changed([self])

    def set_hidden(self, hidden):
        self.__force_hidden = hidden

    def visible(self) -> bool:
        if self.__force_hidden:
            return False
        if self.__vis_cache is not None:
            return self.__vis_cache
        if self.__vis_when:
            for other_param, op, value in self.__vis_when:
                if op == '==' and other_param.value() != value \
                        or op == '!=' and other_param.value() == value \
                        or op == '>' and other_param.value() <= value \
                        or op == '>=' and other_param.value() < value \
                        or op == '<' and other_param.value() >= value \
                        or op == '<=' and other_param.value() > value \
                        or op == 'in' and other_param.value() not in value \
                        or op == 'not in' and other_param.value() in value:
                    self.__vis_cache = False
                    return False
        self.__vis_cache = True
        return True

    def _add_referencing_me(self, other_parameter: "Parameter"):
        """
        other_parameter MUST belong to the same node to avoid cross-node references
        :param other_parameter:
        :return:
        """
        assert self.has_same_parent(other_parameter), 'references MUST belong to the same node'
        self.__params_referencing_me.add(other_parameter)

    def _remove_referencing_me(self, other_parameter: "Parameter"):
        assert other_parameter in self.__params_referencing_me
        self.__params_referencing_me.remove(other_parameter)

    def references(self) -> Tuple["Parameter", ...]:
        """
        returns tuple of parameters referenced by this parameter's definition
        static/dynamic references from expressions ARE NOT INCLUDED - they are not parameter's DEFINITION
        currently the only thing that can be a reference is parameter from visibility conditions
        """
        return tuple(x[0] for x in self.__vis_when)

    def visibility_conditions(self) -> Tuple[Tuple["Parameter", str, Union[bool, int, float, str, tuple]], ...]:
        return tuple(self.__vis_when)

    def append_visibility_condition(self, other_param: "Parameter", condition: str, value: Union[bool, int, float, str, tuple]) -> "Parameter":
        """
        condition currently can only be a simplest
        :param other_param:
        :param condition:
        :param value:
        :return: self to allow easy chaining
        """
        allowed_conditions = ('==', '!=', '>=', '<=', '<', '>', 'in', 'not in')
        if condition not in allowed_conditions:
            raise ParameterDefinitionError(f'condition must be one of: {", ".join(x for x in allowed_conditions)}')
        if condition in ('in', 'not in') and not isinstance(value, tuple):
            raise ParameterDefinitionError('for in/not in conditions value must be a tuple of possible values')
        elif condition not in ('in', 'not in') and isinstance(value, tuple):
            raise ParameterDefinitionError('value can be tuple only for in/not in conditions')

        otype = other_param.type()
        if otype == NodeParameterType.INT:
            if not isinstance(value, tuple):
                value = int(value)
        elif otype == NodeParameterType.BOOL:
            if not isinstance(value, tuple):
                value = bool(value)
        elif otype == NodeParameterType.FLOAT:
            if not isinstance(value, tuple):
                value = float(value)
        elif otype == NodeParameterType.STRING:
            if not isinstance(value, tuple):
                value = str(value)
        else:  # for future
            raise ParameterDefinitionError(f'cannot add visibility condition check based on this type of parameters: {otype}')
        self.__vis_when.append((other_param, condition, value))
        other_param._add_referencing_me(self)
        self.__vis_cache = None

        self.parent()._children_definition_changed([self])
        return self

    def add_menu(self, menu_items_pairs) -> "Parameter":
        """
        adds UI menu to parameter param_name
        :param menu_items_pairs: dict of label -> value for parameter menu. type of value MUST match type of parameter param_name. type of label MUST be string
        :return: self to allow easy chaining
        """
        # sanity check and regroup
        my_type = self.type()
        menu_items = {}
        menu_order = []
        for key, value in menu_items_pairs:
            menu_items[key] = value
            menu_order.append(key)
            if not isinstance(key, str):
                raise ParameterDefinitionError('menu label type must be string')
            if my_type == NodeParameterType.INT and not isinstance(value, int):
                raise ParameterDefinitionError(f'wrong menu value for int parameter "{self.name()}"')
            elif my_type == NodeParameterType.BOOL and not isinstance(value, bool):
                raise ParameterDefinitionError(f'wrong menu value for bool parameter "{self.name()}"')
            elif my_type == NodeParameterType.FLOAT and not isinstance(value, float):
                raise ParameterDefinitionError(f'wrong menu value for float parameter "{self.name()}"')
            elif my_type == NodeParameterType.STRING and not isinstance(value, str):
                raise ParameterDefinitionError(f'wrong menu value for string parameter "{self.name()}"')

        self.__menu_items = menu_items
        self.__menu_items_order = menu_order
        self.parent()._children_definition_changed([self])
        return self

    def has_menu(self):
        return self.__menu_items is not None

    def get_menu_items(self):
        return self.__menu_items_order, self.__menu_items

    def has_same_parent(self, other_parameter: "Parameter") -> bool:
        """
        finds if somewhere down the hierarchy there is a shared parent of self and other_parameter
        """
        my_ancestry_line = set()
        ancestor = self
        while ancestor is not None:
            my_ancestry_line.add(ancestor)
            ancestor = ancestor.parent()

        ancestor = other_parameter
        while ancestor is not None:
            if ancestor in my_ancestry_line:
                return True
            ancestor = ancestor.parent()
        return False

    def __setstate__(self, state):
        """
        overriden for easier parameter class iterations during active development.
        otherwise all node ui data should be recreated from zero in DB every time a change is made
        """
        # this init here only to init new shit when unpickling old parameters without resetting DB all the times
        self.__init__('', '', NodeParameterType.INT, 0, False)
        self.__dict__.update(state)


class ParameterError(RuntimeError):
    pass


class ParameterDefinitionError(ParameterError):
    pass


class ParameterNotFound(ParameterError):
    pass


class ParameterNameCollisionError(ParameterError):
    pass


class ParameterReadonly(ParameterError):
    pass


class ParameterLocked(ParameterError):
    pass


class ParameterCannotHaveExpressions(ParameterError):
    pass


class ParametersLayoutBase(ParameterHierarchyItem):
    def __init__(self):
        super(ParametersLayoutBase, self).__init__()
        self.__parameters: Dict[str, Parameter] = {}  # just for quicker access
        self.__layouts: Set[ParametersLayoutBase] = set()
        self.__block_ui_callbacks = False

    def initializing_interface_lock(self):
        return self.block_ui_callbacks()

    def block_ui_callbacks(self):
        class _iiLock:
            def __init__(self, lockable):
                self.__nui = lockable
                self.__prev_state = False

            def __enter__(self):
                self.__prev_state = self.__nui._ParametersLayoutBase__block_ui_callbacks
                self.__nui._ParametersLayoutBase__block_ui_callbacks = True

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.__nui._ParametersLayoutBase__block_ui_callbacks = self.__prev_state

        return _iiLock(self)

    def _is_initialize_lock_set(self):
        return self.__block_ui_callbacks

    def add_parameter(self, new_parameter: Parameter):
        self.add_generic_leaf(new_parameter)

    def add_generic_leaf(self, item: ParameterHierarchyLeaf):
        if not self._is_initialize_lock_set():
            raise LayoutError('initializing interface not inside initializing_interface_lock')
        item.set_parent(self)

    def add_layout(self, new_layout: "ParametersLayoutBase"):
        if not self._is_initialize_lock_set():
            raise LayoutError('initializing interface not inside initializing_interface_lock')
        new_layout.set_parent(self)

    def items(self, recursive=False) -> Iterable["ParameterHierarchyItem"]:
        for child in self.children():
            yield child
            if not recursive:
                continue
            elif isinstance(child, ParametersLayoutBase):
                for child_param in child.parameters(recursive=recursive):
                    yield child_param

    def parameters(self, recursive=False) -> Iterable[Parameter]:
        for item in self.items(recursive=recursive):
            if isinstance(item, Parameter):
                yield item

    def parameter(self, name: str) -> Parameter:
        if name in self.__parameters:
            return self.__parameters[name]
        for layout in self.__layouts:
            try:
                return layout.parameter(name)
            except ParameterNotFound:
                continue
        raise ParameterNotFound(f'parameter "{name}" not found in layout hierarchy')

    def visible(self) -> bool:
        return len(self.children()) != 0 and any(x.visible() for x in self.items())

    def _child_added(self, child: "ParameterHierarchyItem"):
        super(ParametersLayoutBase, self)._child_added(child)
        if isinstance(child, Parameter):
            # check global parameter name uniqueness
            rootparent = self
            while isinstance(rootparent.parent(), ParametersLayoutBase):
                rootparent = rootparent.parent()
            if child.name() in (x.name() for x in rootparent.parameters(recursive=True) if x != child):
                raise ParameterNameCollisionError('cannot add parameters with the same name to the same layout hierarchy')
            self.__parameters[child.name()] = child
        elif isinstance(child, ParametersLayoutBase):
            self.__layouts.add(child)
            # check global parameter name uniqueness
            rootparent = self
            while isinstance(rootparent.parent(), ParametersLayoutBase):
                rootparent = rootparent.parent()
            new_params = list(child.parameters(recursive=True))
            existing_params = set(x.name() for x in rootparent.parameters(recursive=True) if x not in new_params)
            for new_param in new_params:
                if new_param.name() in existing_params:
                    raise ParameterNameCollisionError('cannot add parameters with the same name to the same layout hierarchy')

    def _child_about_to_be_removed(self, child: "ParameterHierarchyItem"):
        if isinstance(child, Parameter):
            del self.__parameters[child.name()]
        elif isinstance(child, ParametersLayoutBase):
            self.__layouts.remove(child)
        super(ParametersLayoutBase, self)._child_about_to_be_removed(child)

    def _children_definition_changed(self, changed_children: Iterable["ParameterHierarchyItem"]):
        """
        :param changed_children:
        :return:
        """
        super(ParametersLayoutBase, self)._children_definition_changed(changed_children)
        # check self.__parameters consistency
        reversed_parameters: Dict[Parameter, str] = {v: k for k, v in self.__parameters.items()}
        for child in changed_children:
            if not isinstance(child, Parameter):
                continue
            if child in reversed_parameters:
                del self.__parameters[reversed_parameters[child]]
                self.__parameters[child.name()] = child

    def _children_value_changed(self, children: Iterable["ParameterHierarchyItem"]):
        """
        :param children:
        :return:
        """
        super(ParametersLayoutBase, self)._children_value_changed(children)

    def _children_appearance_changed(self, children: Iterable["ParameterHierarchyItem"]):
        super(ParametersLayoutBase, self)._children_appearance_changed(children)

    def relative_size_for_child(self, child: ParameterHierarchyItem) -> Tuple[float, float]:
        """
        get relative size of a child in this layout
        the exact interpretation of size is up to subclass to decide
        :param child:
        :return:
        """
        raise NotImplementedError()


class OrderedParametersLayout(ParametersLayoutBase):
    def __init__(self):
        super(OrderedParametersLayout, self).__init__()
        self.__parameter_order: List[ParameterHierarchyItem] = []

    def _child_added(self, child: "ParameterHierarchyItem"):
        super(OrderedParametersLayout, self)._child_added(child)
        self.__parameter_order.append(child)

    def _child_about_to_be_removed(self, child: "ParameterHierarchyItem"):
        self.__parameter_order.remove(child)
        super(OrderedParametersLayout, self)._child_about_to_be_removed(child)

    def items(self, recursive=False):
        """
        unlike base method, we need to return parameters in order
        :param recursive:
        :return:
        """
        for child in self.__parameter_order:
            yield child
            if not recursive:
                continue
            elif isinstance(child, ParametersLayoutBase):
                for child_param in child.items(recursive=recursive):
                    yield child_param

    def relative_size_for_child(self, child: ParameterHierarchyItem) -> Tuple[float, float]:
        """
        get relative size of a child in this layout
        the exact interpretation of size is up to subclass to decide
        :param child:
        :return:
        """
        assert child in self.children()
        return 1.0, 1.0


class VerticalParametersLayout(OrderedParametersLayout):
    """
    simple vertical parameter layout.
    """
    pass


class CollapsableVerticalGroup(VerticalParametersLayout):
    """
    a vertical parameter layout to be drawn as collapsable block
    """

    def __init__(self, group_name, group_label):
        super(CollapsableVerticalGroup, self).__init__()

        # for now it's here just to ensure name uniqueness. in future - maybe store collapsed state
        self.__unused_param = Parameter(group_name, group_name, NodeParameterType.BOOL, True)

        self.__group_name = group_name
        self.__group_label = group_label

    def is_collapsed(self):
        return True

    def name(self):
        return self.__group_name

    def label(self):
        return self.__group_label


class OneLineParametersLayout(OrderedParametersLayout):
    """
    horizontal parameter layout.
    unlike vertical, this one has to keep track of portions of line it's parameters are taking
    parameters of this group should be rendered in one line
    """

    def __init__(self):
        super(OneLineParametersLayout, self).__init__()
        self.__hsizes = {}

    def _children_appearance_changed(self, children: Iterable["ParameterHierarchyItem"]):
        super(ParametersLayoutBase, self)._children_appearance_changed(children)
        self.__hsizes = {}

    def _children_definition_changed(self, children: Iterable["ParameterHierarchyItem"]):
        super(OneLineParametersLayout, self)._children_definition_changed(children)
        self.__hsizes = {}

    def relative_size_for_child(self, child: ParameterHierarchyItem) -> Tuple[float, float]:
        assert child in self.children()
        if child not in self.__hsizes:
            self._update_hsizes()
        assert child in self.__hsizes
        return self.__hsizes[child], 1.0

    def _update_hsizes(self):
        self.__hsizes = {}
        totalitems = 0
        for item in self.items():
            if item.visible():
                totalitems += 1
        if totalitems == 0:
            uniform_size = 1.0
        else:
            uniform_size = 1.0 / float(totalitems)
        for item in self.items():
            self.__hsizes[item] = uniform_size


class MultiGroupLayout(OrderedParametersLayout):
    """
    this group can dynamically spawn more parameters according to it's template
    spawning more parameters does NOT count as definition change
    """

    def __init__(self, name, label=None):
        super(MultiGroupLayout, self).__init__()
        self.__template: Union[ParametersLayoutBase, Parameter, None] = None
        if label is None:
            label = 'count'
        self.__count_param = Parameter(name, label, NodeParameterType.INT, 0, can_have_expression=False)
        self.__count_param.set_parent(self)
        self.__last_count = 0
        self.__nested_indices = []

    def nested_indices(self):
        """
        if a multiparam is inside other multiparams - those multiparams should add their indices
        to this one, so that this multiparam will be able to uniquely and predictable name it's parameters
        """
        return tuple(self.__nested_indices)

    def __append_nested_index(self, index: int):
        """
        this should be called only when a multiparam is instanced by another multiparam
        """
        self.__nested_indices.append(index)

    def set_spawning_template(self, layout: ParametersLayoutBase):
        self.__template = deepcopy(layout)

    def add_layout(self, new_layout: "ParametersLayoutBase"):
        """
        this function is unavailable cuz of the nature of this layout
        """
        raise LayoutError('NO')

    def add_parameter(self, new_parameter: Parameter):
        """
        this function is unavailable cuz of the nature of this layout
        """
        raise LayoutError('NO')

    def add_template_instance(self):
        self.__count_param.set_value(self.__count_param.value() + 1)

    def _children_value_changed(self, children: Iterable["ParameterHierarchyItem"]):

        for child in children:
            if child == self.__count_param:
                break
        else:
            super(MultiGroupLayout, self)._children_value_changed(children)
            return
        if self.__count_param.value() < 0:
            self.__count_param.set_value(0)
            super(MultiGroupLayout, self)._children_value_changed(children)
            return

        new_count = self.__count_param.value()
        if self.__last_count < new_count:
            if self.__template is None:
                raise LayoutError('template is not set')
            for _ in range(new_count - self.__last_count):
                # note: the check below is good, but it's not needed currently, cuz visibility condition on append checks common parent
                # and nodes not from template do not share parents with template, so that prevents external references
                for param in self.__template.parameters(recursive=True):
                    # sanity check - for now we only support references within the same template block only
                    for ref_param in param.references():
                        if not ref_param.has_same_parent(param):
                            raise ParameterDefinitionError('Parameters within MultiGroupLayout\'s template currently cannot reference outer parameters')
                ##
                new_layout = deepcopy(self.__template)
                i = len(self.children()) - 1
                for param in new_layout.parameters(recursive=True):
                    param._set_name(param.name() + '_' + '.'.join(str(x) for x in (*self.nested_indices(), i)))
                    parent = param.parent()
                    if isinstance(parent, MultiGroupLayout):
                        for idx in self.nested_indices():
                            parent.__append_nested_index(idx)
                        parent.__append_nested_index(i)
                new_layout.set_parent(self)
        elif self.__last_count > self.__count_param.value():
            for _ in range(self.__last_count - new_count):
                instances = list(self.items(recursive=False))
                assert len(instances) > 1
                instances[-1].set_parent(None)
        self.__last_count = new_count
        super(MultiGroupLayout, self)._children_value_changed(children)

    def _child_added(self, child: "ParameterHierarchyItem"):
        super(MultiGroupLayout, self)._child_added(child)

    def _child_about_to_be_removed(self, child: "ParameterHierarchyItem"):
        super(MultiGroupLayout, self)._child_about_to_be_removed(child)


@dataclass
class ParameterFullValue:
    unexpanded_value: Union[int, float, str, bool]
    expression: Optional[str]

