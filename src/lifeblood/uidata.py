import lz4.frame
import asyncio
import pickle
import os
import pathlib
import math
from copy import deepcopy
from .enums import NodeParameterType
from .processingcontext import ProcessingContext
from .node_visualization_classes import NodeColorScheme
import re

from typing import TYPE_CHECKING, TypedDict, Dict, Any, List, Set, Optional, Tuple, Union, Iterable, FrozenSet, Type

if TYPE_CHECKING:
    from .basenode import BaseNode


async def create_uidata(ui_nodes, ui_connections, ui_tasks, ui_workers, all_task_groups):
    return await asyncio.get_event_loop().run_in_executor(None, UiData, ui_nodes, ui_connections, ui_tasks, ui_workers, all_task_groups)


class ParameterExpressionError(Exception):
    def __init__(self, inner_exception):
        self.__inner_exception = inner_exception

    def __str__(self):
        return f'ParameterExpressionError: {str(self.__inner_exception)}'

    def inner_expection(self):
        return self.__inner_exception


class UiData:
    def __init__(self, ui_nodes, ui_connections, ui_tasks, ui_workers, all_task_groups):
        self.__nodes = ui_nodes
        self.__conns = ui_connections
        self.__tasks = ui_tasks
        self.__workers = ui_workers
        self.__task_groups = all_task_groups
        # self.__conns = {}
        # for conn in raw_connections:
        #     id_out = conn['node_id_out']
        #     id_in = conn['node_id_in']
        #     if id_out not in self.__conns:
        #         self.__conns[id_out] = {}
        #     if id_in not in self.__conns[id_out]:
        #         self.__conns[id_out][id_in] = []
        #     self.__conns[id_out][id_in].append(dict(conn))

    def nodes(self):
        return self.__nodes

    def connections(self):
        return self.__conns

    def tasks(self):
        return self.__tasks

    def workers(self):
        return self.__workers

    def task_groups(self):
        return self.__task_groups

    async def serialize(self, compress=False) -> bytes:
        res = await asyncio.get_event_loop().run_in_executor(None, pickle.dumps, self)
        if not compress:
            return b'\0\0\0' + res
        return b'lz4' + await asyncio.get_event_loop().run_in_executor(None, lz4.frame.compress, res)

    def __repr__(self):
        return f'{self.__nodes} :::: {self.__conns}'

    @classmethod
    def deserialize_noasync(cls, data: bytes) -> "UiData":
        cmp = data[:3]
        if cmp == b'lz4':
            return pickle.loads(lz4.frame.decompress(data[3:]))
        elif cmp == b'\0\0\0':
            return pickle.loads(data[3:])
        raise NotImplementedError(f'data compression format {repr(cmp)} is not implemented')


# if TYPE_CHECKING:
#     class Parameter(TypedDict):
#         type: NodeParameterType
#         value: Any
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


def evaluate_expression(expression, context: Optional[ProcessingContext]):
    try:
        return eval(expression,
                    {'os': os, 'pathlib': pathlib, **{k: getattr(math, k) for k in dir(math) if not k.startswith('_')}},
                    context.locals() if context is not None else {})
    except Exception as e:
        raise ParameterExpressionError(e)


class Parameter(ParameterHierarchyLeaf):

    class DontChange:
        pass

    def __init__(self, param_name: str, param_label: Optional[str], param_type: NodeParameterType, param_val: Any, can_have_expression: bool = True, readonly: bool = False, default_value = None):
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

        self.__expression = None
        self.__can_have_expressions = can_have_expression

        self.__re_expand_pattern = re.compile(r'`(.*?)`')

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

    def unexpanded_value(self, context: Optional[ProcessingContext] = None):
        return self.__value

    def default_value(self):
        """
        note that this value will be unexpanded

        :return:
        """
        return self.__default_value

    def value(self, context: Optional[ProcessingContext] = None) -> Any:
        """
        returns value of this parameter
        :param context: optional dict like locals, for expression evaluations
        """

        if self.__expression is not None:
            result = evaluate_expression(self.__expression, context)
            # check type and cast
            if self.__type == NodeParameterType.INT:
                result = int(result)
            elif self.__type == NodeParameterType.FLOAT:
                result = float(result)
            elif self.__type == NodeParameterType.STRING and not isinstance(result, str):
                result = str(result)
            elif self.__type == NodeParameterType.BOOL:
                result = bool(result)
            return result

        if self.__type != NodeParameterType.STRING:
            return self.__value

        # for string parameters we expand expressions in ``, kinda like bash
        return self.__re_expand_pattern.sub(lambda m: str(evaluate_expression(m.group(1), context)), self.__value)

    def set_slider_visualization(self, value_min=DontChange, value_max=DontChange):  # type: (Union[int, float], Union[int, float]) -> None
        """
        set a visual slider's minimum and maximum
        this does nothing to the parameter itself, and it's up to parameter renderer to interpret this data
        """
        if self.__type not in (NodeParameterType.INT, NodeParameterType.FLOAT):
            raise RuntimeError('cannot set limits for parameters of types other than INT and FLOAT')

        if self.__type == NodeParameterType.INT:
            value_min = int(value_min)
        elif self.__type == NodeParameterType.FLOAT:
            value_min = float(value_min)

        if self.__type == NodeParameterType.INT:
            value_max = int(value_max)
        elif self.__type == NodeParameterType.FLOAT:
            value_max = float(value_max)

        self.__display_borders = (value_min, value_max)

    def set_value_limits(self, value_min=DontChange, value_max=DontChange):  # type: (Union[int, float, None, Type[DontChange]], Union[int, float, None, Type[DontChange]]) -> None
        """
        set minimum and maximum values that parameter will enforce
        None means no limit (unset limit)
        """
        if self.__type not in (NodeParameterType.INT, NodeParameterType.FLOAT):
            raise RuntimeError('cannot set limits for parameters of types other than INT and FLOAT')
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

    def set_text_multiline(self, syntax_hint=None):
        if self.__type != NodeParameterType.STRING:
            raise RuntimeError('multiline can be only set for string parameters')
        self.__string_multiline = True
        self.__string_multiline_syntax_hint = syntax_hint

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

    def set_value(self, value: Any):
        if self.__is_readonly:
            raise ParameterReadonly()
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
        if not self.__can_have_expressions:
            raise ParameterCannotHaveExpressions()
        if expression != self.__expression:
            self.__expression = expression
            if self.parent() is not None:
                self.parent()._children_definition_changed([self])

    def remove_expression(self):
        self.set_expression(None)

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
                        or op == '<=' and other_param.value() > value:
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

    def append_visibility_condition(self, other_param: "Parameter", condition: str, value) -> "Parameter":
        """
        condition currently can only be a simplest
        :param other_param:
        :param condition:
        :param value:
        :return: self to allow easy chaining
        """

        assert condition in ('==', '!=', '>=', '<=', '<', '>')

        otype = other_param.type()
        if otype == NodeParameterType.INT:
            value = int(value)
        elif otype == NodeParameterType.BOOL:
            value = bool(value)
        elif otype == NodeParameterType.FLOAT:
            value = float(value)
        elif otype != NodeParameterType.STRING:  # for future
            raise RuntimeError(f'cannot add visibility condition check based on this type of parameters: {otype}')
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
                raise RuntimeError('menu label type must be string')
            if my_type == NodeParameterType.INT and not isinstance(value, int):
                raise RuntimeError(f'wrong menu value for int parameter "{self.name()}"')
            elif my_type == NodeParameterType.BOOL and not isinstance(value, bool):
                raise RuntimeError(f'wrong menu value for bool parameter "{self.name()}"')
            elif my_type == NodeParameterType.FLOAT and not isinstance(value, float):
                raise RuntimeError(f'wrong menu value for float parameter "{self.name()}"')
            elif my_type == NodeParameterType.STRING and not isinstance(value, str):
                raise RuntimeError(f'wrong menu value for string parameter "{self.name()}"')

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

    def nodeui(self) -> Optional["NodeUi"]:
        """
        returns parent nodeui if it is the root of current hierarchy. otherwise returns None
        """
        ancestor = self
        while ancestor is not None:
            if isinstance(ancestor, NodeUi):
                return ancestor
            ancestor = ancestor.parent()
        return None

    def __setstate__(self, state):
        """
        overriden for easier parameter class iterations during active development.
        otherwise all node ui data should be recreated from zero in DB every time a change is made
        """
        # this init here only to init new shit when unpickling old parameters without resetting DB all the times
        self.__init__('', '', NodeParameterType.INT,  0, False)
        self.__dict__.update(state)


class ParameterError(RuntimeError):
    pass


class ParameterNotFound(ParameterError):
    pass


class ParameterNameCollisionError(ParameterError):
    pass


class ParameterReadonly(ParameterError):
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
        class _iiLock:
            def __init__(self, lockable):
                self.__nui = lockable

            def __enter__(self):
                self.__nui._ParametersLayoutBase__block_ui_callbacks = True

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.__nui._ParametersLayoutBase__block_ui_callbacks = False

        return _iiLock(self)

    def _is_initialize_lock_set(self):
        return self.__block_ui_callbacks

    def add_parameter(self, new_parameter: Parameter):
        if not self._is_initialize_lock_set():
            raise RuntimeError('initializing interface not inside initializing_interface_lock')
        new_parameter.set_parent(self)

    def add_layout(self, new_layout: "ParametersLayoutBase"):
        if not self._is_initialize_lock_set():
            raise RuntimeError('initializing interface not inside initializing_interface_lock')
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
        raise ParameterNotFound(f'parameter {name} not found in layout hierarchy')

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
        :param children:
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
    def __init__(self, name):
        super(MultiGroupLayout, self).__init__()
        self.__template: Union[ParametersLayoutBase, Parameter, None] = None
        self.__count_param = Parameter(name, 'count', NodeParameterType.INT, 0, can_have_expression=False)
        self.__count_param.set_parent(self)
        self.__last_count = 0

    def set_spawning_template(self, layout: ParametersLayoutBase):
        self.__template = deepcopy(layout)

    def add_layout(self, new_layout: "ParametersLayoutBase"):
        """
        this function is unavailable cuz of the nature of this layout
        """
        raise RuntimeError('NO')

    def add_parameter(self, new_parameter: Parameter):
        """
        this function is unavailable cuz of the nature of this layout
        """
        raise RuntimeError('NO')

    def add_template_instance(self):
        self.__count_param.set_value(self.__count_param.value() + 1)

    def _children_value_changed(self, children: Iterable["ParameterHierarchyItem"]):

        for child in children:
            if child == self.__count_param:
                break
        else:
            super(MultiGroupLayout, self)._children_value_changed(children)
        if self.__count_param.value() < 0:
            self.__count_param.set_value(0)
            super(MultiGroupLayout, self)._children_value_changed(children)
            return

        new_count = self.__count_param.value()
        if self.__last_count < new_count:
            if self.__template is None:
                raise RuntimeError('template is not set')
            for _ in range(new_count - self.__last_count):
                new_layout = deepcopy(self.__template)
                i = len(self.children()) - 1
                for param in new_layout.parameters(recursive=True):
                    param._set_name(param.name() + '_' + str(i))
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


class NodeUi(ParameterHierarchyItem):
    def __init__(self, attached_node: "BaseNode"):
        super(NodeUi, self).__init__()
        self.__parameter_layout = VerticalParametersLayout()
        self.__parameter_layout.set_parent(self)
        self.__attached_node: Optional[BaseNode] = attached_node
        self.__block_ui_callbacks = False
        self.__inputs_names = ('main',)
        self.__outputs_names = ('main',)

        self.__groups_stack = []

        # default colorscheme
        self.__color_scheme = NodeColorScheme()
        self.__color_scheme.set_main_color(0.1882, 0.2510, 0.1882)  # dark-greenish

    def is_attached_to_node(self):
        return self.__attached_node is not None

    def attach_to_node(self, node: "BaseNode"):
        self.__attached_node = node

    def color_scheme(self):
        return self.__color_scheme

    def main_parameter_layout(self):
        return self.__parameter_layout

    def parent(self) -> Optional["ParameterHierarchyItem"]:
        return None

    def set_parent(self, item: Optional["ParameterHierarchyItem"]):
        if item is not None:
            raise RuntimeError('NodeUi class is supposed to be tree root')

    def initializing_interface_lock(self):
        class _iiLock:
            def __init__(self, lockable):
                self.__nui = lockable

            def __enter__(self):
                self.__nui._NodeUi__block_ui_callbacks = True

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.__nui._NodeUi__block_ui_callbacks = False

        return _iiLock(self)

    class _slwrapper:
        def __init__(self, ui: "NodeUi", layout_creator, layout_creator_kwargs=None):
            self.__ui = ui
            self.__layout_creator = layout_creator
            self.__layout_creator_kwargs = layout_creator_kwargs or {}

        def __enter__(self):
            new_layout = self.__layout_creator(**self.__layout_creator_kwargs)
            self.__ui._NodeUi__groups_stack.append(new_layout)
            with self.__ui._NodeUi__parameter_layout.initializing_interface_lock():
                self.__ui._NodeUi__parameter_layout.add_layout(new_layout)

        def __exit__(self, exc_type, exc_val, exc_tb):
            layout = self.__ui._NodeUi__groups_stack.pop()
            self.__ui._add_layout(layout)

    def parameters_on_same_line_block(self):
        """
        use it in with statement
        :return:
        """

        if not self.__block_ui_callbacks:
            raise RuntimeError('initializing NodeUi interface not inside initializing_interface_lock')
        return NodeUi._slwrapper(self, OneLineParametersLayout)

    def multigroup_parameter_block(self, name: str):
        """
        use it in with statement
        creates a block like multiparameter block in houdini
        any parameters added will be actually added to template to be instanced later as needed
        :return:
        """
        class _slwrapper_multi:
            def __init__(self, ui: "NodeUi", name: str):
                self.__ui = ui
                self.__new_layout = None
                self.__name = name

            def __enter__(self):
                self.__new_layout = VerticalParametersLayout()
                self.__ui._NodeUi__groups_stack.append(self.__new_layout)

            def __exit__(self, exc_type, exc_val, exc_tb):
                assert self.__ui._NodeUi__groups_stack.pop() == self.__new_layout
                with self.__ui._NodeUi__parameter_layout.initializing_interface_lock():
                    multi_layout = MultiGroupLayout(self.__name)
                    with multi_layout.initializing_interface_lock():
                        multi_layout.set_spawning_template(self.__new_layout)
                    self.__ui._add_layout(multi_layout)

            def multigroup(self):
                return self.__new_layout

        if not self.__block_ui_callbacks:
            raise RuntimeError('initializing NodeUi interface not inside initializing_interface_lock')
        return _slwrapper_multi(self, name)

    def collapsable_group_block(self, group_name: str, group_label: str = ''):
        """
        use it in with statement
        creates a visually distinct group of parameters that renderer should draw as a collapsable block

        :return:
        """
        if not self.__block_ui_callbacks:
            raise RuntimeError('initializing NodeUi interface not inside initializing_interface_lock')
        return NodeUi._slwrapper(self, CollapsableVerticalGroup, {'group_name': group_name, 'group_label': group_label})

    def _add_layout(self, new_layout):
        if not self.__block_ui_callbacks:
            raise RuntimeError('initializing NodeUi interface not inside initializing_interface_lock')
        layout = self.__parameter_layout
        if len(self.__groups_stack) != 0:
            layout = self.__groups_stack[-1]
        with layout.initializing_interface_lock():
            layout.add_layout(new_layout)

    def add_parameter(self, param_name: str, param_label: Optional[str], param_type: NodeParameterType, param_val: Any, can_have_expressions: bool = True, readonly: bool = False):
        if not self.__block_ui_callbacks:
            raise RuntimeError('initializing NodeUi interface not inside initializing_interface_lock')
        layout = self.__parameter_layout
        if len(self.__groups_stack) != 0:
            layout = self.__groups_stack[-1]
        with layout.initializing_interface_lock():
            newparam = Parameter(param_name, param_label, param_type, param_val, can_have_expressions, readonly)
            layout.add_parameter(newparam)
        return newparam

    def add_input(self, input_name):
        if not self.__block_ui_callbacks:
            raise RuntimeError('initializing NodeUi interface not inside initializing_interface_lock')
        if input_name not in self.__inputs_names:
            self.__inputs_names += (input_name,)

    def add_output(self, output_name):
        if not self.__block_ui_callbacks:
            raise RuntimeError('initializing NodeUi interface not inside initializing_interface_lock')
        if output_name not in self.__outputs_names:
            self.__outputs_names += (output_name,)

    def add_output_for_spawned_tasks(self):
        return self.add_output('spawned')

    def _children_definition_changed(self, children: Iterable["ParameterHierarchyItem"]):
        self.__ui_callback()

    def _children_value_changed(self, children: Iterable["ParameterHierarchyItem"]):
        self.__ui_callback()

    def __ui_callback(self):
        if self.__attached_node is not None and not self.__block_ui_callbacks:
            self.__attached_node._ui_changed()

    def inputs_names(self) -> Tuple[str]:
        return self.__inputs_names

    def outputs_names(self) -> Tuple[str]:
        return self.__outputs_names

    def parameter(self, param_name: str) -> Parameter:
        return self.__parameter_layout.parameter(param_name)

    def parameters(self) -> Iterable[Parameter]:
        return self.__parameter_layout.parameters(recursive=True)

    def items(self, recursive=False) -> Iterable[ParameterHierarchyItem]:
        return self.__parameter_layout.items(recursive=recursive)

    def __deepcopy__(self, memo):
        cls = self.__class__
        crap = cls.__new__(cls)
        newdict = self.__dict__.copy()
        newdict['_NodeUi__attached_node'] = None
        for k, v in newdict.items():
            crap.__dict__[k] = deepcopy(v, memo)
        return crap

    def serialize(self) -> bytes:
        """
        note - this serialization disconnects the node to which this UI is connected
        :return:
        """
        obj = deepcopy(self)
        assert obj.__attached_node is None
        return pickle.dumps(obj)

    async def serialize_async(self) -> bytes:
        return await asyncio.get_event_loop().run_in_executor(None, self.serialize)

    def __repr__(self):
        return 'NodeUi: ' + ', '.join(('%s: %s' % (x.name() if isinstance(x, Parameter) else '-layout-', x) for x in self.__parameter_layout.items()))

    @classmethod
    def deserialize(cls, data: bytes) -> "NodeUi":
        return pickle.loads(data)

    @classmethod
    async def deserialize_async(cls, data: bytes) -> "NodeUi":
        return await asyncio.get_event_loop().run_in_executor(None, cls.deserialize, data)