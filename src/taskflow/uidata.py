import asyncio
import pickle
from copy import deepcopy
from .enums import NodeParameterType
import re

from typing import TYPE_CHECKING, TypedDict, Dict, Any, List, Set, Optional, Tuple, Union, Iterable, FrozenSet

if TYPE_CHECKING:
    from .basenode import BaseNode


async def create_uidata(ui_nodes, ui_connections, ui_tasks, all_task_groups):
    return await asyncio.get_event_loop().run_in_executor(None, UiData, ui_nodes, ui_connections, ui_tasks, all_task_groups)


class UiData:
    def __init__(self, ui_nodes, ui_connections, ui_tasks, all_task_groups):
        self.__nodes = ui_nodes
        self.__conns = ui_connections
        self.__tasks = ui_tasks
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

    def task_groups(self):
        return self.__task_groups

    async def serialize(self) -> bytes:
        return await asyncio.get_event_loop().run_in_executor(None, pickle.dumps, self)

    def __repr__(self):
        return f'{self.__nodes} :::: {self.__conns}'

    @classmethod
    def deserialize(cls, data: bytes) -> "UiData":
        return pickle.loads(data)


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
            self.__parent._children_definition_changed([self])

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


class Parameter(ParameterHierarchyLeaf):
    def __init__(self, param_name: str, param_label: Optional[str], param_type: NodeParameterType, param_val: Any):
        super(Parameter, self).__init__()
        self.__name = param_name
        self.__label = param_label
        self.__type = param_type
        self.__value = None
        self.__menu_items: Dict[str, str] = None
        self.__menu_items_order: List[str] = []
        self.__vis_when = None

        # links
        self.__params_referencing_me: Set["Parameter"] = set()

        # caches
        self.__vis_cache = None

        self.set_value(param_val)

    def name(self) -> str:
        return self.__name

    def label(self) -> Optional[str]:
        return self.__label

    def type(self) -> NodeParameterType:
        return self.__type

    def value(self):
        return self.__value

    def set_value(self, value: Any):
        if self.__type == NodeParameterType.FLOAT:
            param_value = float(value)
        elif self.__type == NodeParameterType.INT:
            param_value = int(value)
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

    def _referencing_param_value_changed(self, other_parameter):
        """
        when a parameter that we are referencing changes - it will report here
        :param other_parameter:
        """
        if self.__vis_when is not None:
            self.__vis_cache = None
            if self.parent() is not None and isinstance(self.parent(), ParametersLayoutBase):
                self.parent()._children_appearance_changed([self])

    def visible(self) -> bool:
        if self.__vis_cache is not None:
            return self.__vis_cache
        if self.__vis_when is not None:
            other_param, op, value = self.__vis_when
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
        self.__params_referencing_me.add(other_parameter)

    def _remove_referencing_me(self, other_parameter: "Parameter"):
        assert other_parameter in self.__params_referencing_me
        self.__params_referencing_me.remove(other_parameter)

    def add_visibility_condition(self, other_param: "Parameter", condition: str, value):
        """
        condition currently can only be a simplest
        :param other_param:
        :param condition:
        :param value:
        :return:
        """

        assert condition in ('==', '!=', '>=', '<=', '<', '>')

        if self.__vis_when is not None:
            self.__vis_when[0]._remove_referencing_me(self)

        otype = other_param.type()
        if otype == NodeParameterType.INT:
            value = int(value)
        elif otype == NodeParameterType.BOOL:
            value = bool(value)
        elif otype == NodeParameterType.FLOAT:
            value = float(value)
        elif otype != NodeParameterType.STRING:  # for future
            raise RuntimeError(f'cannot add visibility condition check based on this type of parameters: {otype}')
        self.__vis_when = (other_param, condition, value)
        other_param._add_referencing_me(self)
        self.__vis_cache = None

        self.parent()._children_definition_changed([self])

    def add_menu(self, menu_items_pairs):
        """
        adds UI menu to parameter param_name
        :param menu_items_pairs: dict of label -> value for parameter menu. type of value MUST match type of parameter param_name. type of label MUST be string
        :return:
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

    def has_menu(self):
        return self.__menu_items is not None

    def get_menu_items(self):
        return self.__menu_items_order, self.__menu_items


class ParameterNotFound(RuntimeError):
    pass


class ParametersLayoutBase(ParameterHierarchyItem):
    def __init__(self):
        super(ParametersLayoutBase, self).__init__()
        self.__parameters: Dict[str: Parameter] = {}  # just for quicker access
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
            if child.name() in (x.name() for x in self.parameters(recursive=True)):
                raise RuntimeError('cannot add parameters with the same name to the same layout hierarchy')
            self.__parameters[child.name()] = child
        elif isinstance(child, ParametersLayoutBase):
            self.__layouts.add(child)

    def _child_about_to_be_removed(self, child: "ParameterHierarchyItem"):
        if isinstance(child, Parameter):
            del self.__parameters[child.name()]
        elif isinstance(child, ParametersLayoutBase):
            self.__layouts.remove(child)
        super(ParametersLayoutBase, self)._child_about_to_be_removed(child)

    def _children_definition_changed(self, children: Iterable["ParameterHierarchyItem"]):
        """
        :param children:
        :return:
        """
        super(ParametersLayoutBase, self)._children_definition_changed(children)

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
                for child_param in child.parameters(recursive=recursive):
                    yield child_param

    def relative_size_for_child(self, child: ParameterHierarchyItem) -> Tuple[float, float]:
        """
        get relative size of a child in this layout
        the exact interpretation of size is up to subclass to decide
        :param child:
        :return:
        """
        raise NotImplementedError()


class VerticalParametersLayout(OrderedParametersLayout):
    """
    simple vertical parameter layout.
    """

    def relative_size_for_child(self, child: ParameterHierarchyItem) -> Tuple[float, float]:
        assert child in self.children()
        return 1.0, 1.0


class OneLineParametersLayout(OrderedParametersLayout):
    """
    horizontal parameter layout.
    unlike vertical, this one has to keep track of portions of line it's parameters are taking
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
        uniform_size = 1.0 / float(totalitems)
        for item in self.items():
            self.__hsizes[item] = uniform_size


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
        def __init__(self, ui: "NodeUi", layout_creator):
            self.__ui = ui
            self.__layout_creator = layout_creator

        def __enter__(self):
            new_layout = self.__layout_creator()
            self.__ui._NodeUi__groups_stack.append(new_layout)
            with self.__ui._NodeUi__parameter_layout.initializing_interface_lock():
                self.__ui._NodeUi__parameter_layout.add_layout(new_layout)

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.__ui._NodeUi__groups_stack.pop()

    def parameters_on_same_line_block(self):
        """
        use it in with statement
        :return:
        """

        if not self.__block_ui_callbacks:
            raise RuntimeError('initializing NodeUi interface not inside initializing_interface_lock')
        return NodeUi._slwrapper(self, OneLineParametersLayout)

    def add_parameter(self, param_name: str, param_label: Optional[str], param_type: NodeParameterType, param_val: Any):
        if not self.__block_ui_callbacks:
            raise RuntimeError('initializing NodeUi interface not inside initializing_interface_lock')
        layout = self.__parameter_layout
        if len(self.__groups_stack) != 0:
            layout = self.__groups_stack[-1]
        with layout.initializing_interface_lock():
            newparam = Parameter(param_name, param_label, param_type, param_val)
            layout.add_parameter(newparam)
        return newparam

    def add_input(self, input_name):
        if not self.__block_ui_callbacks:
            raise RuntimeError('initializing NodeUi interface not inside initializing_interface_lock')
        if input_name not in self.__outputs_names:
            self.__outputs_names += (input_name,)

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

    def serialize(self) -> bytes:
        obj = deepcopy(self)
        obj.__attached_node = None
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
