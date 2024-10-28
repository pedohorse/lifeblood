import asyncio
import pickle
from copy import deepcopy
from .enums import NodeParameterType
from .node_visualization_classes import NodeColorScheme
from .node_ui_callback_receiver_base import NodeUiCallbackReceiverBase
from .node_parameters import CollapsableVerticalGroup, LayoutError, LayoutReadonlyError, MultiGroupLayout, OneLineParametersLayout, Parameter, ParameterError, ParameterFullValue, ParameterHierarchyItem, ParameterLocked, ParameterReadonly, ParametersLayoutBase, Separator, VerticalParametersLayout
from .logging import get_logger

from typing import Dict, Any, Optional, Tuple, Iterable, Callable


class NodeUiError(RuntimeError):
    pass


class NodeUiDefinitionError(RuntimeError):
    pass


class _SpecialOutputCountChangingLayout(VerticalParametersLayout):
    def __init__(self, nodeui: "NodeUi", parameter_name, parameter_label):
        super(_SpecialOutputCountChangingLayout, self).__init__()
        self.__my_nodeui = nodeui
        newparam = Parameter(parameter_name, parameter_label, NodeParameterType.INT, 2, can_have_expression=False)
        newparam.set_value_limits(2)
        with self.initializing_interface_lock():
            self.add_parameter(newparam)

    def add_layout(self, new_layout: "ParametersLayoutBase"):
        """
        this function is unavailable cuz of the nature of this layout
        """
        raise LayoutError('NO')

    def add_parameter(self, new_parameter: Parameter):
        """
        this function is unavailable cuz of the nature of this layout
        """
        if len(list(self.parameters())) > 0:
            raise LayoutError('NO')
        super(_SpecialOutputCountChangingLayout, self).add_parameter(new_parameter)

    def _children_value_changed(self, children: Iterable["ParameterHierarchyItem"]):
        # we expect this special layout to have only one single specific child
        child = None
        for child in children:
            break
        if child is None:
            return
        assert isinstance(child, Parameter)
        new_num_outputs = child.value()
        num_outputs = len(self.__my_nodeui.outputs_names())
        if num_outputs == new_num_outputs:
            return

        if num_outputs < new_num_outputs:
            for i in range(num_outputs, new_num_outputs):
                self.__my_nodeui._add_output_unsafe(f'output{i}')
        else:  # num_outputs > new_num_outputs
            for _ in range(new_num_outputs, num_outputs):
                self.__my_nodeui._remove_last_output_unsafe()
        self.__my_nodeui._outputs_definition_changed()


class NodeUi(ParameterHierarchyItem):
    def __init__(self, change_callback_receiver: NodeUiCallbackReceiverBase):
        super(NodeUi, self).__init__()
        self.__logger = get_logger('scheduler.nodeUI')
        self.__parameter_layout = VerticalParametersLayout()
        self.__parameter_layout.set_parent(self)
        self.__change_callback_receiver: Optional[NodeUiCallbackReceiverBase] = change_callback_receiver
        self.__block_ui_callbacks = False
        self.__lock_ui_readonly = False
        self.__postpone_ui_callbacks = False
        self.__postponed_callbacks = None
        self.__inputs_names = ('main',)
        self.__outputs_names = ('main',)

        self.__groups_stack = []

        self.__have_output_parameter_set: bool = False

        # default colorscheme
        self.__color_scheme = NodeColorScheme()
        self.__color_scheme.set_main_color(0.1882, 0.2510, 0.1882)  # dark-greenish

    def set_ui_change_callback_receiver(self, callback_receiver: NodeUiCallbackReceiverBase):
        self.__change_callback_receiver = callback_receiver

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
        return self.block_ui_callbacks()

    def block_ui_callbacks(self):
        class _iiLock:
            def __init__(self, lockable):
                self.__nui = lockable
                self.__prev_state = None

            def __enter__(self):
                self.__prev_state = self.__nui._NodeUi__block_ui_callbacks
                self.__nui._NodeUi__block_ui_callbacks = True

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.__nui._NodeUi__block_ui_callbacks = self.__prev_state

        if self.__lock_ui_readonly:
            raise LayoutReadonlyError()
        return _iiLock(self)

    def lock_interface_readonly(self):
        raise NotImplementedError("read trello task, read TODO. this do NOT work multitheaded, leads to permalocks, needs rethinking")
        class _roLock:
            def __init__(self, lockable):
                self.__nui = lockable
                self.__prev_state = None

            def __enter__(self):
                self.__prev_state = self.__nui._NodeUi__lock_ui_readonly
                self.__nui._NodeUi__lock_ui_readonly = True

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.__nui._NodeUi__lock_ui_readonly = self.__prev_state

        return _roLock(self)

    def postpone_ui_callbacks(self):
        """
        use this in with-statement
        for mass change of parameters it may be more efficient to perform changes in batches
        """
        class _iiPostpone:
            def __init__(self, nodeui):
                self.__nui = nodeui
                self.__val = None

            def __enter__(self):
                if not self.__nui._NodeUi__postpone_ui_callbacks:
                    assert self.__nui._NodeUi__postponed_callbacks is None
                    self.__val = self.__nui._NodeUi__postpone_ui_callbacks
                    self.__nui._NodeUi__postpone_ui_callbacks = True
                # otherwise: already blocked - we are in nested block, ignore

            def __exit__(self, exc_type, exc_val, exc_tb):
                if self.__val is None:
                    return
                assert not self.__val  # nested block should do nothing
                self.__nui._NodeUi__postpone_ui_callbacks = self.__val
                if self.__nui._NodeUi__postponed_callbacks is not None:
                    self.__nui._NodeUi__ui_callback(self.__nui._NodeUi__postponed_callbacks)
                    self.__nui._NodeUi__postponed_callbacks = None

        return _iiPostpone(self)

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
        return self.parameter_layout_block(OneLineParametersLayout)

    def parameter_layout_block(self, parameter_layout_producer: Callable[[], ParametersLayoutBase]):
        """
        arbitrary simple parameter override block
        use it in with statement
        :return:
        """
        if self.__lock_ui_readonly:
            raise LayoutReadonlyError()
        if not self.__block_ui_callbacks:
            raise NodeUiDefinitionError('initializing NodeUi interface not inside initializing_interface_lock')
        return NodeUi._slwrapper(self, parameter_layout_producer)

    def add_parameter_to_control_output_count(self, parameter_name: str, parameter_label: str):
        """
        a very special function for a very special case when you want the number of outputs to be controlled
        by a parameter

        from now on output names will be: 'main', 'output1', 'output2', ...

        :return:
        """
        if self.__lock_ui_readonly:
            raise LayoutReadonlyError()
        if not self.__block_ui_callbacks:
            raise NodeUiDefinitionError('initializing NodeUi interface not inside initializing_interface_lock')
        if self.__have_output_parameter_set:
            raise NodeUiDefinitionError('there can only be one parameter to control output count')
        self.__have_output_parameter_set = True
        self.__outputs_names = ('main', 'output1')

        with self.parameter_layout_block(lambda: _SpecialOutputCountChangingLayout(self, parameter_name, parameter_label)):
            # no need to do anything, with block will add that layout to stack, and parameter is created in that layout's constructor
            layout = self.current_layout()
            # this layout should always have exactly one parameter
            assert len(list(layout.parameters())) == 1, f'oh no, {len(list(layout.parameters()))}'
        return layout.parameter(parameter_name)

    def multigroup_parameter_block(self, name: str, label: Optional[str] = None):
        """
        use it in with statement
        creates a block like multiparameter block in houdini
        any parameters added will be actually added to template to be instanced later as needed
        :return:
        """
        class _slwrapper_multi:
            def __init__(self, ui: "NodeUi", name: str, label: Optional[str] = None):
                self.__ui = ui
                self.__new_layout = None
                self.__name = name
                self.__label = label

            def __enter__(self):
                self.__new_layout = VerticalParametersLayout()
                self.__ui._NodeUi__groups_stack.append(self.__new_layout)
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                assert self.__ui._NodeUi__groups_stack.pop() == self.__new_layout
                with self.__ui._NodeUi__parameter_layout.initializing_interface_lock():
                    multi_layout = MultiGroupLayout(self.__name, self.__label)
                    with multi_layout.initializing_interface_lock():
                        multi_layout.set_spawning_template(self.__new_layout)
                    self.__ui._add_layout(multi_layout)

            def multigroup(self) -> VerticalParametersLayout:
                return self.__new_layout

        if self.__lock_ui_readonly:
            raise LayoutReadonlyError()
        if not self.__block_ui_callbacks:
            raise NodeUiDefinitionError('initializing NodeUi interface not inside initializing_interface_lock')
        return _slwrapper_multi(self, name, label)

    def current_layout(self):
        """
        get current layout to which add_parameter would add parameter
        this can be main nodeUI's layout, but can be something else, if we are in some with block,
        like for ex: collapsable_group_block or parameters_on_same_line_block

        :return:
        """
        if not self.__block_ui_callbacks:
            raise NodeUiDefinitionError('initializing NodeUi interface not inside initializing_interface_lock')
        layout = self.__parameter_layout
        if len(self.__groups_stack) != 0:
            layout = self.__groups_stack[-1]
        return layout

    def collapsable_group_block(self, group_name: str, group_label: str = ''):
        """
        use it in with statement
        creates a visually distinct group of parameters that renderer should draw as a collapsable block

        :return:
        """
        if self.__lock_ui_readonly:
            raise LayoutReadonlyError()
        if not self.__block_ui_callbacks:
            raise NodeUiDefinitionError('initializing NodeUi interface not inside initializing_interface_lock')
        return NodeUi._slwrapper(self, CollapsableVerticalGroup, {'group_name': group_name, 'group_label': group_label})

    def _add_layout(self, new_layout):
        if not self.__block_ui_callbacks:
            raise NodeUiDefinitionError('initializing NodeUi interface not inside initializing_interface_lock')
        layout = self.__parameter_layout
        if len(self.__groups_stack) != 0:
            layout = self.__groups_stack[-1]
        with layout.initializing_interface_lock():
            layout.add_layout(new_layout)

    def add_parameter(self, param_name: str, param_label: Optional[str], param_type: NodeParameterType, param_val: Any, can_have_expressions: bool = True, readonly: bool = False) -> Parameter:
        if self.__lock_ui_readonly:
            raise LayoutReadonlyError()
        if not self.__block_ui_callbacks:
            raise NodeUiDefinitionError('initializing NodeUi interface not inside initializing_interface_lock')
        layout = self.__parameter_layout
        if len(self.__groups_stack) != 0:
            layout = self.__groups_stack[-1]
        with layout.initializing_interface_lock():
            newparam = Parameter(param_name, param_label, param_type, param_val, can_have_expressions, readonly)
            layout.add_parameter(newparam)
        return newparam

    def add_separator(self):
        if self.__lock_ui_readonly:
            raise LayoutReadonlyError()
        if not self.__block_ui_callbacks:
            raise NodeUiDefinitionError('initializing NodeUi interface not inside initializing_interface_lock')
        layout = self.__parameter_layout
        if len(self.__groups_stack) != 0:
            layout = self.__groups_stack[-1]
        with layout.initializing_interface_lock():
            newsep = Separator()
            layout.add_generic_leaf(newsep)
        return newsep

    def add_input(self, input_name):
        if self.__lock_ui_readonly:
            raise LayoutReadonlyError()
        if not self.__block_ui_callbacks:
            raise NodeUiDefinitionError('initializing NodeUi interface not inside initializing_interface_lock')
        if input_name not in self.__inputs_names:
            self.__inputs_names += (input_name,)

    def _add_output_unsafe(self, output_name):
        if output_name not in self.__outputs_names:
            self.__outputs_names += (output_name,)

    def add_output(self, output_name):
        if self.__lock_ui_readonly:
            raise LayoutReadonlyError()
        if not self.__block_ui_callbacks:
            raise NodeUiDefinitionError('initializing NodeUi interface not inside initializing_interface_lock')
        if self.__have_output_parameter_set:
            raise NodeUiDefinitionError('cannot add outputs when output count is controlled by a parameter')
        return self._add_output_unsafe(output_name)

    def _remove_last_output_unsafe(self):
        if len(self.__outputs_names) < 2:
            return
        self.__outputs_names = self.__outputs_names[:-1]

    def remove_last_output(self):
        if self.__lock_ui_readonly:
            raise LayoutReadonlyError()
        if not self.__block_ui_callbacks:
            raise NodeUiDefinitionError('initializing NodeUi interface not inside initializing_interface_lock')
        if self.__have_output_parameter_set:
            raise NodeUiDefinitionError('cannot add outputs when output count is controlled by a parameter')
        return self._remove_last_output_unsafe()

    def add_output_for_spawned_tasks(self):
        return self.add_output('spawned')

    def _children_definition_changed(self, children: Iterable["ParameterHierarchyItem"]):
        self.__ui_callback(definition_changed=True)

    def _children_value_changed(self, children: Iterable["ParameterHierarchyItem"]):
        self.__ui_callback(definition_changed=False)

    def _outputs_definition_changed(self):  #TODO: not entirely sure how safe this is right now
        self.__ui_callback(definition_changed=True)

    def __ui_callback(self, definition_changed=False):
        if self.__lock_ui_readonly:
            raise LayoutReadonlyError()
        if self.__postpone_ui_callbacks:
            # so we save definition_changed to __postponed_callbacks
            self.__postponed_callbacks = self.__postponed_callbacks or definition_changed
            return

        if self.__change_callback_receiver is not None and not self.__block_ui_callbacks:
            self.__change_callback_receiver._ui_changed(definition_changed)

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

    def set_parameters_batch(self, parameters: Dict[str, ParameterFullValue]):
        """
        If signal blocking is needed - caller can do it

        for now it's implemented the stupid way
        """
        names_to_set = list(parameters.keys())
        names_to_set.append(None)
        something_set_this_iteration = False
        parameters_were_postponed = False
        for param_name in names_to_set:
            if param_name is None:
                if parameters_were_postponed:
                    if not something_set_this_iteration:
                        self.__logger.warning(f'failed to set all parameters!')
                        break
                    names_to_set.append(None)
                something_set_this_iteration = False
                continue
            assert isinstance(param_name, str)
            param = self.parameter(param_name)
            if param is None:
                parameters_were_postponed = True
                continue
            param_value = parameters[param_name]
            try:
                param.set_value(param_value.unexpanded_value)
            except (ParameterReadonly, ParameterLocked):
                # if value is already correct - just skip
                if param.unexpanded_value() != param_value.unexpanded_value:
                    self.__logger.error(f'unable to set value for "{param_name}"')
                    # shall we just ignore the error?
            except ParameterError as e:
                self.__logger.error(f'failed to set value for "{param_name}" because {repr(e)}')
            if param.can_have_expressions():
                try:
                    param.set_expression(param_value.expression)
                except (ParameterReadonly, ParameterLocked):
                    # if value is already correct - just skip
                    if param.expression() != param_value.expression:
                        self.__logger.error(f'unable to set expression for "{param_name}"')
                        # shall we just ignore the error?
                except ParameterError as e:
                    self.__logger.error(f'failed to set expression for "{param_name}" because {repr(e)}')
            elif param_value.expression is not None:
                self.__logger.error(f'parameter "{param_name}" cannot have expressions, yet expression is stored for it')

            something_set_this_iteration = True

    def __deepcopy__(self, memo):
        cls = self.__class__
        crap = cls.__new__(cls)
        newdict = self.__dict__.copy()
        newdict['_NodeUi__change_callback_receiver'] = None
        newdict['_NodeUi__lock_ui_readonly'] = False
        assert id(self) not in memo
        memo[id(self)] = crap  # to avoid recursion, though manual tells us to treat memo as opaque object
        for k, v in newdict.items():
            crap.__dict__[k] = deepcopy(v, memo)
        return crap

    def __setstate__(self, state):
        ensure_attribs = {  # this exists only for the ease of upgrading NodeUi classes during development
            '_NodeUi__lock_ui_readonly': False,
            '_NodeUi__postpone_ui_callbacks': False
        }
        self.__dict__.update(state)
        for attrname, default_value in ensure_attribs.items():
            if not hasattr(self, attrname):
                setattr(self, attrname, default_value)

    def serialize(self) -> bytes:
        """
        note - this serialization disconnects the node to which this UI is connected
        :return:
        """
        obj = deepcopy(self)
        assert obj.__change_callback_receiver is None
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
