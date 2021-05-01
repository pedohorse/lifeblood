import asyncio
import pickle
from copy import copy
from .enums import NodeParameterType
import re

from typing import TYPE_CHECKING, TypedDict, Dict, Any, List, Optional, Tuple, Union

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


if TYPE_CHECKING:
    class Parameter(TypedDict):
        type: NodeParameterType
        value: Any


class NodeUi:
    def __init__(self, attached_node: "BaseNode"):
        self.__parameters: Dict[str: Parameter] = {}
        self.__parameter_order: List[str] = []
        self.__attached_node: Optional[BaseNode] = attached_node
        self.__block_ui_callbacks = False
        self.__inputs_names = ('main',)
        self.__outputs_names = ('main',)

    def initializing_interface_lock(self):
        class _iiLock:
            def __init__(self, lockable):
                self.__nui = lockable

            def __enter__(self):
                self.__nui._NodeUi__block_ui_callbacks = True

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.__nui._NodeUi__block_ui_callbacks = False

        return _iiLock(self)

    def _make_unique_parm_name(self, name: str) -> str:
        i = 1
        param_name = name + str(i)
        while param_name in self.__parameters:
            i += 1
            param_name = name + str(i)
        return param_name

    def add_parameter(self, param_name: str, param_label: Optional[str], param_type: NodeParameterType, param_val: Any):
        if not self.__block_ui_callbacks:
            raise RuntimeError('initializing NodeUi interface not inside initializing_interface_lock')
        if param_name in self.__parameters:
            raise RuntimeError(f'parameter "{param_name}" already exists on the node')
        if param_label is None:
            param_label = param_name

        # analyze parm line and adjust width
        cnt_in_line = None
        if len(self.__parameter_order) > 0 and self.__parameters[self.__parameter_order[-1]].get('is_ui_modifier', False) \
                                           and self.__parameters[self.__parameter_order[-1]].get('type', None) == 'sameline':
            psep = False
            pcnt = 1
            pparams = []
            for pname in reversed(self.__parameter_order):
                psep = not psep
                if psep:
                    if not self.__parameters[pname].get('is_ui_modifier', False) or self.__parameters[pname].get('type', None) != 'sameline':
                        break
                else:
                    pcnt += 1
                    pparams.append(self.__parameters[pname])
            cnt_in_line = pcnt
            for param in pparams:
                param['_inline_cnt'] = cnt_in_line
                if '_inline_ord' not in param:
                    param['_inline_ord'] = 0
        #
        self.__parameter_order.append(param_name)
        self.__parameters[param_name] = {'label': param_label, 'type': param_type, 'value': param_val, 'is_ui_modifier': False}
        if cnt_in_line:
            self.__parameters[param_name]['_inline_cnt'] = cnt_in_line
            self.__parameters[param_name]['_inline_ord'] = cnt_in_line - 1
        self.__reset_all_cache()
        self.__ui_callback([param_name])

    def parameter_line_portion(self, param_name: str) -> float:
        param = self.__parameters[param_name]
        if '_inline_cnt' not in param:
            return 1.0
        assert '_inline_ord' in param
        if param.get('__width_cache', None) is not None:
            return param['__width_cache']

        idx = self.__parameter_order.index(param_name)
        line_ord = param['_inline_ord']
        line_cnt = param['_inline_cnt']

        viz_cnt_in_line = 0
        for i in range(idx - 2*line_ord, idx + 2*(-line_ord + line_cnt) -1):  # 2* cuz there are sameline modifier between each param in line
            if self.__parameters[self.__parameter_order[i]].get('is_ui_modifier', False):
                continue
            if self.is_parameter_visible(self.__parameter_order[i]):
                viz_cnt_in_line += 1
        param['__width_cache'] = 1 / viz_cnt_in_line
        return param['__width_cache']

    def __reset_all_cache(self):
        for param_name, param in self.__parameters.items():
            for item in param:
                if item.startswith('__'):
                    param[item] = None

    def next_parameter_same_line(self):
        if not self.__block_ui_callbacks:
            raise RuntimeError('initializing NodeUi interface not inside initializing_interface_lock')
        param_name = self._make_unique_parm_name('__next_same_line')  # TODO: get rid of this modifier - it only makes shit more complicated
        self.__parameter_order.append(param_name)
        self.__parameters[param_name] = {'label': None, 'type': 'sameline', 'value': None, 'is_ui_modifier': True}
        self.__ui_callback([param_name])

    def is_parameter_visible(self, param_name: str):
        param_dict = self.__parameters[param_name]
        if param_dict.get('__vis_cache', None) is not None:
            return param_dict['__vis_cache']
        if '_vis_when' in param_dict:
            other_param_name, op, value = param_dict['_vis_when']
            other_param = self.__parameters.get(other_param_name, None)
            if op == '==' and other_param['value'] != value \
                    or op == '!=' and other_param['value'] == value \
                    or op == '>' and other_param['value'] <= value \
                    or op == '>=' and other_param['value'] < value \
                    or op == '<' and other_param['value'] >= value \
                    or op == '<=' and other_param['value'] > value:
                param_dict['__vis_cache'] = False
                return False
        param_dict['__vis_cache'] = not param_dict.get('is_ui_modifier', False)
        return True

    def add_visibility_condition(self, param_name: str, condition: Union[str, Tuple[str, str, Any]]):
        """
        condition currently can only be a simplest
        :param param_name:
        :param condition:
        :return:
        """
        if not self.__block_ui_callbacks:
            raise RuntimeError('initializing NodeUi interface not inside initializing_interface_lock')
        if param_name not in self.__parameters:
            raise RuntimeError(f'parameter "{param_name}" does not exists on the node')
        if isinstance(condition, str):
            match = re.match(r'(\w+)(==|!=|>=|<=|<|>)(.*)', condition)
            if match is None:
                raise RuntimeError(f'bad visibility condition: {condition}')
            oparam, op, value = tuple(match.groups())
            if oparam not in self.__parameters:
                raise RuntimeError('parameters in visibility condition must already be added to allow type checking')
            otype = self.__parameters[oparam]['type']
            if otype == NodeParameterType.INT:
                value = int(value)
            elif otype == NodeParameterType.BOOL:
                value = bool(value)
            elif otype == NodeParameterType.FLOAT:
                value = float(value)
            elif otype != NodeParameterType.STRING:  # for future
                raise RuntimeError(f'cannot add visibility condition check based on this type of parameters: {otype}')
            self.__parameters[param_name]['_vis_when'] = (oparam, op, value)
        else:
            self.__parameters[param_name]['_vis_when'] = condition
        self.__reset_all_cache()

    def add_menu_to_parameter(self, param_name: str, menu_items_pairs):
        """
        adds UI menu to parameter param_name
        :param param_name: parameter to add menu to. Must already exist on the node
        :param menu_items: dict of label -> value for parameter menu. type of value MUST match type of parameter param_name. type of label MUST be string
        :return:
        """
        if not self.__block_ui_callbacks:
            raise RuntimeError('initializing NodeUi interface not inside initializing_interface_lock')
        if param_name not in self.__parameters:
            raise RuntimeError(f'parameter "{param_name}" does not exists on the node')
        # sanity check and regroup
        param_type = self.__parameters[param_name]['type']
        menu_items = {}
        menu_order = []
        for key, value in menu_items_pairs:
            menu_items[key] = value
            menu_order.append(key)
            if not isinstance(key, str):
                raise RuntimeError('menu label type must be string')
            if param_type == NodeParameterType.INT and not isinstance(value, int):
                raise RuntimeError(f'wrong menu value for int parameter "{param_name}"')
            elif param_type == NodeParameterType.BOOL and not isinstance(value, bool):
                raise RuntimeError(f'wrong menu value for bool parameter "{param_name}"')
            elif param_type == NodeParameterType.FLOAT and not isinstance(value, float):
                raise RuntimeError(f'wrong menu value for float parameter "{param_name}"')
            elif param_type == NodeParameterType.STRING and not isinstance(value, str):
                raise RuntimeError(f'wrong menu value for string parameter "{param_name}"')

        self.__parameters[param_name]['menu_items'] = menu_items
        self.__parameters[param_name]['_menu_items_order'] = menu_order
        self.__ui_callback([param_name])

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

    def __ui_callback(self, params: List[str]):
        if self.__attached_node is not None and not self.__block_ui_callbacks:
            self.__attached_node._ui_changed(params)

    def parameter_order(self) -> List[str]:
        return self.__parameter_order

    def parameters(self) -> Dict[str, "Parameter"]:
        return self.__parameters

    def inputs_names(self) -> Tuple[str]:
        return self.__inputs_names

    def outputs_names(self) -> Tuple[str]:
        return self.__outputs_names

    def parameter_value(self, param_name: str):
        return self.__parameters[param_name]['value']

    def set_parameter(self, param_name: str, param_value: Any):
        if param_name not in self.__parameters:
            raise KeyError('wrong param name! this node does not have such parameter')
        ptype = self.__parameters[param_name]['type']
        if ptype == NodeParameterType.FLOAT:
            param_value = float(param_value)
        elif ptype == NodeParameterType.INT:
            param_value = int(param_value)
        elif ptype == NodeParameterType.BOOL:
            param_value = bool(param_value)
        elif ptype == NodeParameterType.STRING:
            param_value = str(param_value)
        else:
            raise NotImplementedError()
        self.__parameters[param_name]['value'] = param_value
        self.__reset_all_cache()
        self.__ui_callback([param_name])

    def parameters_items(self):
        def _iterator():
            for param in self.__parameter_order:
                yield param, self.__parameters[param]
        return _iterator()

    def serialize(self) -> bytes:
        obj = copy(self)
        obj.__attached_node = None
        return pickle.dumps(obj)

    async def serialize_async(self) -> bytes:
        return await asyncio.get_event_loop().run_in_executor(None, self.serialize)

    def __repr__(self):
        return 'NodeUi: ' + ', '.join(('%s: %s' % (x, self.__parameters[x]) for x in self.__parameter_order))

    @classmethod
    def deserialize(cls, data: bytes) -> "NodeUi":
        return pickle.loads(data)

    @classmethod
    async def deserialize_async(cls, data: bytes) -> "NodeUi":
        return await asyncio.get_event_loop().run_in_executor(None, cls.deserialize, data)
