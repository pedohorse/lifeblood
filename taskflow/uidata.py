import asyncio
import pickle
from copy import copy
from .enums import NodeParameterType

from typing import TYPE_CHECKING, TypedDict, Dict, Any, List, Optional

if TYPE_CHECKING:
    from .basenode import BaseNode

async def create_uidata(raw_nodes, raw_connections, raw_tasks):
    return await asyncio.get_event_loop().run_in_executor(None, UiData, raw_nodes, raw_connections, raw_tasks)


class UiData:
    def __init__(self, raw_nodes, raw_connections, raw_tasks):
        self.__nodes = {x['id']: dict(x) for x in raw_nodes}
        self.__conns = {x['id']: dict(x) for x in raw_connections}
        self.__tasks = {x['id']: dict(x) for x in raw_tasks}
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

    def add_parameter(self, param_name: str, param_type: NodeParameterType, param_val: Any):
        self.__parameter_order.append(param_name)
        self.__parameters[param_name] = {'type': param_type, 'value': param_val}
        if self.__attached_node is not None:
            self.__attached_node._ui_changed([param_name])

    def parameter_order(self):
        return self.__parameter_order

    def parameters(self):
        return self.__parameters

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
        if self.__attached_node is not None:
            self.__attached_node._ui_changed([param_name])

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
