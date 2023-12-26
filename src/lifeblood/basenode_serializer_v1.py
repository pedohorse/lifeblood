import pickle
from io import BytesIO
from dataclasses import dataclass, is_dataclass
import json
from .basenode_serialization import NodeSerializerBase, FailedToDeserialize
from .basenode import BaseNode, NodeParameterType

from typing import Callable, Optional, Tuple, Union

from .node_dataprovider_base import NodeDataProvider
from .nodegraph_holder_base import NodeGraphHolderBase


@dataclass
class ParameterData:
    name: str
    type: NodeParameterType
    unexpanded_value: Union[int, float, str, bool]
    expression: Optional[str]


def create_node_maker(node_data_provider: NodeDataProvider) -> Callable[[str, str, NodeGraphHolderBase, int], BaseNode]:
    def create_node(type_name: str, name: str, sched_parent, node_id) -> BaseNode:
        node = node_data_provider.node_factory(type_name)(name)
        node.set_parent(sched_parent, node_id)
        return node
    return create_node


class NodeSerializerV1(NodeSerializerBase):
    def serialize(self, node: BaseNode) -> Tuple[bytes, Optional[bytes]]:
        raise DeprecationWarning('no use this!')

    def deserialize(self, parent: NodeGraphHolderBase, node_id: int, node_data_provider: NodeDataProvider, data: bytes, state: Optional[bytes]) -> BaseNode:
        # this be pickled
        # we do hacky things here fo backward compatibility
        class Unpickler(pickle.Unpickler):
            def find_class(self, module, name):
                if module == 'lifeblood.pluginloader' and name == 'create_node':
                    return create_node_maker(node_data_provider)
                return super(Unpickler, self).find_class(module, name)

        if state is not None:
            raise FailedToDeserialize(f'deserialization v1 is not expecting a separate state data')

        try:
            newobj: BaseNode = Unpickler(BytesIO(data)).load()
        except Exception as e:
            raise FailedToDeserialize(f'error loading pickle: {e}') from None

        newobj.set_parent(parent, node_id)
        return newobj
