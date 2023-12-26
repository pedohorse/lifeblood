from dataclasses import dataclass, is_dataclass
import json
from .basenode_serialization import NodeSerializerBase, FailedToDeserialize
from .basenode import BaseNode, NodeParameterType
from .uidata import ParameterFullValue

from typing import Optional, Tuple, Union

from .node_dataprovider_base import NodeDataProvider
from .nodegraph_holder_base import NodeGraphHolderBase


@dataclass
class ParameterData:
    name: str
    type: NodeParameterType
    unexpanded_value: Union[int, float, str, bool]
    expression: Optional[str]


class NodeSerializerV2(NodeSerializerBase):
    class Serializer(json.JSONEncoder):
        def default(self, obj):
            if is_dataclass(obj):
                dcs = obj.__dict__  # dataclasses.asdict is recursive, kills inner dataclasses
                dcs['__dataclass__'] = obj.__class__.__name__
                return dcs
            elif isinstance(obj, NodeParameterType):
                return {'value': obj.value,
                        '__NodeParameterType__': '==3*E=='
                        }
            return super(NodeSerializerV2.Serializer, self).default(obj)

    class Deserializer(json.JSONDecoder):
        def dedata(self, obj):
            if '__dataclass__' in obj:
                data = globals()[obj['__dataclass__']](**{k: v for k, v in obj.items() if k != '__dataclass__'})
                if obj['__dataclass__'] == 'NodeData':
                    data.pos = tuple(data.pos)
                return data
            elif obj.get('__NodeParameterType__', None) == '==3*E==':
                return NodeParameterType(obj['value'])
            return obj

        def __init__(self):
            super(NodeSerializerV2.Deserializer, self).__init__(object_hook=self.dedata)

    def serialize(self, node: BaseNode) -> Tuple[bytes, Optional[bytes]]:
        param_values = {}
        for param in node.get_ui().parameters():
            param_values[param.name()] = ParameterData(
                param.name(),
                param.type(),
                param.unexpanded_value(),
                param.expression()
            )

        data_dict = {
            'format_version': 2,
            'type_name': node.type_name(),
            'name': node.name(),
            'ingraph_id': node.id(),  # node_id will be overriden on deserialize, to make sure scheduler is consistent
            'type_definition_hash': node.my_plugin().hash(),
            'parameters': param_values,
        }

        return (
            json.dumps(data_dict, cls=NodeSerializerV2.Serializer).encode('latin1'),
            self.serialize_state_only(node)
        )

    def serialize_state_only(self, node: BaseNode) -> Optional[bytes]:
        state = node.get_state()
        return None if state is None else json.dumps(state, cls=NodeSerializerV2.Serializer).encode('latin1')

    def deserialize(self, parent: NodeGraphHolderBase, node_id: int, node_data_provider: NodeDataProvider, data: bytes, state: Optional[bytes]) -> BaseNode:
        try:
            data_dict = json.loads(data.decode('latin1'), cls=NodeSerializerV2.Deserializer)
        except json.JSONDecodeError:
            raise FailedToDeserialize('not a json') from None
        for musthave in ('format_version', 'type_name', 'type_definition_hash', 'parameters', 'name', 'ingraph_id'):
            if musthave not in data_dict:
                raise FailedToDeserialize('missing required fields')
        if (fv := data_dict['format_version']) != 2:
            raise FailedToDeserialize(f'format_version {fv} is not supported')
        new_node = node_data_provider.node_factory(data_dict['type_name'])(data_dict['name'])
        new_node.set_parent(parent, node_id)
        with new_node.get_ui().block_ui_callbacks():
            new_node.get_ui().set_parameters_batch({name: ParameterFullValue(val.unexpanded_value, val.expression) for name, val in data_dict['parameters'].items()})
        if state:
            new_node.set_state(json.loads(state.decode('latin1'), cls=NodeSerializerV2.Deserializer))
        # TODO: add ser-de tests!

        return new_node
