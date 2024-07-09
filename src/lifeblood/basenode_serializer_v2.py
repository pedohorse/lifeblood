from dataclasses import dataclass, is_dataclass
import json
from .common_serialization import AttribSerializer, AttribDeserializer
from .basenode_serialization import NodeSerializerBase, IncompatibleDeserializationMethod, FailedToApplyNodeState, FailedToApplyParameters
from .basenode import BaseNode
from .enums import NodeParameterType
from .uidata import ParameterFullValue

from typing import Optional, Tuple, Union

from .node_dataprovider_base import NodeDataProvider


@dataclass
class ParameterData:
    name: str
    type: NodeParameterType
    unexpanded_value: Union[int, float, str, bool]
    expression: Optional[str]


class NodeSerializerV2(NodeSerializerBase):
    """
    Universal json-like serializer
    Note, this supports more things than json, such as:
    - tuples
    - sets
    - int dict keys
    - tuple dict keys
    - limited set of dataclasses

    the final string though is json-compliant
    """

    class Serializer(AttribSerializer):
        def _reform(self, obj):
            if is_dataclass(obj):
                dcs = self._reform(obj.__dict__)  # dataclasses.asdict is recursive, kills inner dataclasses
                dcs['__dataclass__'] = obj.__class__.__name__
                dcs['__special_object_type__'] = 'dataclass'
                return dcs
            elif isinstance(obj, NodeParameterType):
                return {'value': obj.value,
                        '__special_object_type__': 'NodeParameterType'
                        }
            return super()._reform(obj)

    class Deserializer(AttribDeserializer):
        def _dedata(self, obj):
            special_type = obj.get('__special_object_type__')
            if special_type == 'dataclass':
                data = globals()[obj['__dataclass__']](**{k: v for k, v in obj.items() if k not in ('__dataclass__', '__special_object_type__')})
                if obj['__dataclass__'] == 'NodeData':
                    data.pos = tuple(data.pos)
                return data
            elif special_type == 'NodeParameterType':
                return NodeParameterType(obj['value'])
            return super()._dedata(obj)

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

    def deserialize(self, node_data_provider: NodeDataProvider, data: bytes, state: Optional[bytes]) -> BaseNode:
        try:
            data_dict = json.loads(data.decode('latin1'), cls=NodeSerializerV2.Deserializer)
        except json.JSONDecodeError:
            raise IncompatibleDeserializationMethod('not a json') from None
        for musthave in ('format_version', 'type_name', 'type_definition_hash', 'parameters', 'name', 'ingraph_id'):
            if musthave not in data_dict:
                raise IncompatibleDeserializationMethod('missing required fields')
        if (fv := data_dict['format_version']) != 2:
            raise IncompatibleDeserializationMethod(f'format_version {fv} is not supported')
        new_node = node_data_provider.node_factory(data_dict['type_name'])(data_dict['name'])
        try:
            with new_node.get_ui().block_ui_callbacks():
                param_dict = {name: ParameterFullValue(val.unexpanded_value, val.expression) for name, val in data_dict['parameters'].items()}
                self.__resource_compatibility_filter(param_dict)  # TODO: remove this a couple of months in the future
                new_node.get_ui().set_parameters_batch(param_dict)
        except Exception:
            # actually set_parameters_batch catches all reasonable exceptions and treats them as warnings,
            #  so this seems unreachable, but if something does happen - we treat it as fail to set all params
            raise FailedToApplyParameters(bad_parameters=data_dict['parameters'].keys())
        if state:
            try:
                new_node.set_state(json.loads(state.decode('latin1'), cls=NodeSerializerV2.Deserializer))
            except Exception as e:
                raise FailedToApplyNodeState(wrapped_expection=e)

        return new_node

    @staticmethod
    def __resource_compatibility_filter(param_dict: dict):
        rename_params = {
            'priority adjustment': '__requirements__.priority_adjustment',
            'worker groups': '__requirements__.worker_groups',
            'worker type': '__requirements__.worker_type',
            'worker cpu cost': '__requirements__.f_min_res_0',
            'worker cpu cost preferred': '__requirements__.f_pref_res_0',
            'worker mem cost': '__requirements__.f_min_res_1',
            'worker mem cost preferred': '__requirements__.f_pref_res_1',
        }
        for param_name in (
                'worker gpu cost', 'worker gpu cost preferred', 'worker gpu mem cost', 'worker gpu mem cost preferred'
        ):
            if param_name in param_dict:
                param_dict.pop(param_name)

        if 'worker cpu cost' in param_dict and '__requirements__.res' not in param_dict:
            param_dict['__requirements__.res'] = ParameterData(
                '__requirements__.res',
                NodeParameterType.INT,
                2,
                None,
            )

        for old_name, new_name in rename_params.items():
            if new_name in param_dict or old_name not in param_dict:
                continue
            param_dict[new_name] = param_dict.pop(old_name)
