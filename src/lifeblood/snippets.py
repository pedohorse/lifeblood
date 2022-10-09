import asyncio
import json
import pickle
import dataclasses
from dataclasses import dataclass

from .enums import NodeParameterType

from typing import Any, Optional, Tuple, Dict, Set, Iterable


class NodeSnippetData:
    """
    class containing enough information to reproduce a certain snippet of nodes, with parameter values and connections ofc
    """

    class Serializer(json.JSONEncoder):
        def default(self, obj):
            if dataclasses.is_dataclass(obj):
                dcs = obj.__dict__  # dataclasses.asdict is recursive, kills inner dataclasses
                dcs['__dataclass__'] = obj.__class__.__name__
                return dcs
            elif isinstance(obj, NodeSnippetData):
                return {'nodes': obj.nodes_data,
                        'connections': obj.connections_data,
                        'label': obj.label,
                        'tags': list(obj.tags),
                        '__NodeSnippetData__': '==3*E==',
                        '__format_version__': [1, 0, 0]
                        }
            elif isinstance(obj, NodeParameterType):
                return {'value': obj.value,
                        '__NodeParameterType__': '==3*E=='
                        }
            return super(NodeSnippetData.Serializer, self).default(obj)

    class Deserializer(json.JSONDecoder):
        def dedata(self, obj):
            if '__dataclass__' in obj:
                data = getattr(NodeSnippetData, obj['__dataclass__'])(**{k: v for k, v in obj.items() if k != '__dataclass__'})
                if obj['__dataclass__'] == 'NodeData':
                    data.pos = tuple(data.pos)
                return data
            elif obj.get('__NodeSnippetData__', None) == '==3*E==':
                if obj['__format_version__'] >= [2]:
                    raise NotImplementedError(f'snippet format {obj["__format_version__"]} is not supported')
                return NodeSnippetData(obj['nodes'], obj['connections'], obj['label'], obj['tags'])
            elif obj.get('__NodeParameterType__', None) == '==3*E==':
                return NodeParameterType(obj['value'])
            return obj

        def __init__(self):
            super(NodeSnippetData.Deserializer, self).__init__(object_hook=self.dedata)

    @dataclass
    class ParamData:
        name: str
        type: NodeParameterType
        uvalue: Any
        expr: Optional[str]

    @dataclass
    class NodeData:
        tmpid: int
        type: str
        name: str
        parameters: Dict[str, "NodeSnippetData.ParamData"]
        pos: Tuple[float, float]

    @dataclass
    class ConnData:
        tmpout: int
        out_name: str
        tmpin: int
        in_name: str

    @property
    def nodes_data(self):
        return self.__nodes_data

    @nodes_data.setter
    def nodes_data(self, nodes_data):
        self.__nodes_data = nodes_data
        self.__avgpos = None

    @property
    def connections_data(self):
        return self.__connections_data

    @connections_data.setter
    def connections_data(self, connections_data):
        self.__connections_data = connections_data

    @property
    def pos(self) -> Tuple[float, float]:
        if self.__avgpos is None:
            avgpos = [0., 0.]
            for nodedata in self.__nodes_data:
                avgpos[0] += nodedata.pos[0]
                avgpos[1] += nodedata.pos[1]
            self.__avgpos = (avgpos[0] / len(self.__nodes_data), avgpos[1] / len(self.__nodes_data))
        return self.__avgpos

    @property
    def label(self) -> Optional[str]:
        return self.__label

    @property
    def tags(self) -> Set[str]:
        return self.__tags

    def add_tag(self, tag: str):
        self.__tags.add(tag)

    def __init__(self, nodes_data: Iterable[NodeData], connections_data: Iterable[ConnData], label: Optional[str] = None, tags: Optional[Iterable[str]] = None):
        self.__nodes_data = list(nodes_data)
        self.__connections_data = list(connections_data)
        self.__label: Optional[str] = label
        self.__tags: Set[str] = set(tags) if tags is not None else set()
        self.__avgpos = None

    def __eq__(self, other: "NodeSnippetData"):
        return self.nodes_data == other.nodes_data and self.connections_data == other.connections_data and \
               self.__tags == other.__tags and \
               self.__label == other.__label

    def __repr__(self):
        nodesrepr = '\n'.join(repr(x) for x in self.nodes_data)
        connsrepr = '\n'.join(repr(x) for x in self.connections_data)
        return f'NodeSnippetData: nodes: {nodesrepr}\n\nconnections: {connsrepr}'

    def serialize(self, ascii=True) -> bytes:
        """
        serialize into bytes, ascii-friendly or not
        """
        if ascii:
            return json.dumps(self, cls=NodeSnippetData.Serializer, indent=4).encode('UTF-8')
        return b'\0' + pickle.dumps(self)

    async def serialize_async(self) -> bytes:
        return await asyncio.get_event_loop().run_in_executor(None, self.serialize)

    @classmethod
    def deserialize(cls, data: bytes) -> "NodeSnippetData":
        if data[0] == 0:
            return pickle.loads(data[1:])
        else:
            return json.loads(data.decode('UTF-8'), cls=NodeSnippetData.Deserializer)

    @classmethod
    async def deserialize_async(cls, data: bytes) -> "NodeSnippetData":
        return await asyncio.get_event_loop().run_in_executor(None, cls.deserialize, data)


class NodeSnippetDataPlaceholder:
    """
    this is an empty metadata-only class to transfer over the net
    """
    def __init__(self, label: Optional[str] = None, tags: Optional[Iterable[str]] = None):
        self.__label = label
        self.__tags = set(tags) if tags is not None else set()

    @property
    def label(self) -> Optional[str]:
        return self.__label

    @property
    def tags(self) -> Set[str]:
        return self.__tags

    @classmethod
    def from_nodesnippetdata(cls, source: NodeSnippetData):
        return NodeSnippetDataPlaceholder(source.label, source.tags)
