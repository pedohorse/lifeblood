import asyncio
from typing import Any, Callable, Dict, Optional, Set, Type, Union
from pathlib import Path
from unittest import TestCase
from lifeblood.enums import NodeParameterType
from lifeblood.basenode_serializer_v2 import NodeSerializerV2
from lifeblood.basenode import BaseNode, NodeUi
from lifeblood.nodegraph_holder_base import NodeGraphHolderBase
from lifeblood.node_dataprovider_base import NodeDataProvider
from lifeblood.snippets import NodeSnippetData


class TestGraphHolder(NodeGraphHolderBase):
    async def get_node_input_connections(self, node_id: int, input_name: Optional[str] = None):
        raise NotImplementedError()

    async def get_node_output_connections(self, node_id: int, output_name: Optional[str] = None):
        raise NotImplementedError()

    async def node_reports_changes_needs_saving(self, node_id):
        print(self, 'node_reports_changes_needs_saving called')

    def get_event_loop(self) -> asyncio.AbstractEventLoop:
        asyncio.get_event_loop()


class TestDataProvider(NodeDataProvider):
    def node_settings_names(self, type_name: str) -> Set[str]:
        raise NotImplementedError()

    def node_settings(self, type_name: str, settings_name: str) -> dict:
        raise NotImplementedError()

    def node_type_names(self) -> Set[str]:
        raise NotImplementedError()

    def node_class(self, type_name) -> Type[BaseNode]:
        raise NotImplementedError()

    def node_factory(self, node_type: str) -> Callable[[str], BaseNode]:
        if node_type == 'lelolelolelo':
            return TestNode1
        raise NotImplementedError()

    def has_node_factory(self, node_type: str) -> bool:
        raise NotImplementedError()

    def node_preset_packages(self) -> Set[str]:
        raise NotImplementedError()

    # node presets -
    def node_preset_names(self, package_name: str) -> Set[str]:
        raise NotImplementedError()

    def node_preset(self, package_name: str, preset_name: str) -> NodeSnippetData:
        raise NotImplementedError()

    def add_settings_to_existing_package(self, package_name_or_path: Union[str, Path], node_type_name: str, settings_name: str, settings: Dict[str, Any]):
        raise NotImplementedError()

    def set_settings_as_default(self, node_type_name: str, settings_name: Optional[str]):
        raise NotImplementedError()


class TestNode1(BaseNode):
    def __init__(self, name):
        super().__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('test param 1', 'oh, i\'m a label', NodeParameterType.FLOAT, 1.2)
            ui.add_parameter('test param 2', 'label 2', NodeParameterType.STRING, 'fofafqf q !@#')
            ui.add_parameter('test param 11', 'oh, i\'m a label', NodeParameterType.INT, 4)
            with ui.multigroup_parameter_block('oh, multiparam !'):
                ui.add_parameter('test multi param 1', 'rerlo', NodeParameterType.BOOL, False)
                ui.add_parameter('test multi param 2', 'rerlo', NodeParameterType.STRING, 'f q w ')
                with ui.multigroup_parameter_block('oh, mememe multiparam !'):
                    ui.add_parameter('test multi multi param 1', 'rerlo 123', NodeParameterType.INT, 2)
                    ui.add_parameter('test multi multi param 2', 'rerlo 234', NodeParameterType.STRING, 'f q w ')

    @classmethod
    def type_name(cls) -> str:
        return 'lelolelolelo'

    def get_state(self) -> Optional[dict]:
        return {'foo': 'barr'}

    def set_state(self, state: dict):
        assert state == {'foo': 'barr'}

    def _ui_changed(self, definition_changed=False):
        print(self, '_ui_changed called')


class TestSerialization(TestCase):
    def nodes_are_same(self, node1: BaseNode, node2: BaseNode):
        # currently there is no param hierarchy comparison
        # so THIS IS A LIGHTER CHECK
        # TODO: implement proper hierarchy comparison,
        #  and replace this with just node1 == node2

        # first sanity check that we compare things we expect
        node1_dict = node1.__dict__
        node2_dict = node2.__dict__
        assert '_parameters' in node1_dict
        assert '_parameters' in node2_dict
        assert isinstance(node1_dict['_parameters'], NodeUi)
        assert isinstance(node2_dict['_parameters'], NodeUi)
        ui1: NodeUi = node1_dict.pop('_parameters')
        ui2: NodeUi = node2_dict.pop('_parameters')

        self.assertDictEqual(
            {param.name(): (param.value(), param.expression()) for param in ui1.parameters()},
            {param.name(): (param.value(), param.expression()) for param in ui2.parameters()}
        )
        self.assertDictEqual(node1_dict, node2_dict)

    def test_simple(self):
        ser = NodeSerializerV2()
        parent = TestGraphHolder()
        dataprov = TestDataProvider()

        test1 = TestNode1('footest')
        test1.set_parent(parent, 123)

        test1.param('test param 1').set_value(3.456)
        test1.param('test param 2').set_value(-42)
        test1.param('oh, multiparam !').set_value(3)
        test1.param('test multi param 1_0').set_value(True)
        test1.param('test multi param 2_0').set_value('sheeeeeesh')
        test1.param('test multi param 1_1').set_value(True)
        test1.param('test multi param 2_0').set_value('sheeeeeeeeeesh')
        test1.param('oh, mememe multiparam !_0').set_value(2)
        test1.param('oh, mememe multiparam !_2').set_value(2)
        test1.param('test multi multi param 1_0.1').set_value(345)
        test1.param('test multi multi param 1_0.0').set_value(246)
        test1.param('test multi multi param 1_2.1').set_value(456)
        test1.param('test multi multi param 1_2.0').set_value(3571)

        test1_data, test1_state = ser.serialize(test1)

        test1_act = ser.deserialize(parent, 123, dataprov, test1_data, test1_state)

        self.nodes_are_same(test1, test1_act)
