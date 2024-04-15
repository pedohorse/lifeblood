from unittest import TestCase
import json
from lifeblood.attribute_serialization import serialize_attributes_core, deserialize_attributes_core
from lifeblood_client.attribute_serialization import serialize_attributes_core as serialize_attributes_core_client, deserialize_attributes_core as deserialize_attributes_core_client
from lifeblood.worker_runtime_pythonpath.lifeblood_connection import serialize_attributes_core as serialize_attributes_core_lbcon, deserialize_attributes_core as deserialize_attributes_core_lbcon


class TestAttributeSerialization(TestCase):
    def test_basic(self):
        exp_attrs = {
            'ass': 'bar',
            'bar': 123,
            'car': 4.56,
            'doc': [2, 5, 7],
            'ecc': (3, 6, 8),
            'foo': {
                'blob': {'q': 'wer', 'a': 'sdf'},
                'alala': [1, 'q', ['e']]
            },
            'geo': {12, 23, 34},
            'haa': {
                12: 'bebe',
                "three": 3.3,
            },
            'iso': [(1, ' a ',), 'blb', {1: 1}, {6.6, 5.5}],
        }

        self.assertDictEqual(exp_attrs, deserialize_attributes_core(serialize_attributes_core(exp_attrs)))

        self.assertDictEqual(exp_attrs, deserialize_attributes_core(serialize_attributes_core_client(exp_attrs)))
        self.assertDictEqual(exp_attrs, deserialize_attributes_core_client(serialize_attributes_core(exp_attrs)))

        self.assertDictEqual(exp_attrs, deserialize_attributes_core(serialize_attributes_core_lbcon(exp_attrs)))
        self.assertDictEqual(exp_attrs, deserialize_attributes_core_lbcon(serialize_attributes_core(exp_attrs)))

    def test_json_compatibility(self):
        exp_attrs = {
            'ass': 'bar',
            'bar': 123,
            'car': 4.56,
            'doc': [2, 5, 7],
            'foo': {
                'blob': {'q': 'wer', 'a': 'sdf'},
                'alala': [1, 'q', ['e']]
            },
        }

        self.assertDictEqual(exp_attrs, deserialize_attributes_core(json.dumps(exp_attrs)))
