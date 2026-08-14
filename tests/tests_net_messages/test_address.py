from unittest import TestCase
from lifeblood.net_messages.address import AddressChain

class TestAddress(TestCase):
    def test_invalid_address(self):
        self.assertFalse(
            AddressChain(None).is_valid()
        )
        self.assertFalse(
            AddressChain('').is_valid()
        )

    def test_valid_address(self):
        self.assertTrue(
            AddressChain('something').is_valid()
        )
        self.assertTrue(
            AddressChain('foo.net:1234').is_valid()
        )
        self.assertTrue(
            AddressChain('123.234.345.456:1515|234.123.345.135:9999').is_valid()
        )

    def test_incorrect_address(self):
        self.assertRaises(
            ValueError,
            AddressChain, '|'
        )
        self.assertRaises(
            ValueError,
            AddressChain, 'addr|'
        )
        self.assertRaises(
            ValueError,
            AddressChain, '|addr'
        )
        self.assertRaises(
            ValueError,
            AddressChain, '|addr|'
        )
