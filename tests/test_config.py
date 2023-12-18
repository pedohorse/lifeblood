import os
import unittest
import shutil
import toml  # for raw file comparison
from lifeblood.config import get_config


class StandardConfigTest(unittest.TestCase):
    _stash = None
    config_base_path = None

    @classmethod
    def setUpClass(cls) -> None:
        cls._stash = os.environ.get('LIFEBLOOD_CONFIG_LOCATION', None)
        conf_orig_path = os.path.join(os.path.dirname(__file__), 'config_data', 'orig')
        conf_test_path = os.path.join(os.path.dirname(__file__), 'config_data', 'test')
        if os.path.exists(conf_test_path):
            shutil.rmtree(conf_test_path)
        shutil.copytree(conf_orig_path, conf_test_path)
        os.environ['LIFEBLOOD_CONFIG_LOCATION'] = conf_test_path
        cls.config_base_path = conf_test_path

    @classmethod
    def tearDownClass(cls) -> None:
        if cls._stash is not None:
            os.environ['LIFEBLOOD_CONFIG_LOCATION'] = cls._stash
        else:
            del os.environ['LIFEBLOOD_CONFIG_LOCATION']

        if os.path.exists(cls.config_base_path):
            shutil.rmtree(cls.config_base_path)

    def assert_toml_contents(self, dict_expected, toml_file):
        with open(toml_file, 'r') as f:
            actual = toml.load(f)
        self.assertDictEqual(dict_expected, actual)

    def test_basic_rw(self):
        config = get_config('foobar')
        expected_config_file_path = os.path.join(self.config_base_path, 'foobar', 'config.toml')
        self.assertEqual(expected_config_file_path, str(config.writeable_file()))

        self.assertEqual('woof', config.get_option_noasync('crap.ass'))
        self.assertEqual(12.3, config.get_option_noasync('crap.bob'))
        self.assertDictEqual({'ass': 'woof', 'bob': 12.3}, config.get_option_noasync('crap'))
        self.assert_toml_contents({'crap': {'ass': 'woof', 'bob': 12.3}}, expected_config_file_path)

        config.set_option_noasync('simple', 42)
        self.assertEqual(42, config.get_option_noasync('simple'))
        config.set_option_noasync('double.one', 'qwe')
        self.assertEqual('qwe', config.get_option_noasync('double.one'))
        config.set_option_noasync('double.two', 'asd')
        self.assertEqual('qwe', config.get_option_noasync('double.one'))
        self.assertEqual('asd', config.get_option_noasync('double.two'))
        self.assertDictEqual({'one': 'qwe', 'two': 'asd'}, config.get_option_noasync('double'))
        self.assert_toml_contents({'crap': {'ass': 'woof', 'bob': 12.3},
                                   'simple': 42,
                                   'double': {'one': 'qwe', 'two': 'asd'}}, expected_config_file_path)

        config.reload()
        self.assertDictEqual({'ass': 'woof', 'bob': 12.3}, config.get_option_noasync('crap'))
        self.assertEqual('qwe', config.get_option_noasync('double.one'))
        self.assertEqual('asd', config.get_option_noasync('double.two'))
        self.assertDictEqual({'one': 'qwe', 'two': 'asd'}, config.get_option_noasync('double'))

    def test_entries_with_dot(self):
        config = get_config('foofar')
        expected_config_file_path = os.path.join(self.config_base_path, 'foofar', 'config.toml')
        self.assertEqual(expected_config_file_path, str(config.writeable_file()))
        config.set_option_noasync('foo."baka.shaka"', 42)

        self.assert_toml_contents({'foo': {'baka.shaka': 42}}, expected_config_file_path)

        self.assertEqual(42, config.get_option_noasync('foo."baka.shaka"'))
        self.assertDictEqual({"baka.shaka": 42}, config.get_option_noasync('foo'))

        config.set_option_noasync('bar."qwe.asd"."foof"."nana.k.."', [1, 2, 3])
        self.assert_toml_contents({'foo': {'baka.shaka': 42},
                                   'bar': {"qwe.asd": {'foof': {'nana.k..': [1, 2, 3]}}}}, expected_config_file_path)
        self.assertListEqual([1, 2, 3], config.get_option_noasync('bar."qwe.asd"."foof"."nana.k.."'))

    def test_condigd(self):
        config = get_config('boofar')
        expected_config_file_path = os.path.join(self.config_base_path, 'boofar', 'config.toml')
        self.assertEqual(expected_config_file_path, str(config.writeable_file()))

        self.assertDictEqual({'one': 13.44, 'two': 'beep', 'three': 456}, config.get_option_noasync('main'))
        self.assertDictEqual({'wow': 'cat'}, config.get_option_noasync('some'))
        self.assertDictEqual({'wee': 'so much'}, config.get_option_noasync('body'))

        config.reload()
        self.assertDictEqual({'one': 13.44, 'two': 'beep', 'three': 456}, config.get_option_noasync('main'))
        self.assertDictEqual({'wow': 'cat'}, config.get_option_noasync('some'))
        self.assertDictEqual({'wee': 'so much'}, config.get_option_noasync('body'))


class DefaultComponentConfigTest(unittest.TestCase):
    def test_default_schediler(self):
        from lifeblood.main_scheduler import default_config
        data = toml.loads(default_config)
        self.assertIn('scheduler', data)
        self.assertIn('globals', data['scheduler'])
        self.assertNotEqual('', data['scheduler']['globals']['global_scratch_location'])

    def test_default_worker(self):
        """
        simply checking that config is a valid toml
        """
        from lifeblood.main_worker import default_config
        data = toml.loads(default_config)

    def test_default_viewer(self):
        """
        simply checking that config is a valid toml
        """
        from lifeblood_viewer import default_config
        data = toml.loads(default_config)
