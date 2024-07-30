import os
from pathlib import Path
import unittest
import shutil
import toml  # for raw file comparison
from lifeblood.config import get_config, Config
from lifeblood.scheduler_config_provider_file import SchedulerConfigProviderFile

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

    def test_configd(self):
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

    def test_save_as_copy(self):
        for overrides in ({}, {'main': {'one': 666}}, {'some': {'wow': 'dog'}}):
            config = Config('boofar')
            config1 = Config('boofar')
            self.assertEqual(config, config1)

            if overrides:
                config.set_overrides(overrides)
                config1.set_overrides(overrides)
            other_config_path = Path(self.config_base_path) / '_tests_' / 'config.toml'
            config.save_as_copy(other_config_path, collapse_overrides=False)

            # ensure config was not changed
            self.assertEqual(config, config1)

            config2 = Config(str(other_config_path.parent))
            if overrides:
                self.assertFalse(config.is_same_as(config2))  # not same without overrides
                config2.set_overrides(overrides)
            # ensure saved config is the same
            self.assertNotEqual(config, config2)  # not equal cuz loaded from different places
            self.assertTrue(config.is_same_as(config2))

            if not overrides:
                continue

            config.save_as_copy(other_config_path, collapse_overrides=True)
            config2 = Config(str(other_config_path.parent))

            # ensure config was not changed
            self.assertEqual(config, config1)

            # ensure saved config is the same, but since overrides are collapsed in config2 - they are same, but not equal
            self.assertNotEqual(config, config2)
            self.assertTrue(config.is_same_as(config2))

    def test_save_as(self):
        for overrides in ({}, {'main': {'one': 666}}, {'some': {'wow': 'dog'}}):
            config = Config('boofar')
            config1 = Config('boofar')
            self.assertEqual(config, config1)

            if overrides:
                config.set_overrides(overrides)
                config1.set_overrides(overrides)
            other_config_path = Path(self.config_base_path) / '_tests1_' / 'config.toml'
            config.save_as(other_config_path, collapse_overrides=False)

            config2 = Config(str(other_config_path.parent))

            if overrides:
                self.assertNotEqual(config, config2)
                config2.set_overrides(overrides)
            self.assertEqual(config, config2)


class DefaultComponentConfigTest(unittest.TestCase):
    def test_default_scheduler(self):
        config_text = SchedulerConfigProviderFile.generate_default_config_text()
        data = toml.loads(config_text)
        # not much we can test just like that,
        # TODO: ideally would be to test for existence of certain form of commented entries, values of which bein equal to defaults.
        self.assertIn('nodes', data)
        self.assertIn('globals', data['nodes'])
        self.assertNotEqual('', data['nodes']['globals']['global_scratch_location'])

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
