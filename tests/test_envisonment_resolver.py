import os
import unittest
from taskflow import environment_resolver
from taskflow.invocationjob import Environment
from taskflow import config


class StandardEnvResTest(unittest.TestCase):
    _stash = None
    @classmethod
    def setUpClass(cls) -> None:
        cls._stash = os.environ.get('TASKFLOW_CONFIG_LOCATION', None)
        os.environ['TASKFLOW_CONFIG_LOCATION'] = os.path.join(os.path.dirname(__file__), 'environment_resolver_data')

    @classmethod
    def tearDownClass(cls) -> None:
        if cls._stash is not None:
            os.environ['TASKFLOW_CONFIG_LOCATION'] = cls._stash

    def test_one(self):
        ser = environment_resolver.StandardEnvironmentResolver()
        origenv = Environment(os.environ)
        env = ser.get_environment({'houdini': '>18.0.0,<19.0.0'})

        self.assertEqual(
            os.pathsep.join(["/path/to/hfs/bin", "/some/other/path/dunno", origenv.get('PATH', ''), "/whatever/you/want/to/append"]),
            env['PATH']
        )

        self.assertEqual(
            os.pathsep.join(["/dunno/smth", origenv.get('PYTHONPATH', '')]),
            env['PYTHONPATH']
        )

    def test_two(self):
        ser = environment_resolver.StandardEnvironmentResolver()
        origenv = Environment(os.environ)
        env = ser.get_environment({'houdini': '>18.0.0,<19.0.0',
                                   'assmouth': '~=2.3.2'})

        self.assertEqual(
            os.pathsep.join(["/path/to/hfs/bin", "/some/other/path/dunno", "bananus", origenv.get('PATH', ''), "who/are/you", "/whatever/you/want/to/append"]),
            env['PATH']
        )

        self.assertEqual(
            os.pathsep.join(["/dunno/smth", origenv.get('PYTHONPATH', '')]),
            env['PYTHONPATH']
        )

        self.assertEqual(
            'yEs',
            env['WooF']
        )
