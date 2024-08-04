import shutil
import sys
import os
import unittest
from typing import Mapping
from unittest import mock
from lifeblood.environment_resolver import StandardEnvironmentResolver, EnvironmentResolverArguments, BaseSimpleProcessSpawnEnvironmentResolverWithPythonCheat

from lifeblood_client import environment_resolver as client_environment_resolver
from lifeblood.invocationjob import Environment
from lifeblood.toml_coders import TomlFlatConfigEncoder
from lifeblood.process_utils import oh_no_its_windows
import toml
from pathlib import Path


class PropertyMock(mock.PropertyMock):
    """
    adjusted copy of mock.PropertyMock
    """

    def __get__(self, obj, obj_type=None):
        return self(obj)

    def __set__(self, obj, val):
        self(obj, val)


class StandardEnvResTest(unittest.IsolatedAsyncioTestCase):
    _stash = None
    @classmethod
    def setUpClass(cls) -> None:
        cls._stash = os.environ.get('LIFEBLOOD_CONFIG_LOCATION', None)
        os.environ['LIFEBLOOD_CONFIG_LOCATION'] = os.path.join(os.path.dirname(__file__), 'environment_resolver_data')

    @classmethod
    def tearDownClass(cls) -> None:
        if cls._stash is not None:
            os.environ['LIFEBLOOD_CONFIG_LOCATION'] = cls._stash

    async def test_one(self):
        ser = StandardEnvironmentResolver()
        origenv = Environment(os.environ)
        env = await ser.get_environment({'package.houdini': '>18.0.0,<19.0.0'})

        self.assertEqual(
            os.pathsep.join(["/path/to/hfs/bin", "/some/other/path/dunno", origenv.get('PATH', ''), "/whatever/you/want/to/append"]),
            env['PATH']
        )

        self.assertEqual(
            os.pathsep.join(["/dunno/smth", origenv.get('PYTHONPATH', '')]),
            env['PYTHONPATH']
        )

    async def test_two(self):
        ser = StandardEnvironmentResolver()
        origenv = Environment(os.environ)
        env = await ser.get_environment({'package.houdini': '>18.0.0,<19.0.0',
                                         'package.assmouth': '~=2.3.2'})

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

    def test_special_encoder(self):
        d = {'packages': {
            'houdini.py2': {
                '18.0.597': {
                    'env': {
                        'PATH': {
                            'prepend': '/opt/hfs18.0.597/bin',
                            'append': 'ass'
                        }
                    },
                    'label': 'ass'
                },
                '18.5.408': {
                    'env': {
                        'PATH': {
                            'prepend': '/opt/hfs18.5.408/bin'
                        },
                        'vata': [1, 2, 5]
                    }
                }
            },
            'houdini.py3': {
                '18.5.499': {
                    'env': {
                        'PATH': {
                            'prepend': '/opt/hfs18.5.499.py3/bin',
                            'append': 'me'
                        }
                    }
                }
            },
            'bonker': {
                '18.5.499': {
                    'lobe.cabe': 2.3,
                    'iii': -1,
                    'env.shmenv': {
                        'PATH.SHMATH': {
                            'prepend': '/opt/hfs18.5.499.py3/bin',
                            'append': 'me'
                        }
                    }
                }
            }
        }
        }
        r = toml.dumps(d, encoder=TomlFlatConfigEncoder())
        r2 = toml.dumps(d)
        # print(r)
        # print('\n\n============================\n\n')
        # print(r2)
        self.assertDictEqual(toml.loads(r), toml.loads(r2))

    # TODO: this is a nasty test with too many too specific mocks
    def test_autodetect_win(self):
        existing_mock_paths = {
            r'C:\Program Files\Side Effects Software': True,
            r'C:\Program Files\Side Effects Software\Houdini 19.5.640': True,
            r'C:\Program Files\Side Effects Software\Houdini 19.5.640\bin': True,
            r'C:\Program Files\Side Effects Software\Houdini 19.5.640\houdini': True,
            r'C:\Program Files\Side Effects Software\Houdini 19.5.640\python': True,
            r'C:\Program Files\Side Effects Software\Houdini 19.5.640\python\bin': True,
            r'C:\Program Files\Side Effects Software\Houdini 19.5.640\python\bin\python3.10.exe': False,
        }
        with mock.patch('pathlib.Path.exists', autospec=True) as pexists, \
             mock.patch('pathlib.Path.iterdir', autospec=True) as piterdir, \
             mock.patch('pathlib.Path.is_dir', autospec=True) as pisdir, \
             mock.patch('pathlib.Path.__truediv__', autospec=True) as pdiv, \
             mock.patch('pathlib.Path.name', new=PropertyMock()) as pname, \
             mock.patch('sys.platform', 'win32'):
            pathsep = '\\'
            pexists.side_effect = lambda self, *args, **kwargs: print(str(self)) or str(self) in existing_mock_paths
            piterdir.side_effect = lambda self, *args, **kwargs: {Path(f"{self}{pathsep}{x[len(str(self))+1:].split(pathsep, 1)[0]}") for x in existing_mock_paths if x.startswith(str(self)) and x != str(self)}
            pisdir.side_effect = lambda self: existing_mock_paths.get(str(self), False)
            pdiv.side_effect = lambda path0, path1: Path(pathsep.join((str(path0), str(path1)))) if str(path1) not in ('', '.') else path0
            pname.side_effect = lambda self: Path(str(self).rsplit(pathsep, 1)[-1])

            result = StandardEnvironmentResolver.autodetect_houdini()
            print(result)
            self.assertIn('houdini.py3_10', result)
            self.assertEqual('C:\\Program Files\\Side Effects Software\\Houdini 19.5.640\\bin', result['houdini.py3_10']['19.5.640']['env']['PATH']['prepend'])

    async def test_base_env(self):
        def add_hacks(env):
            if oh_no_its_windows and 'PYTHONIOENCODING' not in env:
                env['PYTHONIOENCODING'] = 'UTF-8'
            return env

        envres1 = StandardEnvironmentResolver()
        self.assertDictEqual(add_hacks(dict(os.environ)), await envres1.get_environment({}))
        envres2 = StandardEnvironmentResolver({})
        self.assertDictEqual(add_hacks({}), await envres2.get_environment({}))
        envres3 = StandardEnvironmentResolver({'foo': 'bar'})
        self.assertDictEqual(add_hacks({'foo': 'bar'}), await envres3.get_environment({}))


class TestMainVsClientCompatibility(unittest.TestCase):
    def test_serde1(self):
        client_envarg = client_environment_resolver.EnvironmentResolverArguments('foobar', {'abc': 'qwe', 'def': 2.3, 'ghi': ['q', 2, 3.3, {'4': []}]})
        data = client_envarg.serialize()
        envarg = EnvironmentResolverArguments.deserialize(data)

        self.assertEqual(client_envarg.name(), envarg.name())
        self.assertEqual(client_envarg.arguments(), envarg.arguments())
        self.assertEqual(client_envarg.serialize(), envarg.serialize())

    def test_serde1inv(self):
        envarg = EnvironmentResolverArguments('foobar', {'abc': 'qwe', 'def': 2.3, 'ghi': ['q', 2, 3.3, {'4': []}]})
        data = envarg.serialize()
        client_envarg = client_environment_resolver.EnvironmentResolverArguments.deserialize(data)

        self.assertEqual(client_envarg.name(), envarg.name())
        self.assertEqual(client_envarg.arguments(), envarg.arguments())
        self.assertEqual(client_envarg.serialize(), envarg.serialize())

    def test_serde2(self):
        """
        supported non-json cases
        """
        client_envarg = client_environment_resolver.EnvironmentResolverArguments(
            'foobar',
            {
                'key': (1, 4.3, (6, 5)),
                42: [11, 22, {2, 5, 7}],
                1: False,
                -999: (None, True, 5),
                'kek': {'q': 'we', 'a': 'sd'},
            }
        )
        data = client_envarg.serialize()
        envarg = EnvironmentResolverArguments.deserialize(data)

        self.assertEqual(client_envarg.name(), envarg.name())
        self.assertEqual(client_envarg.arguments(), envarg.arguments())
        self.assertEqual(client_envarg.serialize(), envarg.serialize())

    def test_serde2inv(self):
        """
        supported non-json cases
        """
        envarg = EnvironmentResolverArguments(
            'foobar',
            {
                'key': (1, 4.3, (6, 5)),
                42: [11, 22, {2, 5, 7}],
                1: False,
                -999: (None, True, 5),
                'kek': {'q': 'we', 'a': 'sd'},
            }
        )
        data = envarg.serialize()
        client_envarg = client_environment_resolver.EnvironmentResolverArguments.deserialize(data)

        self.assertEqual(client_envarg.name(), envarg.name())
        self.assertEqual(client_envarg.arguments(), envarg.arguments())
        self.assertEqual(client_envarg.serialize(), envarg.serialize())


class TestBaseSimpleProcessSpawnEnvironmentResolverWithPythonCheat(unittest.IsolatedAsyncioTestCase):
    if oh_no_its_windows:
        @staticmethod
        def normalize_path(x: str):
            """
            stoopid windows...
            """
            x = x.lower()
            if os.path.splitext(x)[1] != '.exe':
                x += '.exe'
            return x
    else:
        @staticmethod
        def normalize_path(x: str):
            return x

    class BaseSimpleBlaBlaBlaTest(BaseSimpleProcessSpawnEnvironmentResolverWithPythonCheat):
        def __init__(self, base_env):
            super().__init__()
            self.__base_env = base_env

        async def get_environment(self, arguments: Mapping) -> "Environment":
            return Environment(self.__base_env)

    async def test_create_process_no_python(self):
        envres = TestBaseSimpleProcessSpawnEnvironmentResolverWithPythonCheat.BaseSimpleBlaBlaBlaTest({})

        with mock.patch('lifeblood.environment_resolver.create_process') as m:
            await envres.create_process({}, ['python', '/foo/bar'])

        self.assertTrue(m.called)

        self.assertEqual(
            self.normalize_path(sys.executable),
            self.normalize_path(shutil.which(m.call_args[0][0][0], path=m.call_args[0][1].get('PATH', None)))
        )

    async def test_create_process_yes_python(self):
        expected_python = Path(__file__).parent / 'data' / 'fake_bin' / 'python'
        envres = TestBaseSimpleProcessSpawnEnvironmentResolverWithPythonCheat.BaseSimpleBlaBlaBlaTest({'PATH': str(expected_python.parent)})

        with mock.patch('lifeblood.environment_resolver.create_process') as m:
            await envres.create_process({}, ['python', '/foo/bar'])

        self.assertTrue(m.called)
        self.assertEqual(
            self.normalize_path(str(expected_python)),
            self.normalize_path(shutil.which(m.call_args[0][0][0], path=m.call_args[0][1].get('PATH', None)))
        )
