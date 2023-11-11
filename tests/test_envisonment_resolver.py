import os
import unittest
from unittest import mock
from lifeblood import environment_resolver
from lifeblood.invocationjob import Environment
from lifeblood.toml_coders import TomlFlatConfigEncoder
import toml
from pathlib import Path
from lifeblood import config


class PropertyMock(mock.PropertyMock):
    """
    adjusted copy of mock.PropertyMock
    """

    def __get__(self, obj, obj_type=None):
        return self(obj)

    def __set__(self, obj, val):
        self(obj, val)


class StandardEnvResTest(unittest.TestCase):
    _stash = None
    @classmethod
    def setUpClass(cls) -> None:
        cls._stash = os.environ.get('LIFEBLOOD_CONFIG_LOCATION', None)
        os.environ['LIFEBLOOD_CONFIG_LOCATION'] = os.path.join(os.path.dirname(__file__), 'environment_resolver_data')

    @classmethod
    def tearDownClass(cls) -> None:
        if cls._stash is not None:
            os.environ['LIFEBLOOD_CONFIG_LOCATION'] = cls._stash

    def test_one(self):
        ser = environment_resolver.StandardEnvironmentResolver()
        origenv = Environment(os.environ)
        env = ser.get_environment({'package.houdini': '>18.0.0,<19.0.0'})

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
        env = ser.get_environment({'package.houdini': '>18.0.0,<19.0.0',
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
        with (mock.patch('pathlib.Path.exists', autospec=True) as pexists,
              mock.patch('pathlib.Path.iterdir', autospec=True) as piterdir,
              mock.patch('pathlib.Path.is_dir', autospec=True) as pisdir,
              mock.patch('pathlib.Path.__truediv__', autospec=True) as pdiv,
              mock.patch('pathlib.Path.name', new=PropertyMock()) as pname,
              mock.patch('sys.platform', 'win32')):
            pathsep = '\\'
            pexists.side_effect = lambda self, *args, **kwargs: print(str(self)) or str(self) in existing_mock_paths
            piterdir.side_effect = lambda self, *args, **kwargs: {Path(f"{self}{pathsep}{x[len(str(self))+1:].split(pathsep, 1)[0]}") for x in existing_mock_paths if x.startswith(str(self)) and x != str(self)}
            pisdir.side_effect = lambda self: existing_mock_paths.get(str(self), False)
            pdiv.side_effect = lambda path0, path1: Path(pathsep.join((str(path0), str(path1)))) if str(path1) not in ('', '.') else path0
            pname.side_effect = lambda self: Path(str(self).rsplit(pathsep, 1)[-1])

            result = environment_resolver.StandardEnvironmentResolver.autodetect_houdini()
            print(result)
            self.assertIn('houdini.py3', result)
            self.assertEqual('C:\\Program Files\\Side Effects Software\\Houdini 19.5.640\\bin', result['houdini.py3']['19.5.640']['env']['PATH']['prepend'])
