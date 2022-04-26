import os
import unittest
from lifeblood import environment_resolver
from lifeblood.invocationjob import Environment
from lifeblood.toml_coders import TomlFlatConfigEncoder
import toml
from lifeblood import config


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
