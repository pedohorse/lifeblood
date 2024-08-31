import os
from unittest import TestCase
import random
from string import ascii_letters
from lifeblood.invocationjob import (Requirements, ResourceRequirements, ResourceRequirement, DeviceRequirements,
                                     DeviceRequirement, Invocation, InvocationJob, InvocationResources, Environment,
                                     InvocationEnvironment)


class TestEnvironment(TestCase):
    def test_init_set_append_prepend(self):
        # init
        env0 = Environment()
        self.assertDictEqual({}, env0)
        env1 = Environment({
            'initi': 234,
            'initf': -42.11,
            'inits': "vaaaaa",
            'initincomp': None,
        })
        self.assertEqual('234', env1['initi'])
        self.assertEqual('-42.11', env1['initf'])
        self.assertEqual('vaaaaa', env1['inits'])
        self.assertEqual('None', env1['initincomp'])

        # set
        env1['fee'] = 135
        self.assertEqual('135', env1['fee'])
        env1['fee'] = 'goora'
        self.assertEqual('goora', env1['fee'])

        # append/prepend
        env1.append('fee', 'booba')
        self.assertEqual(os.pathsep.join(('goora', 'booba')), env1['fee'])
        env1.prepend('fee', 'zooga')
        self.assertEqual(os.pathsep.join(('zooga', 'goora', 'booba')), env1['fee'])
        env1.append('bar1', 'eerrtt')
        self.assertEqual('eerrtt', env1['bar1'])
        env1.append('bar2', 'zzxxcc')
        self.assertEqual('zzxxcc', env1['bar2'])

        # all
        self.assertDictEqual({
            'initi': '234',
            'initf': '-42.11',
            'inits': "vaaaaa",
            'initincomp': 'None',
            'fee': os.pathsep.join(('zooga', 'goora', 'booba')),
            'bar1': 'eerrtt',
            'bar2': 'zzxxcc',
        }, env1)

    def test_expand_variables(self):
        env = Environment({'foo': 'bar', 'hur': 'bla'})
        env.append('qwe', 'ba$foo')
        env.prepend('rty', 'we${hur}t')
        self.assertEqual('babar', env['qwe'])
        self.assertEqual('weblat', env['rty'])
        env['foo'] = '$foo'
        self.assertEqual('bar', env['foo'])
        env['foo'] = '!!${foo}@@'
        self.assertEqual('!!bar@@', env['foo'])
        env['goo'] = 'no $none'
        self.assertEqual('no ', env['goo'])


class TestInvocationEnvironment(TestCase):
    def test_resolve(self):
        env1 = InvocationEnvironment()
        self.assertDictEqual({}, env1.resolve())

        env1['fee'] = 'qet'
        env1.append('fee', 'poop')
        env1.prepend('fee', 'gloop')

        env1['ber'] = 'qq$fee'

        self.assertDictEqual({
            'fee': os.pathsep.join(('gloop', 'qet', 'poop')),
            'ber': 'qq' + os.pathsep.join(('gloop', 'qet', 'poop')),
        }, env1.resolve())

    def test_extend(self):
        env1 = InvocationEnvironment()
        env2 = InvocationEnvironment()

        env1['qwe'] = 'rty'
        env1.prepend('asd', 'fgh')

        env2.append('qwe', 'uio')
        env2['asd'] = 'fgh'

        env1.extend(env2)

        self.assertDictEqual({
            'qwe': os.pathsep.join(('rty', 'uio')),
            'asd': 'fgh',
        }, env1.resolve())

    def test_base_env(self):
        env1 = InvocationEnvironment()
        env1['asd'] = 'fgh'
        env1['zxc'] = 'bob $foo'

        self.assertDictEqual({
            'qwe': 'rty',
            'foo': 'bar',
            'asd': 'fgh',
            'zxc': 'bob bar'
        }, env1.resolve(Environment({
            'qwe': 'rty',
            'foo': 'bar',
        })))


class TestRequirements(TestCase):
    def test_serde(self):
        rng = random.Random(12432365)

        for _ in range(66):
            resreq = {}
            devreq = {}
            for i in range(rng.randint(0, 10)):
                name = ''.join(rng.choice(ascii_letters) for _ in range(rng.randint(1, 33)))
                resreq[name] = ResourceRequirement(rng.uniform(0, 100), rng.uniform(0, 100))
            for i in range(rng.randint(0, 10)):
                name = ''.join(rng.choice(ascii_letters) for _ in range(rng.randint(1, 33)))
                dresreq = {}
                for _ in range(rng.randint(0, 10)):
                    rname = ''.join(rng.choice(ascii_letters) for _ in range(rng.randint(1, 33)))
                    dresreq[rname] = ResourceRequirement(rng.uniform(0, 100), rng.uniform(0, 100))
                devreq[name] = DeviceRequirement(
                    resources=ResourceRequirements(dresreq),
                )

            expected_req = Requirements(
                resources=ResourceRequirements(resreq),
                devices=DeviceRequirements(devreq),
            )

            self.assertEqual(expected_req, Requirements.deserialize_from_string(expected_req.serialize_to_string()))


class TestInvocation(TestCase):
    def test_serde(self):
        exp_inv1 = Invocation(
            InvocationJob(
                ['beep', 'boop'],
            ),
            1234,
            4321,
            InvocationResources(
                {
                    'mem': 123456,
                    'cats': 4.4445
                },
                {
                    'wheel': ['w1', 'w2', 'q4'],
                    'dog': ['bark', 'woof']
                }
            )
        )

        self.assertEqual(exp_inv1, Invocation.deserialize_from_data(exp_inv1.serialize_to_data()))

    def test_eq1(self):
        exp_inv1 = Invocation(
            InvocationJob(
                ['beep', 'boop'],
            ),
            1234,
            4321,
            InvocationResources(
                {
                    'mem': 123456,
                    'cats': 4.4445
                },
                {
                    'wheel': ['w1', 'w2', 'q4'],
                    'dog': ['bark', 'woof']
                }
            )
        )
        exp_inv1a = Invocation(
            InvocationJob(
                ['beep', 'boop'],
            ),
            1234,
            4321,
            InvocationResources(
                {
                    'mem': 123456,
                    'cats': 4.4445
                },
                {
                    'wheel': ['w1', 'w2', 'q4'],
                    'dog': ['bark', 'woof']
                }
            )
        )
        exp_inv2 = Invocation(
            InvocationJob(
                ['boop'],
            ),
            1234,
            4321,
            InvocationResources(
                {
                    'mem': 123456,
                    'cats': 4.4445
                },
                {
                    'wheel': ['w1', 'w2', 'q4'],
                    'dog': ['bark', 'woof']
                }
            )
        )
        exp_inv3 = Invocation(
            InvocationJob(
                ['beep', 'boop'],
            ),
            5555,
            4321,
            InvocationResources(
                {
                    'mem': 123456,
                    'cats': 4.4445
                },
                {
                    'wheel': ['w1', 'w2', 'q4'],
                    'dog': ['bark', 'woof']
                }
            )
        )
        exp_inv4 = Invocation(
            InvocationJob(
                ['beep', 'boop'],
            ),
            1234,
            5555,
            InvocationResources(
                {
                    'mem': 123456,
                    'cats': 4.4445
                },
                {
                    'wheel': ['w1', 'w2', 'q4'],
                    'dog': ['bark', 'woof']
                }
            )
        )
        exp_inv5 = Invocation(
            InvocationJob(
                ['beep', 'boop'],
            ),
            1234,
            4321,
            InvocationResources(
                {},
                {}
            )
        )
        exp_inv6 = Invocation(
            InvocationJob(
                ['beep', 'boop'],
            ),
            1234,
            4321,
            InvocationResources(
                {
                    'mam': 123456,
                    'cats': 4.4445
                },
                {
                    'wheel': ['w1', 'w2', 'q4'],
                    'dog': ['bark', 'woof']
                }
            )
        )
        exp_inv7 = Invocation(
            InvocationJob(
                ['beep', 'boop'],
            ),
            1234,
            4321,
            InvocationResources(
                {
                    'mem': 123457,
                    'cats': 4.4445
                },
                {
                    'wheel': ['w1', 'w2', 'q4'],
                    'dog': ['bark', 'woof']
                }
            )
        )
        exp_inv8 = Invocation(
            InvocationJob(
                ['beep', 'boop'],
            ),
            1234,
            4321,
            InvocationResources(
                {
                    'mem': 123456,
                    'cats': 4.4445
                },
                {
                    'wheel': ['w1', 'w2', 'q4'],
                    'dog': ['hurk', 'woof']
                }
            )
        )

        self.assertEqual(exp_inv1, exp_inv1)
        self.assertEqual(exp_inv1, exp_inv1a)
        self.assertEqual(exp_inv1a, exp_inv1a)
        self.assertEqual(exp_inv2, exp_inv2)
        self.assertNotEqual(exp_inv1, exp_inv2)
        self.assertNotEqual(exp_inv2, exp_inv1)
        self.assertEqual(exp_inv3, exp_inv3)
        self.assertNotEqual(exp_inv1, exp_inv3)
        self.assertNotEqual(exp_inv3, exp_inv1)
        self.assertEqual(exp_inv4, exp_inv4)
        self.assertNotEqual(exp_inv1, exp_inv4)
        self.assertNotEqual(exp_inv4, exp_inv1)
        self.assertEqual(exp_inv5, exp_inv5)
        self.assertNotEqual(exp_inv1, exp_inv5)
        self.assertNotEqual(exp_inv5, exp_inv1)
        self.assertEqual(exp_inv6, exp_inv6)
        self.assertNotEqual(exp_inv1, exp_inv6)
        self.assertNotEqual(exp_inv6, exp_inv1)
        self.assertEqual(exp_inv7, exp_inv7)
        self.assertNotEqual(exp_inv1, exp_inv7)
        self.assertNotEqual(exp_inv7, exp_inv1)
        self.assertEqual(exp_inv8, exp_inv8)
        self.assertNotEqual(exp_inv1, exp_inv8)
        self.assertNotEqual(exp_inv8, exp_inv1)
