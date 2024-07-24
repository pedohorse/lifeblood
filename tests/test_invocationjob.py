from unittest import TestCase
import random
from string import ascii_letters
from lifeblood.invocationjob import (Requirements, ResourceRequirements, ResourceRequirement, DeviceRequirements,
                                     DeviceRequirement, Invocation, InvocationJob, InvocationResources)


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
