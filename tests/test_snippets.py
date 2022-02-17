from unittest import TestCase
from random import Random
import string
from lifeblood.snippets import NodeSnippetData
from lifeblood.enums import NodeParameterType


class SerializationTest(TestCase):

    def create_random_data(self, seed) -> NodeSnippetData:
        rng = Random(seed)
        nodes = []
        nodes_count = rng.randint(0, 333)
        for i in range(nodes_count):
            params = {}
            for _ in range(rng.randint(0, 100)):
                paramname = ''.join(rng.choice(string.ascii_letters) for _ in range(rng.randint(1, 32)))
                paramtype = rng.choice(list(NodeParameterType))
                if paramtype == NodeParameterType.STRING:
                    paramval = ''.join(rng.choice(string.ascii_letters) for _ in range(rng.randint(1, 32)))
                elif paramtype == NodeParameterType.INT:
                    paramval = rng.randint(-999999, 999999)
                elif paramtype == NodeParameterType.BOOL:
                    paramval = rng.random() > 0.5
                elif paramtype == NodeParameterType.FLOAT:
                    paramval = rng.uniform(-999999, 999999)
                else:
                    raise NotImplementedError()
                paramexpr = None
                if rng.random() > 0.1:
                    paramexpr = ''.join(rng.choice(string.ascii_letters) for _ in range(rng.randint(1, 256)))
                params[paramname] = NodeSnippetData.ParamData(paramname, paramtype, paramval, paramexpr)
            typename = ''.join(rng.choice(string.ascii_letters) for _ in range(rng.randint(1, 32)))
            name = ''.join(rng.choice(string.ascii_letters) for _ in range(rng.randint(1, 32)))
            nodes.append(NodeSnippetData.NodeData(i, typename, name, params, (rng.uniform(-100, 100), rng.uniform(-100, 100))))
        conns = []
        if nodes_count > 0:
            for i in range(rng.randint(0, 666)):
                conns.append(NodeSnippetData.ConnData(rng.randint(0, nodes_count-1),
                                                      ''.join(rng.choice(string.ascii_letters) for _ in range(rng.randint(1, 32))),
                                                      rng.randint(0, nodes_count - 1),
                                                      ''.join(rng.choice(string.ascii_letters) for _ in range(rng.randint(1, 32))),
                                                      ))
        label = None
        if rng.random() > 0.1:
            label = ''.join(rng.choice(string.ascii_letters) for _ in range(rng.randint(1, 32)))
        tags = None
        if rng.random() > 0.1:
            tags = [''.join(rng.choice(string.ascii_letters) for _ in range(rng.randint(1, 32))) for _ in range(rng.randint(0, 13))]
        return NodeSnippetData(nodes, conns, label, tags)

    def test_simple(self):
        x = NodeSnippetData([NodeSnippetData.NodeData(0, 'foo', 'bar',
                                                      {'qwe': NodeSnippetData.ParamData('asd',
                                                                                        NodeParameterType.FLOAT,
                                                                                        0.1,
                                                                                        None)
                                                       }, (1.2, 3.4))],
                            [NodeSnippetData.ConnData(0, 'ass', 0, 'mouth')])

        # print(x.serialize().decode('UTF-8'))
        self.assertEqual(NodeSnippetData.deserialize(x.serialize()), x)

    def test_random(self):
        for i in range(16):
            x = self.create_random_data(i)
            y = self.create_random_data(i)

            self.assertEqual(x, y)
            ascii = Random(i*1.234).random() > 0.5
            self.assertEqual(NodeSnippetData.deserialize(x.serialize(ascii)), x)
