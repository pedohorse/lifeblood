from lifeblood.basenode import BaseNode
from lifeblood.nodethings import ProcessingResult, ProcessingError
from lifeblood.enums import NodeParameterType
from lifeblood.uidata import NodeUi, MultiGroupLayout, Parameter

from typing import Iterable


def node_class():
    return Wedge


class Wedge(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'wedge attributes'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'wedge', 'attribute', 'core'

    @classmethod
    def type_name(cls) -> str:
        return 'wedge'

    def __init__(self, name: str):
        super(Wedge, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            with ui.multigroup_parameter_block('wedge count'):
                with ui.parameters_on_same_line_block():
                    typeparm = ui.add_parameter('wtype', None, NodeParameterType.INT, 0)
                    typeparm.add_menu((('by count', 0),
                                       ('by inc', 1)))

                    ui.add_parameter('attr', 'attribute', NodeParameterType.STRING, 'attr1')
                    ui.add_parameter('from', 'to', NodeParameterType.FLOAT, 0.0)
                    ui.add_parameter('to', 'count', NodeParameterType.FLOAT, 9.0).append_visibility_condition(typeparm, '==', 0)
                    ui.add_parameter('count', None, NodeParameterType.INT, 10).append_visibility_condition(typeparm, '==', 0)

                    ui.add_parameter('max', 'inc by', NodeParameterType.FLOAT, 9.0).append_visibility_condition(typeparm, '==', 1)
                    ui.add_parameter('inc', None, NodeParameterType.FLOAT, 1).append_visibility_condition(typeparm, '==', 1)

    def process_task(self, context) -> ProcessingResult:
        wedges_count = context.param_value('wedge count')
        if wedges_count <= 0:
            return ProcessingResult()
        wedge_ranges = []
        for i in range(wedges_count):
            wtype = context.param_value(f'wtype_{i}')
            if wtype == 0:
                wedge_ranges.append((0, context.param_value(f'attr_{i}'), context.param_value(f'from_{i}'), context.param_value(f'to_{i}'), context.param_value(f'count_{i}')))
            elif wtype == 1:
                wedge_ranges.append((1, context.param_value(f'attr_{i}'), context.param_value(f'from_{i}'), context.param_value(f'max_{i}'), context.param_value(f'inc_{i}')))
            else:
                raise ProcessingError('bad wedge type')

        all_wedges = []

        def _do_iter(cur_vals, level=0):
            if level == wedges_count:
                all_wedges.append(cur_vals)
                return
            if wedge_ranges[level][0] == 0:
                _, attr, fr, to, cnt = wedge_ranges[level]
                for i in range(cnt):
                    new_vals = cur_vals.copy()
                    t = i * 1.0 / (cnt-1)
                    new_vals[attr] = fr*(1-t) + to*t
                    _do_iter(new_vals, level+1)
            elif wedge_ranges[level][0] == 1:
                _, attr, fr, to, inc = wedge_ranges[level]
                if inc == 0:
                    raise ProcessingError('increment cannot be zero')
                elif inc > 0:
                    while fr <= to:
                        new_vals = cur_vals.copy()
                        new_vals[attr] = fr
                        _do_iter(new_vals, level+1)
                        fr += inc
                else:
                    while fr >= to:
                        new_vals = cur_vals.copy()
                        new_vals[attr] = fr
                        _do_iter(new_vals, level + 1)
                        fr += inc

        _do_iter({})

        res = ProcessingResult()
        res.split_task(len(all_wedges))
        for i, attrs in enumerate(all_wedges):
            res.set_split_task_attribs(i, attrs)
        return res

    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()
