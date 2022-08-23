from copy import copy
from lifeblood.basenode import BaseNode
from lifeblood.enums import NodeParameterType
from lifeblood.nodethings import ProcessingResult, ProcessingError
import math

from typing import Iterable


def node_class():
    return FramerangeSplitter


class FramerangeSplitter(BaseNode):

    @classmethod
    def label(cls) -> str:
        return 'attribute splitter'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'attribute', 'splitter', 'core'

    @classmethod
    def type_name(cls) -> str:
        return 'attribute_splitter'

    @classmethod
    def description(cls) -> str:
        return 'splits task\'s attribute value into chunks of "chunk size" elements\n' \
               'Works ONLY on array/list attributes\n' \
               'NOTE that task wil ALWAYS be splitted, even if just in 1 piece\n' \
               '\n' \
               'for ex. attribute "something" of value [3,4,5,6,7,8,9,10,11,12,13]\n' \
               'with chunk size of 4\n' \
               'will split task into 3 tasks with attributes "something" equals to:\n' \
               '[3,4,5,6]  [7,8,9,10]  [11,12,13]  respectivelly'

    def __init__(self, name):
        super(FramerangeSplitter, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            mode_param = ui.add_parameter('split type', 'mode', NodeParameterType.STRING, 'list') \
                            .add_menu((('Split List', 'list'), ('Split Range', 'range')))
            ui.add_separator()
            ui.add_parameter('attribute name', 'attribute value to split', NodeParameterType.STRING, 'frames').append_visibility_condition(mode_param, '==', 'list')
            ui.add_parameter('chunk size', 'chunk size', NodeParameterType.INT, 10).append_visibility_condition(mode_param, '==', 'list')

            range_mode_param = ui.add_parameter('range mode', 'Range Mode', NodeParameterType.INT, 0) \
                                    .add_menu((('start-end', 0), ('start-size', 1), ('start-end-size', 2))) \
                                    .append_visibility_condition(mode_param, '==', 'range')
            with ui.parameters_on_same_line_block():
                ui.add_parameter('range start', 'Start', NodeParameterType.FLOAT, 0).append_visibility_condition(mode_param, '==', 'range')
                ui.add_parameter('range end', 'End', NodeParameterType.FLOAT, 100).append_visibility_condition(mode_param, '==', 'range')
                ui.add_parameter('range inc', 'Inc', NodeParameterType.FLOAT, 1).append_visibility_condition(mode_param, '==', 'range')
            split_by_param = ui.add_parameter('range split by', 'Split By', NodeParameterType.INT, 0) \
                                .add_menu((('chunk size', 0), ('number of chunks', 1))) \
                                .append_visibility_condition(mode_param, '==', 'range')
            ui.add_parameter('range chunk', 'Chunk Size', NodeParameterType.INT, 10) \
                .append_visibility_condition(mode_param, '==', 'range') \
                .append_visibility_condition(split_by_param, '==', 0)
            ui.add_parameter('range count', 'Chunks Count', NodeParameterType.INT, 10) \
                .append_visibility_condition(mode_param, '==', 'range') \
                .append_visibility_condition(split_by_param, '==', 1)

            ui.add_parameter('out range type', 'Type', NodeParameterType.INT, 0) \
                .add_menu((('Int', 0), ('Float', 1))) \
                .append_visibility_condition(mode_param, '==', 'range')

            ui.add_separator()
            ui.add_parameter('out start name', 'Output Range Start Attribute Name', NodeParameterType.STRING, 'chunk_start') \
                .append_visibility_condition(mode_param, '==', 'range')
            ui.add_parameter('out end name', 'Output Range End Attribute Name', NodeParameterType.STRING, 'chunk_end') \
                .append_visibility_condition(mode_param, '==', 'range') \
                .append_visibility_condition(range_mode_param, 'in', (0, 2))
            ui.add_parameter('out size name', 'Output Range Size Attribute Name', NodeParameterType.STRING, 'chunk_size') \
                .append_visibility_condition(mode_param, '==', 'range') \
                .append_visibility_condition(range_mode_param, 'in', (1, 2))

    @staticmethod
    def _ranges_from_chunksize(start, end, inc, chunk_size):
        if chunk_size <= 0:
            raise ProcessingError('chunk size must be positive')
        ranges = []
        cur = start
        while cur < end:
            count_till_end = math.ceil((end - cur) / inc)
            if count_till_end == 0:
                break
            ranges.append((cur, min(end, cur + inc * min(count_till_end, chunk_size)), min(count_till_end, chunk_size)))
            cur = ranges[-1][1]
        return ranges

    @staticmethod
    def _ranges_from_chunkcount(start, end, inc, chunk_count):
        if chunk_count <= 0:
            raise ProcessingError('chunk count must be positive')
        ranges = []
        print(f'- {start}, {end} :{inc}')
        for i in range(chunk_count):
            e1 = start + math.ceil(((end - start) / chunk_count * i) / inc) * inc
            e2 = min(end, start + math.ceil(((end - start) / chunk_count * (i + 1)) / inc) * inc)
            print(e1, e2)
            if e1 >= e2:  # case where chunk_count is bigger than possible
                continue
            new_range = (e1, e2, math.ceil((e2-e1)/inc))
            ranges.append(new_range)
        return ranges

    def process_task(self, context):
        mode = context.param_value('split type')

        if mode == 'list':
            attrs = context.task_attributes()
            attr_name = context.param_value('attribute name')
            if attr_name not in attrs:
                raise ProcessingError('attribute "frames" not found')
            attr_value = attrs[attr_name]
            if not isinstance(attr_value, list):
                raise ProcessingError(f'attribute "{attr_name}" must be a list')
            res = ProcessingResult()
            chunksize = context.param_value('chunk size')

            split_into = 1 + (len(attr_value) - 1) // chunksize
            # yes we can split into 1 part. this should behave the same way as when splitting into multiple parts
            # in order to have consistent behaviour in the graph
            res.split_task(split_into)
            for i in range(split_into):
                res.set_split_task_attrib(i, attr_name, attr_value[chunksize*i:chunksize*(i+1)])
            return res
        elif mode == 'range':
            res = ProcessingResult()
            start = context.param_value('range start')
            end = context.param_value('range end')
            inc = context.param_value('range inc')

            rtype = context.param_value('out range type')
            split_by_type = context.param_value('range split by')
            if rtype == 0:  # int
                start = int(start)
                end = int(end)
                inc = int(inc)

            if inc <= 0:
                raise ProcessingError('inc must be positive')
            if end <= start:  # not equal cuz end specifies NEXT element after last
                raise ProcessingError('end cannot be less or equal than start')

            if split_by_type == 0:  # chunk size
                chunk_size = context.param_value('range chunk')
                ranges = self._ranges_from_chunksize(start, end, inc, chunk_size)
            elif split_by_type == 1:  # number of chunks
                chunk_count = context.param_value('range count')
                ranges = self._ranges_from_chunkcount(start, end, inc, chunk_count)
            else:
                raise NotImplementedError('wtf is this split-by mode?')

            rstart_name = context.param_value('out start name')
            rend_name = context.param_value('out end name')
            rsize_name = context.param_value('out size name')

            rmode = context.param_value('range mode')
            res.split_task(len(ranges))
            for i, (start, end, count) in enumerate(ranges):
                res.set_split_task_attrib(i, rstart_name, start)
                if rmode in (0, 2):
                    res.set_split_task_attrib(i, rend_name, end)
                if rmode in (1, 2):
                    res.set_split_task_attrib(i, rsize_name, count)
            return res
        else:
            raise NotImplementedError(f'mode "{mode}" is not implemented')


if __name__ == '__main__':
    # some fast tests
    r = FramerangeSplitter._ranges_from_chunkcount(10, 12, 3, 10)
    assert r == [(10, 12, 1)], r
    r = FramerangeSplitter._ranges_from_chunkcount(10, 15, 3, 10)
    assert r == [(10, 13, 1), (13, 15, 1)], r
    r = FramerangeSplitter._ranges_from_chunkcount(15, 16, 3, 10)
    assert r == [(15, 16, 1)], r
    r = FramerangeSplitter._ranges_from_chunkcount(15, 29, 3, 3)
    assert r == [(15, 21, 2), (21, 27, 2), (27, 29, 1)], r
    r = FramerangeSplitter._ranges_from_chunkcount(15, 29, 3, 4)
    assert r == [(15, 21, 2), (21, 24, 1), (24, 27, 1), (27, 29, 1)], r

    r = FramerangeSplitter._ranges_from_chunksize(10, 12, 3, 1)
    assert r == [(10, 12, 1)], r
    r = FramerangeSplitter._ranges_from_chunksize(10, 12, 3, 10)
    assert r == [(10, 12, 1)], r
    r = FramerangeSplitter._ranges_from_chunksize(10, 15, 3, 1)
    assert r == [(10, 13, 1), (13, 15, 1)], r
    r = FramerangeSplitter._ranges_from_chunksize(10, 15, 3, 10)
    assert r == [(10, 15, 2)], r
    r = FramerangeSplitter._ranges_from_chunksize(15, 16, 3, 10)
    assert r == [(15, 16, 1)], r
    r = FramerangeSplitter._ranges_from_chunksize(15, 38, 3, 3)
    assert r == [(15, 24, 3), (24, 33, 3), (33, 38, 2)], r
