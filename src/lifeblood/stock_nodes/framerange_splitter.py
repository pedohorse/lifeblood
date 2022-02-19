from copy import copy
from lifeblood.basenode import BaseNode
from lifeblood.enums import NodeParameterType
from lifeblood.nodethings import ProcessingResult, ProcessingError

from typing import Iterable


def node_class():
    return FramerangeSplitter


class FramerangeSplitter(BaseNode):

    @classmethod
    def label(cls) -> str:
        return 'frame range splitter'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'frame', 'framerange', 'splitter', 'stock'

    @classmethod
    def type_name(cls) -> str:
        return 'framerange_splitter'

    def __init__(self, name):
        super(FramerangeSplitter, self).__init__(name)
        with self.get_ui().initializing_interface_lock():
            self.get_ui().add_parameter('chunk size', 'chunk size in frames', NodeParameterType.INT, 10)

    def process_task(self, context):
        attrs = context.task_attributes()
        if 'frames' not in attrs:
            raise ProcessingError('attribute "frames" not found')
        frames = attrs['frames']
        if not isinstance(frames, list):
            return ProcessingResult()
        res = ProcessingResult()
        chunksize = context.param_value('chunk size')

        # res.set_attribute('frames', frames[:chunksize])
        split_into = 1 + (len(frames) - 1) // chunksize
        # yes we can split into 1 part. this should behave the same way as when splitting into multiple parts
        # in order to have consistent behaviour in the graph
        res.split_task(split_into)
        for i in range(split_into):
            res.set_split_task_attrib(i, 'frames', frames[chunksize*i:chunksize*(i+1)])
        return res

    def postprocess_task(self, context):
        return ProcessingResult()
