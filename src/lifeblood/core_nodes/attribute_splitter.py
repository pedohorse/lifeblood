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
            ui.add_parameter('attribute name', 'attribute value to split', NodeParameterType.STRING, 'frames')
            ui.add_parameter('chunk size', 'chunk size', NodeParameterType.INT, 10)

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
