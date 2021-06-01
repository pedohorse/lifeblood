from copy import copy
from taskflow.basenode import BaseNode
from taskflow.enums import NodeParameterType
from taskflow.nodethings import ProcessingResult, TaskSpawn

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

    def __init__(self, name):
        super(FramerangeSplitter, self).__init__(name)
        with self.get_ui().initializing_interface_lock():
            self.get_ui().add_parameter('chunk size', None, NodeParameterType.INT, 10)

    def process_task(self, task_row):
        attrs = self._get_task_attributes(task_row)
        if 'frames' not in attrs:
            return ProcessingResult()
        frames = attrs['frames']
        if not isinstance(frames, list):
            return ProcessingResult()
        res = ProcessingResult()
        chunksize = self.param_value('chunk size')
        if len(frames) < chunksize:
            return ProcessingResult()

        res.set_attribute('frames', frames[:chunksize])
        for i in range(1, 1 + (len(frames) - 1) // chunksize):
            newattrs = copy(attrs)
            newattrs['frames'] = frames[chunksize*i:chunksize*(i+1)]
            spawn = TaskSpawn((task_row.get('name', None) or '') + '_part%d' % i, None, **newattrs)
            spawn.set_node_output_name('main')
            spawn.force_set_node_task_id(task_row['node_id'], task_row['id'])
            res.add_spawned_task(spawn)
        return res

    def postprocess_task(self, task_row):
        return ProcessingResult()
