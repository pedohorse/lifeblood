from taskflow.basenode import BaseNode
from taskflow.nodethings import ProcessingResult
from taskflow.uidata import NodeParameterType
from taskflow.invocationjob import InvocationJob
from taskflow.invocationjob import InvocationJob, InvocationEnvironment

from typing import Iterable


def node_class():
    return Ffmpeg


class Ffmpeg(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'mantra'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'houdini', 'mantra', 'ifd', 'stock'

    def __init__(self, name):
        super(Ffmpeg, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('ffmpeg bin path', 'ffmpeg binary', NodeParameterType.STRING, 'ffmpeg')
            ui.add_parameter('fps', 'fps', NodeParameterType.INT, 24)
            ui.add_parameter('icrf', 'quality %', NodeParameterType.INT, 75)
            ui.add_parameter('-pix_fmt', 'pixel format', NodeParameterType.STRING, 'yuv420p')
            ui.add_parameter('-vcodec', 'codec', NodeParameterType.STRING, 'libx264')
            ui.add_parameter('outpath', 'movie path', NodeParameterType.STRING, '/tmp/movie.mp4')

    def process_task(self, task_dict) -> ProcessingResult:
        binpath = self.param_value('ffmpeg bin path')
        attributes = self._get_task_attributes(task_dict)
        if 'sequence' not in attributes:
            return ProcessingResult()
        sequence = attributes['sequence']

        assert isinstance(sequence, list), 'sequence attribute is supposed to be list of frames, or list of sequences'

        outopts = ['-vcodec', self.param_value('-vcodec'),
                   '-crf', 100 - self.param_value('icrf'),
                   '-pix_fmt', self.param_value('-pix_fmt')]
        fps = self.param_value('fps')
        outpath = self.param_value('outpath')

        if isinstance(sequence[0], str):  # we have sequence of frames
            args = [binpath, '-f', 'concat', '-safe', '0', '-r', fps, '-i', ':/framelist.txt', *outopts, '-y', outpath]
            job = InvocationJob(args)
            framelist = '\n'.join(f'file \'{seqitem}\'' for seqitem in sequence)  # file in ffmpeg concat format
            job.set_extra_file('framelist.txt', framelist)
        else:
            raise RuntimeError('bad attribute value')

        res = ProcessingResult(job)
        res.set_attribute('file', outpath)
        return res



    def postprocess_task(self, task_dict) -> ProcessingResult:
        return ProcessingResult()
