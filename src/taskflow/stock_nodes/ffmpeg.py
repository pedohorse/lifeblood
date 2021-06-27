from math import sqrt, floor, ceil

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
        return 'ffmpeg'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'houdini', 'mantra', 'ifd', 'stock'

    def __init__(self, name):
        super(Ffmpeg, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('ffmpeg bin path', 'ffmpeg binary', NodeParameterType.STRING, 'ffmpeg')
            ui.add_parameter('fps', 'fps', NodeParameterType.INT, 24)
            ui.add_parameter('icrf', 'quality %', NodeParameterType.INT, 75).set_slider_visualization(1, 100)
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
        elif isinstance(sequence[0], list):
            # for now logic is this:
            # calc dims
            # pick first sequence, use as size ref with dims
            # woop woop
            num_items = len(sequence)
            count_h = ceil(sqrt(num_items))
            count_w = ceil(num_items/count_h)

            filter = f"color=c='Black':r={fps}[null];" \
                     f'[null][0]scale2ref=w=iw*{count_w}:h=ih*{count_h}[prebase][sink];[sink]nullsink;[prebase]setsar=1/1[base0];'

            filterlines = []
            input = 0
            for h in range(count_h):
                for w in range(count_w):
                    filterlines.append(f"[base{input}][{input}]overlay={'shortest=1:' if input == 0 else ''}x='w*{w}':y='h*{h}'[base{input+1}]")
                    input += 1
                    if input == num_items:
                        break
                if input == num_items:
                    break

            filterlines.append(f'[base{input}]null')
            filter += ';'.join(filterlines)

            args = [binpath]
            for i in range(num_items):
                args += ['-f', 'concat', '-safe', '0', '-r', fps, '-i', f':/framelist{i}.txt']
            args += ['-filter_complex', filter]
            args += [*outopts, '-y', outpath]
            job = InvocationJob(args)
            for i, subsequence in enumerate(sequence):
                framelist = '\n'.join(f'file \'{seqitem}\'' for seqitem in subsequence)  # file in ffmpeg concat format
                job.set_extra_file(f'framelist{i}.txt', framelist)
        else:
            raise RuntimeError('bad attribute value')


        res = ProcessingResult(job)
        res.set_attribute('file', outpath)
        return res



    def postprocess_task(self, task_dict) -> ProcessingResult:
        return ProcessingResult()
