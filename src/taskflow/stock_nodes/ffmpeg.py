import os
from math import sqrt, floor, ceil

from taskflow.basenode import BaseNode
from taskflow.nodethings import ProcessingResult, ProcessingError
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

    @classmethod
    def type_name(cls) -> str:
        return 'ffmpeg'

    def __init__(self, name):
        super(Ffmpeg, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.125, 0.33, 0.05)
            ui.add_parameter('ffmpeg bin path', 'ffmpeg binary', NodeParameterType.STRING, 'ffmpeg')
            ui.add_parameter('fps', 'fps', NodeParameterType.INT, 24)
            ui.add_parameter('icrf', 'quality %', NodeParameterType.INT, 75).set_slider_visualization(1, 100)
            ui.add_parameter('-pix_fmt', 'pixel format', NodeParameterType.STRING, 'yuv420p')
            ui.add_parameter('-vcodec', 'codec', NodeParameterType.STRING, 'libx264')
            ui.add_parameter('outpath', 'movie path', NodeParameterType.STRING, '/tmp/movie.mp4')
            docc = ui.add_parameter('docc', 'color correct', NodeParameterType.BOOL, False)
            ui.add_parameter('cc gamma', 'gamma', NodeParameterType.FLOAT, 1.0).add_visibility_condition(docc, '==', True)

    def process_task(self, context) -> ProcessingResult:
        binpath = context.param_value('ffmpeg bin path')
        attributes = context.task_attributes()
        if 'sequence' not in attributes:
            raise ProcessingError('required attribute "sequence" not found')
        sequence = attributes['sequence']

        assert isinstance(sequence, list), 'sequence attribute is supposed to be list of frames, or list of sequences'

        outopts = ['-vcodec', context.param_value('-vcodec'),
                   '-crf', 100 - context.param_value('icrf'),
                   '-pix_fmt', context.param_value('-pix_fmt')]
        fps = context.param_value('fps')
        outpath = context.param_value('outpath')

        # TODO: MOVE THIS TO WORKER! AND MIND POTENTIAL EMBEDDED FILES IN FUTURE
        os.makedirs(os.path.dirname(outpath), exist_ok=True)

        if isinstance(sequence[0], str):  # we have sequence of frames
            filterline = None
            if context.param_value('docc'):
                gam = 1.0 / context.param_value("cc gamma")
                filterline = f'lut=r=gammaval({gam}):g=gammaval({gam}):b=gammaval({gam})'  # this does NOT work for float pixels tho. seems that nothing in FFMPEG does
            args = [binpath, '-f', 'concat', '-safe', '0', '-r', fps, '-i', ':/framelist.txt']
            if filterline:
                args += ['-filter:v', filterline]
            args += [*outopts, '-y', outpath]
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
            if context.param_value('docc'):
                gam = context.param_value("cc gamma")
                filterlines.append(f'[base{input}]lut=r=gammaval({gam}):g=gammaval({gam}):b=gammaval({gam})')  # this does NOT work for float pixels tho. seems that nothing in FFMPEG does
            else:
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


    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()
