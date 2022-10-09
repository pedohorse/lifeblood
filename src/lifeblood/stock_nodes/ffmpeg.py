import os
from math import sqrt, floor, ceil

from lifeblood.basenode import BaseNode
from lifeblood.nodethings import ProcessingResult, ProcessingError
from lifeblood.uidata import NodeParameterType
from lifeblood.invocationjob import InvocationJob
from lifeblood.invocationjob import InvocationJob, InvocationEnvironment

from typing import Iterable


def node_class():
    return Ffmpeg


class Ffmpeg(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'ffmpeg'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'ffmpeg', 'video', 'convert'

    @classmethod
    def type_name(cls) -> str:
        return 'ffmpeg'

    def __init__(self, name):
        super(Ffmpeg, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.125, 0.33, 0.05)
            ui.add_parameter('sequence', 'sequence attribute name', NodeParameterType.STRING, 'sequence')
            ui.add_parameter('ffmpeg bin path', 'ffmpeg binary', NodeParameterType.STRING, 'ffmpeg')
            ui.add_parameter('fps', 'fps', NodeParameterType.INT, 24)
            ui.add_parameter('icrf', 'quality %', NodeParameterType.INT, 75).set_slider_visualization(1, 100)
            ui.add_parameter('-pix_fmt', 'pixel format', NodeParameterType.STRING, 'yuv420p')
            ui.add_parameter('-vcodec', 'codec', NodeParameterType.STRING, 'libx264')
            ui.add_parameter('outpath', 'movie path', NodeParameterType.STRING, '/tmp/movie.mp4')
            docc = ui.add_parameter('docc', 'color correct', NodeParameterType.BOOL, False)
            ui.add_parameter('cc gamma', 'gamma', NodeParameterType.FLOAT, 1.0).append_visibility_condition(docc, '==', True)

    def process_task(self, context) -> ProcessingResult:
        binpath = context.param_value('ffmpeg bin path')
        attributes = context.task_attributes()
        sequence_attrib_name = context.param_value('sequence')
        if sequence_attrib_name not in attributes:
            raise ProcessingError(f'required attribute "{sequence_attrib_name}" not found')
        sequence = attributes[sequence_attrib_name]

        assert isinstance(sequence, list), 'sequence attribute is supposed to be list of frames, or list of sequences'

        outopts = ['-vcodec', context.param_value('-vcodec'),
                   '-crf', 100 - context.param_value('icrf'),
                   '-pix_fmt', context.param_value('-pix_fmt')]
        fps = context.param_value('fps')
        outpath = context.param_value('outpath')

        # TODO: MOVE THIS TO WORKER! AND MIND POTENTIAL EMBEDDED FILES IN FUTURE
        os.makedirs(os.path.dirname(outpath), exist_ok=True)

        if isinstance(sequence[0], str):  # we have sequence of frames
            filterlines = []
            filterlines.append(f"color=c='Black':r={fps}[null];"
                               f'[null][0]scale2ref=w=iw:h=ih[prebase][sink];[sink]nullsink;[prebase]setsar=1/1[base0];'
                               f'[base0][0]overlay=shortest=1')

            if context.param_value('docc'):
                gam = context.param_value("cc gamma")
                filterlines.append(f'lutrgb=r=gammaval({gam}):g=gammaval({gam}):b=gammaval({gam})')  # this does NOT work for float pixels tho. seems that nothing in FFMPEG does
            args = [binpath, '-f', 'concat', '-safe', '0', '-r', fps, '-i', ':/framelist.txt']
            args += ['-filter_complex', ','.join(filterlines)]  # , here - cuz we just chain those guys together. ";" is for separating chains, not filters in single chain as it is here
            args += [*outopts, '-y', outpath]
            job = InvocationJob(args)
            framelist = '\n'.join(f'file \'{seqitem}\'' for seqitem in sequence)  # file in ffmpeg concat format
            job.set_extra_file('framelist.txt', framelist)
        elif isinstance(sequence[0], list):  # WTF is this? :) i forgot... but #TODO: black base for image needed in case above too
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
                filterlines.append(f'[base{input}]lutrgb=r=gammaval({gam}):g=gammaval({gam}):b=gammaval({gam})')  # this does NOT work for float pixels tho. seems that nothing in FFMPEG does
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
