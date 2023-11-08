import os
import re

from math import sqrt, floor, ceil

from lifeblood.basenode import BaseNode
from lifeblood.nodethings import ProcessingResult, ProcessingError
from lifeblood.uidata import NodeParameterType
from lifeblood.invocationjob import InvocationJob
from lifeblood.invocationjob import InvocationJob, InvocationEnvironment

from typing import Iterable


def node_class():
    return Ffmpeg


MODE_SEQUENCE_TO_MOVIE = 0
MODE_MOVIE_TO_SEQUENCE = 1


DESCRIPTION = '''
This node uses FFMPEG to manipulate given movie files and sequences.
You need to separately download and install FFMPEG to be able to use this node.

in sequence-to-movie mode this node converts sequence or sequences found in the attribute
of the given name ("images" by default) to a video.
If "images" is a list of files - those files are treated as individual frames.
If "images" is a list of sequences - those sequences are treated as parts of a mosaic

in movie-to-sequence mode this node converts input movie into a sequence
sequence must have ffmpeg sequence pattern, like %04d to specify where to put frame number
in the file name. 0 there means zero-padded, 4 means use minimum 4 digits for the number. 
'''


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

    @classmethod
    def description(cls) -> str:
        return DESCRIPTION

    def __init__(self, name):
        super(Ffmpeg, self).__init__(name)
        ui = self.get_ui()
        with ((ui.initializing_interface_lock())):
            ui.color_scheme().set_main_color(0.125, 0.33, 0.05)

            mode_parm = ui.add_parameter('operation_mode', 'mode', NodeParameterType.INT, 0)
            mode_parm.add_menu(
                (
                    ('sequence to movie', MODE_SEQUENCE_TO_MOVIE),
                    ('movie to sequence', MODE_MOVIE_TO_SEQUENCE))
            )
            ui.add_parameter('sequence', 'sequence attribute name', NodeParameterType.STRING, 'images'
                             ).append_visibility_condition(mode_parm, '==', MODE_SEQUENCE_TO_MOVIE)
            ui.add_parameter('ffmpeg bin path', 'ffmpeg binary', NodeParameterType.STRING, 'ffmpeg')

            # sequence to movie params
            stm_params = []
            stm_params.append(ui.add_parameter('fps', 'fps', NodeParameterType.INT, 24))
            stm_params.append(ui.add_parameter('icrf', 'quality %', NodeParameterType.INT, 75).set_slider_visualization(1, 100))
            stm_params.append(ui.add_parameter('-pix_fmt', 'pixel format', NodeParameterType.STRING, 'yuv420p'))
            stm_params.append(ui.add_parameter('-vcodec', 'codec', NodeParameterType.STRING, 'libx264'))
            stm_params.append(ui.add_parameter('outpath', 'movie path', NodeParameterType.STRING, '/tmp/movie.mp4'))
            docc = ui.add_parameter('docc', 'color correct', NodeParameterType.BOOL, False)
            stm_params.append(docc)
            stm_params.append(ui.add_parameter('cc gamma', 'gamma', NodeParameterType.FLOAT, 1.0).append_visibility_condition(docc, '==', True))
            for param in stm_params:
                param.append_visibility_condition(mode_parm, '==', MODE_SEQUENCE_TO_MOVIE)

            # movie to sequence params
            mts_params = []
            mts_params.append(ui.add_parameter('ffprobe bin path', 'ffprobe binary', NodeParameterType.STRING, 'ffprobe'))
            mts_params.append(ui.add_parameter('in_movie', 'movie path', NodeParameterType.STRING, '`task["file"]`'))
            mts_params.append(ui.add_parameter('out_sequence', 'output sequence', NodeParameterType.STRING, '/tmp/file.%04d.png'))
            mts_params.append(ui.add_parameter('start_frame', 'sequence start frame', NodeParameterType.INT, 1))
            with ui.parameters_on_same_line_block():
                do_out_images_attr = ui.add_parameter('do_out_attr_name', 'output sequence images', NodeParameterType.BOOL, False)
                mts_params.append(do_out_images_attr)
                mts_params.append(
                    ui.add_parameter('out_attr_name', 'sequence attribute name', NodeParameterType.STRING, 'images')
                    .append_visibility_condition(do_out_images_attr, '==', True)
                )

            for param in mts_params:
                param.append_visibility_condition(mode_parm, '==', MODE_MOVIE_TO_SEQUENCE)

    def process_task(self, context) -> ProcessingResult:
        mode = context.param_value('operation_mode')
        if mode == MODE_SEQUENCE_TO_MOVIE:
            return self._process_sequence_to_movie(context)
        elif mode == MODE_MOVIE_TO_SEQUENCE:
            return self._process_movie_to_sequence(context)
        else:
            raise ProcessingError(f'unknown processing mode {mode}')

    def _process_sequence_to_movie(self, context) -> ProcessingResult:
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

    def _process_movie_to_sequence(self, context):
        binpath = context.param_value('ffmpeg bin path')
        probe_binpath = context.param_value('ffprobe bin path')

        input = context.param_value('in_movie')
        output = context.param_value('out_sequence')
        start_frame = context.param_value('start_frame')
        output_attr_name = context.param_value('out_attr_name')
        do_output_attr_name = context.param_value('do_out_attr_name')

        rx_str = r"%(0)?(\d+)d"
        rx = re.compile(rx_str)

        # TODO: add input file existence check to the script
        script = \
            (f'import sys\n'
             f'from subprocess import Popen, PIPE\n'
             f'import re\n'
             f'from pathlib import Path\n'
             f'from lifeblood_connection import set_attributes\n'
             f'\n'
             f'start_frame = {start_frame}\n'
             f'output_pattern = {repr(output)}\n'
             f'rx = re.compile({repr(rx_str)})''\n'
             'Path(output_pattern).parent.mkdir(parents=True, exist_ok=True)\n'
             f'\n'
             f'out, _ = Popen(\n'
             f'    [{repr(probe_binpath)}, "-v", "error", "-count_frames", "-show_entries", "stream=nb_read_frames", "-of", "csv=p=0", {repr(input)}],\n'
             f'    stdout=PIPE\n'
             f').communicate()\n'
             f'\n'
             f'try:\n'
             f'    frame_count = int(out.strip().split(b"\\n", 1)[0].strip())\n'
             f'except Exception as e:\n'
             f'    raise RuntimeError(f"failed to get frame count from the movie: {{e}}")\n'
             f'\n'
             f'excode = Popen(\n'
             f'    [{repr(binpath)}, "-i", {repr(input)}, "-start_number", str(start_frame), output_pattern]\n'
             f').wait()\n'
             'if excode != 0:\n'
             '    sys.exit(excode)\n'
             '\n'
             'frame_files = []\n'
             'frames = list(range(start_frame, start_frame + frame_count))\n'
             'for frame in frames:\n'
             '    frame_files.append(rx.sub(lambda m: "{{:{}{}d}}".format(m.group(1) or "", m.group(2)).format(frame), output_pattern))\n'
             '\n'
             f'attributes = {{"frames": frames}}\n'
             f'if {repr(do_output_attr_name)}:\n'
             f'    attributes[{repr(output_attr_name)}] = frame_files\n'
             f'set_attributes(attributes, blocking=True)\n'
             f'')

        # TODO: add option to create per-frame children
        if rx.search(output) is None:
            raise ProcessingError(f"frame patter (like %04d) not detected in output sequence {output}")

        inv = InvocationJob(['python', ':/convert.py'])
        inv.set_extra_file('convert.py', script)

        return ProcessingResult(inv)

    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()
