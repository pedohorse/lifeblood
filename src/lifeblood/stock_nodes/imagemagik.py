import os
import shlex
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


description = \
'''uses imagemagick on a set of images
currently can only do montage
Args:
    sequence: list attribute to process as image source
              can either be a list of strings - one montage will be generated
              or a list of lists of strings - a montage will be generated 
                  for each bunch of images from inner lists with the same index:
                  [[a1, a2, a3], [b1,b2,b3]] will result in montages: [a1b1, a2b2, a3b3]
                                               
    hint tile count: generally tile count is guessed, but it can be hinted as well
    background: set background color, or none
    force depth: force depth of output image (if supported by format)
    force colorspace: force colorspace of output image (if supported by format)
    custom arguments: custom arguments to provide to montage command (in the end)
    use custom imagemagic path: by default it's assumed that imagemagick is available in PATH
                                you can provide specific bin directory where to look for montage command
    movie path: where to save output picture.
                if "replace # with padded frame" is set - ### will be replaced with properly padded frame number
                note that "frames" attribute must have enough values to provide frame values
                for all montages provided by "sequence"
                meaning number of frames must be >= then minimal sublist of "sequence"  
'''


class Ffmpeg(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'imagemagick'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'image', 'magic', 'stock', 'convert', 'montage'

    @classmethod
    def type_name(cls) -> str:
        return 'imagemagick'

    @classmethod
    def description(cls) -> str:
        return description

    def __init__(self, name):
        super(Ffmpeg, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.129, 0.239, 0.569)
            mode_param = ui.add_parameter('mode', 'mode', NodeParameterType.STRING, 'montage').add_menu((('montage', 'montage'), ('convert', 'convert')))

            ui.add_parameter('sequence', 'sequence attribute name', NodeParameterType.STRING, 'sequence')

            # montage parameters
            ui.add_parameter('bordersize', 'border size (pixels)', NodeParameterType.INT, 0)\
                .set_value_limits(value_min=0)\
                .append_visibility_condition(mode_param, '==', 'montage')
            with ui.parameters_on_same_line_block():
                dohint = ui.add_parameter('use tile hint', 'hint tile count', NodeParameterType.BOOL, False)
                dohint.append_visibility_condition(mode_param, '==', 'montage')
                ui.add_parameter('tile hint', 'hint', NodeParameterType.INT, 3)\
                    .append_visibility_condition(dohint, '==', True)\
                    .append_visibility_condition(mode_param, '==', 'montage')\
                    .set_value_limits(value_min=1)
                ui.add_parameter('tile hint is', '', NodeParameterType.STRING, 'col')\
                    .add_menu((('columns', 'col'), ('rows', 'row')))\
                    .append_visibility_condition(dohint, '==', True) \
                    .append_visibility_condition(mode_param, '==', 'montage')

            # convert parameters
            # are there any convert-specific parameters?

            # common parameters
            with ui.parameters_on_same_line_block():
                bgp = ui.add_parameter('background', 'background', NodeParameterType.BOOL, False)
                ui.add_parameter('background colorr', '', NodeParameterType.FLOAT, 0.0).append_visibility_condition(bgp, '==', True).set_value_limits(0.0, 1.0)
                ui.add_parameter('background colorg', '', NodeParameterType.FLOAT, 0.0).append_visibility_condition(bgp, '==', True).set_value_limits(0.0, 1.0)
                ui.add_parameter('background colorb', '', NodeParameterType.FLOAT, 0.0).append_visibility_condition(bgp, '==', True).set_value_limits(0.0, 1.0)
                ui.add_parameter('background colora', '', NodeParameterType.FLOAT, 1.0).append_visibility_condition(bgp, '==', True).set_value_limits(0.0, 1.0)
            with ui.parameters_on_same_line_block():
                dptp = ui.add_parameter('dodepth', 'force depth', NodeParameterType.BOOL, False)
                ui.add_parameter('depth', '', NodeParameterType.INT, 8)\
                    .add_menu((('8bit', 8), ('16bit', 16), ('32bit', 32)))\
                    .append_visibility_condition(dptp, '==', True)
            with ui.parameters_on_same_line_block():
                cspp = ui.add_parameter('docolorspace', 'force colorspace', NodeParameterType.BOOL, False)
                ui.add_parameter('colorspace', '', NodeParameterType.STRING, 'sRGB')\
                    .add_menu((('CMY', 'CMY'), ('CMYK', 'CMYK'), ('Gray', 'Gray'),
                               ('HCL', 'HCL'), ('HCLp', 'HCLp'), ('HSB', 'HSB'),
                               ('HSI', 'HSI'), ('HSL', 'HSL'), ('HSV', 'HSV'),
                               ('HWB', 'HWB'), ('Jzazbz', 'Jzazbz'), ('Lab', 'Lab'),
                               ('LCHab', 'LCHab'), ('LCHuv', 'LCHuv'), ('LMS', 'LMS'),
                               ('Log', 'Log'), ('Luv', 'Luv'), ('OHTA', 'OHTA'),
                               ('Rec601YCbCr', 'Rec601YCbCr'), ('Rec709YCbCr', 'Rec709YCbCr'), ('RGB', 'RGB'),
                               ('scRGB', 'scRGB'), ('sRGB', 'sRGB'), ('Transparent', 'Transparent'),
                               ('xyY', 'xyY'), ('XYZ', 'XYZ'), ('YCbCr', 'YCbCr'),
                               ('YCC', 'YCC'), ('YDbDr', 'YDbDr'), ('YIQ', 'YIQ'),
                               ('YPbPr', 'YPbPr'), ('YUV', 'YUV')
                               ))\
                    .append_visibility_condition(cspp, '==', True)
            ui.add_parameter('custom args', 'custom arguments', NodeParameterType.STRING, '')
            with ui.parameters_on_same_line_block():
                checkbox = ui.add_parameter('use custom path', 'use custom imagemagic path', NodeParameterType.BOOL, False)
                ui.add_parameter('imagemagic bin dir', 'path to bin dir', NodeParameterType.STRING, '').append_visibility_condition(checkbox, '==', True)
            with ui.parameters_on_same_line_block():
                ui.add_parameter('outpath', 'movie path', NodeParameterType.STRING, '/tmp/output.####.png')
                ui.add_parameter('outpath has #', 'replace # with padded frame', NodeParameterType.BOOL, True)

    def process_task(self, context) -> ProcessingResult:
        attributes = context.task_attributes()
        mode = context.param_value('mode')
        sequence_attrib_name = context.param_value('sequence')
        if mode == 'montage':
            if sequence_attrib_name not in attributes:
                raise ProcessingError(f'required attribute "{sequence_attrib_name}" not found')
            sequence = attributes[sequence_attrib_name]
            result_to_file = False
            if isinstance(sequence[0], str):  # we have sequence of frames -
                # treat it as parts of same mosaic in case of mosaic
                sequence = [[x] for x in sequence]  # just pack elements into lists and adapt for general case
                result_to_file = True
        elif mode == 'convert':
            # logic is - if sequence attrib exists - take it, if no - use 'file'
            if sequence_attrib_name in attributes:
                sequence = [attributes[sequence_attrib_name]]  # 2 nested lists to unify processing with montage mode
                result_to_file = False
            elif 'file' in attributes:
                sequence = [[attributes['file']]]  # 2 nested lists to unify processing with montage mode
                result_to_file = True
            else:
                raise ProcessingError(f'either "file" or "{sequence_attrib_name}" attribute must exist')
        frames = []
        if 'frames' in attributes:
            frames = attributes['frames']

        assert isinstance(sequence, list), 'sequence attribute is supposed to be list of frames, or list of sequences'

        outpath = context.param_value('outpath')
        replace_hashes = context.param_value('outpath has #')

        if context.param_value('use custom path'):
            bin = os.path.join(context.param_value('imagemagic bin dir'), mode)
        else:
            bin = mode

        custom_args = shlex.split(context.param_value('custom args'))

        hint = None
        border = None
        if mode == 'montage':
            if context.param_value('use tile hint'):
                if context.param_value('tile hint is') == 'col':
                    hint = f'{context.param_value("tile hint")}x'
                else:  # row
                    hint = f'x{context.param_value("tile hint")}'
            border = context.param_value('bordersize')

        assert isinstance(sequence[0], list)

        # construct a mosaic from one image from each subsequences
        numframes = min(len(x) for x in sequence)
        if replace_hashes and len(frames) < numframes:
            raise ProcessingError(f'frame list must have at least {numframes} frames to properly resolve output name')

        # just in case let's keep this script as py2-3 compatible as possible. why? actually there's no reason, unless it can be run from within some specific DCC, which it should
        script = f'import os\n' \
                 f'import re\n' \
                 f'import subprocess\n' \
                 f'sequence = {repr(sequence)}\n' \
                 f'frames = {repr(frames)}\n' \
                 f'outpath = {repr(outpath)}\n' \
                 f'if not os.path.exists(os.path.dirname(outpath)):\n' \
                 f'    try:\n' \
                 f'        os.makedirs(os.path.dirname(outpath))\n' \
                 f'    except OSError as e:\n' \
                 f'        import errno\n' \
                 f'        if e.errno != errno.EEXIST:\n' \
                 f'            raise\n' \
                 f'for i in range({numframes}):\n' \
                 f'    print("ALF_PROGRESS {{}}%".format(int(i*100.0/{numframes})))\n' \
                 f'    args = [{repr(bin)}]\n' \
                 f'    args += [x[i] for x in sequence]\n'
        if hint is not None:
            script += f"    args += ['-tile', {repr(hint)}]\n"
        if border is not None:
            script += f"    args += ['-geometry', f'+{border}+{border}']\n"
        if not context.param_value('background'):
            script += f'    args += ["-background", "none"]\n'
        else:
            r = context.param_value('background colorr') * 100
            g = context.param_value('background colorg') * 100
            b = context.param_value('background colorb') * 100
            a = context.param_value('background colora')
            script += f'    args += ["-background", "rgba({r}%,{g}%,{b}%,{a})"]\n'
            if mode == 'convert':
                script += f'    args += ["-flatten"]\n'
        if context.param_value('dodepth'):
            script += f'    args += ["-depth", "{context.param_value("depth")}"]\n'
        if context.param_value('docolorspace'):
            script += f'    args += ["-colorspace", {repr(context.param_value("colorspace"))}]\n'
        script += f"    args += {repr(custom_args)}\n"
        if replace_hashes:
            script += "    args.append(re.sub('#+', lambda m: '{{:0{}d}}'.format(len(m.group(0))).format(frames[i]), outpath))\n"
        else:
            script += "    args.append(outpath)\n"
        script += f'    subprocess.Popen(args).wait()\n'

        job = InvocationJob(['python', ':/work_to_do.py'])
        job.set_extra_file('work_to_do.py', script)
        res = ProcessingResult(job)
        if result_to_file:  # saving to "file" attribute a single path
            if replace_hashes:
                res.set_attribute('file', re.sub('#+', lambda m: f'{frames[0]:0{len(m.group(0))}d}', outpath))
            else:
                res.set_attribute('file', outpath)
        else:  # otherwise save a sequence of files into sequence attribute
            if replace_hashes:
                res.set_attribute('sequence', [re.sub('#+', lambda m: f'{i:0{len(m.group(0))}d}', outpath) for i in frames])
            else:
                res.set_attribute('sequence', [outpath for i in frames])  # this is a weird case...
        return res

    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()
