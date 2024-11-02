import os
import shlex
import re

from lifeblood.node_plugin_base import BaseNode
from lifeblood.nodethings import ProcessingResult, ProcessingError
from lifeblood.enums import NodeParameterType
from lifeblood.invocationjob import InvocationJob

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
        with ((ui.initializing_interface_lock())):
            ui.color_scheme().set_main_color(0.129, 0.239, 0.569)
            mode_param = ui.add_parameter('mode', 'mode', NodeParameterType.STRING, 'convert-file')
            mode_param.add_menu((
                ('montage sequence', 'montage'),
                ('convert sequence', 'convert'),
                ('convert file', 'convert-file'),
            ))

            ui.add_parameter('sequence', 'sequence attribute name', NodeParameterType.STRING, 'sequence') \
                .append_visibility_condition(mode_param, '!=', 'convert-file')
            ui.add_parameter('file in', 'file to convert', NodeParameterType.STRING, '`task["file"]`') \
                .append_visibility_condition(mode_param, '==', 'convert-file')

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
                do_resize = ui.add_parameter('doresize', 'resize', NodeParameterType.BOOL, False)
                resize_type = ui.add_parameter('resize type', 'resize method', NodeParameterType.INT, 0) \
                    .add_menu((
                        ('absolute', 0),
                        ('relative', 1),
                        ('percentage', 2),
                    )) \
                    .append_visibility_condition(do_resize, '==', True)
                aspect_parm = ui.add_parameter('resize aspect', 'resize by', NodeParameterType.INT, 0) \
                    .add_menu((
                        ('height & width', 0),
                        ('height', 1),
                        ('width', 2),
                    )) \
                    .append_visibility_condition(do_resize, '==', True)

                # abs resize
                ui.add_parameter('resize abs w', 'width', NodeParameterType.INT, 640) \
                    .append_visibility_condition(do_resize, '==', True) \
                    .append_visibility_condition(resize_type, '==', 0) \
                    .append_visibility_condition(aspect_parm, 'in', (0, 2))
                ui.add_parameter('resize abs h', 'height', NodeParameterType.INT, 480) \
                    .append_visibility_condition(do_resize, '==', True) \
                    .append_visibility_condition(resize_type, '==', 0) \
                    .append_visibility_condition(aspect_parm, 'in', (0, 1))

                # rel resize
                ui.add_parameter('resize rel w', '+width', NodeParameterType.INT, 0) \
                    .append_visibility_condition(do_resize, '==', True) \
                    .append_visibility_condition(resize_type, '==', 1) \
                    .append_visibility_condition(aspect_parm, 'in', (0, 2))
                ui.add_parameter('resize rel h', '+height', NodeParameterType.INT, 0) \
                    .append_visibility_condition(do_resize, '==', True) \
                    .append_visibility_condition(resize_type, '==', 1) \
                    .append_visibility_condition(aspect_parm, 'in', (0, 1))

                # prec resize
                ui.add_parameter('resize perc w', 'width %', NodeParameterType.FLOAT, 100) \
                    .append_visibility_condition(do_resize, '==', True) \
                    .append_visibility_condition(resize_type, '==', 2) \
                    .append_visibility_condition(aspect_parm, 'in', (0, 2))
                ui.add_parameter('resize perc h', 'height %', NodeParameterType.FLOAT, 100) \
                    .append_visibility_condition(do_resize, '==', True) \
                    .append_visibility_condition(resize_type, '==', 2) \
                    .append_visibility_condition(aspect_parm, 'in', (0, 1))

            with ui.parameters_on_same_line_block():
                bgp = ui.add_parameter('background', 'background', NodeParameterType.BOOL, False)
                ui.add_parameter('background colorr', '', NodeParameterType.FLOAT, 0.0).append_visibility_condition(bgp, '==', True).set_value_limits(0.0, 1.0)
                ui.add_parameter('background colorg', '', NodeParameterType.FLOAT, 0.0).append_visibility_condition(bgp, '==', True).set_value_limits(0.0, 1.0)
                ui.add_parameter('background colorb', '', NodeParameterType.FLOAT, 0.0).append_visibility_condition(bgp, '==', True).set_value_limits(0.0, 1.0)
                ui.add_parameter('background colora', '', NodeParameterType.FLOAT, 1.0).append_visibility_condition(bgp, '==', True).set_value_limits(0.0, 1.0)
            with ui.parameters_on_same_line_block():
                dptp = ui.add_parameter('dodepth', 'force bit depth', NodeParameterType.BOOL, False)
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
            with ui.parameters_on_same_line_block():
                dotext = ui.add_parameter('dotext', 'add text', NodeParameterType.BOOL, False)
                ui.add_parameter('text', None, NodeParameterType.STRING, 'i am text') \
                    .append_visibility_condition(dotext, '==', True)
            with ui.collapsable_group_block("text params", "text options"):
                with ui.parameters_on_same_line_block():  # color
                    ui.add_parameter('text color labal', None, NodeParameterType.STRING, 'color', False, True) \
                        .append_visibility_condition(dotext, '==', True)
                    ui.add_parameter('text color R', None, NodeParameterType.FLOAT, 1.0) \
                        .append_visibility_condition(dotext, '==', True)
                    ui.add_parameter('text color G', None, NodeParameterType.FLOAT, 1.0) \
                        .append_visibility_condition(dotext, '==', True)
                    ui.add_parameter('text color B', None, NodeParameterType.FLOAT, 1.0) \
                        .append_visibility_condition(dotext, '==', True)
                    ui.add_parameter('text color A', None, NodeParameterType.FLOAT, 1.0) \
                        .append_visibility_condition(dotext, '==', True)
                ui.add_parameter('text size', None, NodeParameterType.INT, 24) \
                    .append_visibility_condition(dotext, '==', True)

            ui.add_parameter('custom args', 'custom arguments', NodeParameterType.STRING, '')

            with ui.parameters_on_same_line_block():
                ui.add_parameter('outpath', 'movie path', NodeParameterType.STRING, '/tmp/output.####.png') \
                    .append_visibility_condition(mode_param, '!=', 'convert-file')
                ui.add_parameter('outpath has #', 'replace # with padded frame', NodeParameterType.BOOL, True) \
                    .append_visibility_condition(mode_param, '!=', 'convert-file')

            ui.add_parameter('file out', 'output file path', NodeParameterType.STRING, '`"".join(((x + "_out" if i==0 else x) for i, x in enumerate(os.path.splitext(task["file"]))))`') \
                .append_visibility_condition(mode_param, '==', 'convert-file')

    def process_task(self, context) -> ProcessingResult:
        attributes = context.task_attributes()
        mode = context.param_value('mode')

        if mode == 'montage':
            binary_name = 'montage'
            outpath = context.param_value('outpath')
            replace_hashes = context.param_value('outpath has #')
            sequence_attrib_name = context.param_value('sequence')

            if sequence_attrib_name not in attributes:
                raise ProcessingError(f'required attribute "{sequence_attrib_name}" not found')
            sequence = attributes[sequence_attrib_name]
            result_to_file = False
            if isinstance(sequence[0], str):  # we have sequence of frames -
                # treat it as parts of same mosaic in case of mosaic
                sequence = [[x] for x in sequence]  # just pack elements into lists and adapt for general case
                result_to_file = True
        elif mode == 'convert':
            binary_name = 'convert'  # TODO: this is deprecated since im7
            outpath = context.param_value('outpath')
            replace_hashes = context.param_value('outpath has #')
            sequence_attrib_name = context.param_value('sequence')

            # logic is - if sequence attrib exists - take it, if no - use 'file'
            if sequence_attrib_name in attributes:
                sequence = [attributes[sequence_attrib_name]]  # 2 nested lists to unify processing with montage mode
                result_to_file = False
            elif 'file' in attributes:  # TODO: not obvious implicit logic - remove it
                sequence = [[attributes['file']]]  # 2 nested lists to unify processing with montage mode
                result_to_file = True
            else:
                raise ProcessingError(f'either "file" or "{sequence_attrib_name}" attribute must exist')
        elif mode == 'convert-file':
            binary_name = 'convert'  # TODO: this is deprecated since im7
            outpath = context.param_value('file out')
            replace_hashes = False
            result_to_file = True

            sequence = [[context.param_value('file in')]]
        else:
            raise ProcessingError(f'unknown mode "{mode}"')
        frames = []
        if 'frames' in attributes:
            frames = attributes['frames']

        assert isinstance(sequence, list), 'sequence attribute is supposed to be list of frames, or list of sequences'

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
                 f'import sys\n' \
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
                 f'    args = [{repr(binary_name)}]\n' \
                 f'    args += [x[i] for x in sequence]\n'

        if context.param_value('doresize'):
            resize_type = context.param_value('resize type')
            if resize_type == 0:  # abs
                w = f"{context.param_value(f'resize abs w')}"
                h = f"{context.param_value(f'resize abs h')}"
            elif resize_type == 1:  # rel
                w = f"{context.param_value(f'resize rel w'):+d}"
                h = f"{context.param_value(f'resize rel h'):+d}"
            elif resize_type == 2:  # perc
                w = f"{context.param_value(f'resize perc w')}%"
                h = f"{context.param_value(f'resize perc h')}%"
            else:
                raise ProcessingError('unknown resize method!')

            resize_aspect = context.param_value('resize aspect')
            if resize_aspect == 0:  # h&w
                resize_arg = f'{w}x{h}!'  # ! means "ignore aspect"
            elif resize_aspect == 1:  # h
                resize_arg = f'x{h}'
            elif resize_aspect == 2:  # w
                resize_arg = w
            else:
                raise ProcessingError('unknown resize aspect type')

            script += f'    args +=["-resize", {repr(resize_arg)}]\n'

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

        if context.param_value('dotext'):
            text = context.param_value('text')
            text_size = context.param_value('text size')
            text_rgba = (
                context.param_value('text color R'),
                context.param_value('text color G'),
                context.param_value('text color B'),
                context.param_value('text color A'),
            )
            if text.startswith('@'):  # @ triggers magick to treat text as file path
                text = '\\' + text
            script += (f'    args += ["-gravity", "southwest", '
                       f'"-pointsize", "{text_size}", '
                       f'"-fill", "rgba({text_rgba[0]*100}%, {text_rgba[1]*100}%, {text_rgba[2]*100}%, {text_rgba[3]})", '
                       f'"-annotate", "0", {repr(text)}]\n')

        script += f"    args += {repr(custom_args)}\n"

        if replace_hashes:
            script += "    args.append(re.sub('#+', lambda m: '{{:0{}d}}'.format(len(m.group(0))).format(frames[i]), outpath))\n"
        else:
            script += "    args.append(outpath)\n"

        script += f'    print(args)\n'
        script += f'    res = subprocess.Popen(args).wait()\n'
        script += ('    if res != 0:\n'
                   '        print("imagemagick process finished with error")\n'
                   '        sys.exit(res)\n')

        job = InvocationJob(['python', ':/work_to_do.py'])
        job.set_extra_file('work_to_do.py', script)
        res = ProcessingResult(job)
        if result_to_file:  # saving to "file" attribute a single path
            if replace_hashes:
                res.set_attribute('__imagemagick_out__file', re.sub('#+', lambda m: f'{frames[0]:0{len(m.group(0))}d}', outpath))
            else:
                res.set_attribute('__imagemagick_out__file', outpath)
            if context.task_has_attribute('__imagemagick_out__sequence'):
                res.remove_attribute('__imagemagick_out__sequence')
        else:  # otherwise save a sequence of files into sequence attribute
            if replace_hashes:
                res.set_attribute('__imagemagick_out__sequence', [re.sub('#+', lambda m: f'{i:0{len(m.group(0))}d}', outpath) for i in frames])
            else:
                res.set_attribute('__imagemagick_out__sequence', [outpath for i in frames])  # this is a weird case...
            if context.task_has_attribute('__imagemagick_out__file'):
                res.remove_attribute('__imagemagick_out__file')
        return res

    def postprocess_task(self, context) -> ProcessingResult:
        res = ProcessingResult()
        for tmp_attr, attr in (
            ('__imagemagick_out__file', 'file'),
            ('__imagemagick_out__sequence', 'sequence'),
        ):
            if context.task_has_attribute(tmp_attr):
                res.remove_attribute(tmp_attr)
                res.set_attribute(attr, context.task_attribute(tmp_attr))
        return res
