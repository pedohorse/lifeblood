from copy import copy
from taskflow.basenode import BaseNode
from taskflow.enums import NodeParameterType
from taskflow.nodethings import ProcessingResult, ProcessingError
from taskflow.invocationjob import InvocationJob, InvocationEnvironment

from typing import Iterable


def node_class():
    return HipIfdGenerator


class HipIfdGenerator(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'ifd generator'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'hip', 'houdini', 'ifd', 'generator', 'render', 'stock'

    @classmethod
    def type_name(cls) -> str:
        return 'hip_ifd_generator'

    def __init__(self, name):
        super(HipIfdGenerator, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.5, 0.25, 0.125)
            ui.add_output_for_spawned_tasks()
            ui.add_parameter('hip path', 'hip file path', NodeParameterType.STRING, "`task['file']`")
            ui.add_parameter('driver path', 'mantra node path', NodeParameterType.STRING, "`task['hipdriver']`")
            ui.add_parameter('ifd file path', 'ifd file path', NodeParameterType.STRING, "`task['global_scratch_location']`/`node.name`/`task.name`/ifds/`node.name`.$F4.ifd.sc")
            ui.add_parameter('ifd force inline', 'force inline ifd', NodeParameterType.BOOL, True)

    def process_task(self, context) -> ProcessingResult:
        """
        this node expects to find the following attributes:
        frames
        hipfile
        hipdriver
        :param context:
        :return:
        """
        attrs = context.task_attributes()
        if any(x not in attrs for x in ('frames',)):
            raise ProcessingError('required attribute "frames" not found')
        hippath = context.param_value('hip path')
        driverpath = context.param_value('driver path')
        frames = attrs['frames']

        env = InvocationEnvironment()

        spawnlines = \
            f"    filepath = node.evalParm('soho_diskfile')\n" \
            f"    outimage = node.evalParm('vm_picture')\n" \
            f"    taskflow_connection.create_task(node.name() + '_spawned frame %g' % frame, frames=[frame], file=filepath, hipfile='{hippath}', outimage=outimage)\n"

        if not self.is_output_connected('spawned'):
            spawnlines = ''

        script = \
            f'import hou\n' \
            f'import taskflow_connection\n' \
            f'print("opening file" + {repr(hippath)})\n' \
            f'hou.hipFile.load("{hippath}")\n' \
            f'node = hou.node("{driverpath}")\n' \
            f'if node.parm("soho_outputmode").evalAsInt() != 1:\n' \
            f'    node.parm("soho_outputmode").set(1)\n'
        ifdpath = context.param_value('ifd file path').strip()
        if ifdpath != '':
            script += \
                f'node.parm("soho_diskfile").set("{ifdpath}")\n'
        if context.param_value('ifd force inline'):
            script += \
                f'node.parm("vm_inlinestorage").set(1)\n'
        script += \
            f'for frame in {repr(frames)}:\n' \
            f'    hou.setFrame(frame)\n' \
            f'    print("rendering frame %d" % frame)\n' \
            f'    node.render(frame_range=(frame, frame))\n' \
            f'{spawnlines}' \
            f'print("all done!")\n'

        inv = InvocationJob(['hython', '-c', script], env)
        res = ProcessingResult(job=inv)
        return res

    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()
