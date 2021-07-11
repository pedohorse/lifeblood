from copy import copy
from taskflow.basenode import BaseNode
from taskflow.enums import NodeParameterType
from taskflow.nodethings import ProcessingResult, TaskSpawn
from taskflow.invocationjob import InvocationJob, InvocationEnvironment
from taskflow.names import match

from typing import Iterable


def node_class():
    return HipDriverRenderer


class HipDriverRenderer(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'hip driver renderer'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'hip', 'houdini', 'driver', 'render', 'stock'

    def __init__(self, name):
        super(HipDriverRenderer, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_output_for_spawned_tasks()
            ui.add_parameter('driver path', 'rop node path', NodeParameterType.STRING, '')
            ui.add_parameter('override', 'override with hipdriver attribute', NodeParameterType.BOOL, False)
            ui.add_parameter('attrs', 'attributes to copy to children', NodeParameterType.STRING, '*')

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
        if any(x not in attrs for x in ('file', 'frames')):
            return ProcessingResult()  # TODO:  better throw error
        hippath = attrs['file']
        if context.param_value('override') and 'hipdriver' in attrs:
            driverpath = attrs['hipdriver']
        else:
            driverpath = context.param_value('driver path')
        frames = attrs['frames']
        matching_attrnames = match(context.param_value('attrs'), attrs.keys())
        attr_to_trans = tuple((x, attrs[x]) for x in matching_attrnames if x not in ('frames', 'file'))

        env = InvocationEnvironment()

        spawnlines = \
            f"    kwargs = {{'frames':[frame]}}\n" \
            f"    for attr, val in {repr(attr_to_trans)}:\n" \
            f"        kwargs[attr] = val\n" \
            f"    kwargs['hipfile'] = {repr(hippath)}\n" \
            f"    if node.parm('filename'):\n" \
            f"        kwargs['file'] = node.evalParm('filename')\n" \
            f"    if node.parm('sopoutput'):\n" \
            f"        kwargs['file'] = node.evalParm('sopoutput')\n" \
            f"    taskflow_connection.create_task(node.name() + '_spawned frame %g' % frame, **kwargs)\n"

        if not self.is_output_connected('spawned'):
            spawnlines = ''

        script = \
            f'import hou\n' \
            f'import taskflow_connection\n' \
            f'print("opening file" + {repr(hippath)})\n' \
            f'hou.hipFile.load("{hippath}")\n' \
            f'node = hou.node("{driverpath}")\n' \
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
