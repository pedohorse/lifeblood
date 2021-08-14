from copy import copy
from taskflow.basenode import BaseNode
from taskflow.enums import NodeParameterType
from taskflow.nodethings import ProcessingResult, ProcessingError
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

    @classmethod
    def type_name(cls) -> str:
        return 'hip_driver_renderer'

    def __init__(self, name):
        super(HipDriverRenderer, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.5, 0.25, 0.125)
            ui.add_output_for_spawned_tasks()
            ui.add_parameter('hip path', 'hip path', NodeParameterType.STRING, "`task['file']`")
            ui.add_parameter('driver path', 'rop node path', NodeParameterType.STRING, "`task['hipdriver']`")
            ui.add_parameter('ignore inputs', 'ignore rop inputs', NodeParameterType.BOOL, True)
            ui.add_parameter('whole range', 'render whole range (needed for alembics)', NodeParameterType.BOOL, False)
            with ui.parameters_on_same_line_block():
                override = ui.add_parameter('do override output', 'override output path', NodeParameterType.BOOL, False)
                ui.add_parameter('override output', None, NodeParameterType.STRING, '').add_visibility_condition(override, '==', True)
            with ui.parameters_on_same_line_block():
                pnoverride = ui.add_parameter('do override parmname', 'use specific parameter name', NodeParameterType.BOOL, False)
                pnoverride.add_visibility_condition(override, '==', True)
                ui.add_parameter('override parmname', None, NodeParameterType.STRING, 'sopoutput').add_visibility_condition(pnoverride, '==', True)

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
        if any(x not in attrs for x in ('frames',)):
            raise ProcessingError('required attribs not found')
        hippath = context.param_value('hip path')
        driverpath = context.param_value('driver path')
        frames = attrs['frames']
        matching_attrnames = match(context.param_value('attrs'), attrs.keys())
        attr_to_trans = tuple((x, attrs[x]) for x in matching_attrnames if x not in ('frames', 'file'))

        env = InvocationEnvironment()

        spawnlines = \
             "kwargs = {{'frames': {frames}}}\n" \
            f"for attr, val in {repr(attr_to_trans)}:\n" \
            f"    kwargs[attr] = val\n" \
            f"kwargs['hipfile'] = {repr(hippath)}\n" \
            f"if node.parm('filename'):\n" \
            f"    kwargs['file'] = node.evalParm('filename')\n" \
            f"if node.parm('sopoutput'):\n" \
            f"    kwargs['file'] = node.evalParm('sopoutput')\n" \
            f"taskflow_connection.create_task(node.name() + '_spawned frame %g' % frame, **kwargs)\n"

        if not self.is_output_connected('spawned'):
            spawnlines = None

        script = \
            f'import hou\n' \
            f'import taskflow_connection\n' \
            f'print("opening file" + {repr(hippath)})\n' \
            f'hou.hipFile.load("{hippath}")\n' \
            f'node = hou.node("{driverpath}")\n'
        if context.param_value('do override output'):
            override_path = context.param_value('override output').strip()
            if override_path != '':
                if context.param_value('do override parmname'):
                    parm_name = context.param_value('override parmname')
                    script += \
                        f'node.parm("{parm_name}").set("{override_path}")\n'
                else:  # try to autodetect
                    script += \
                        f'if node.type().name() in ("geometry", "rop_geometry"):\n' \
                        f'    output_parm = "sopoutput"\n' \
                        f'elif node.type().name() in ("alembic", "rop_alembic"):\n' \
                        f'    output_parm = "filename"\n' \
                        f'else:\n' \
                        f'    raise RuntimeError("cannot autodetect output parameter for given node type %s " % node.type().name())\n' \
                        f'node.parm(output_parm).set("{override_path}")\n'

        if context.param_value('whole range'):
            script += \
                f'hou.setFrame({frames[0]})\n'\
                f'node.render(frame_range=({frames[0]}, {frames[-1]}, {frames[min(1, len(frames)-1)] - frames[0] or 1}), ignore_inputs={context.param_value("ignore inputs")})\n'
            if spawnlines is not None:
                script += \
                    spawnlines.format(frames=repr(frames))
        else:
            script += \
                f'for frame in {repr(frames)}:\n' \
                f'    hou.setFrame(frame)\n' \
                f'    print("rendering frame %d" % frame)\n' \
                f'    node.render(frame_range=(frame, frame), ignore_inputs={context.param_value("ignore inputs")})\n'
            if spawnlines is not None:
                spawnlines = spawnlines.format(frames='[frame]')
                script += '\n'.join(f'    {line}' for line in spawnlines.split('\n')) + '\n'

        script += f'print("all done!")\n'

        inv = InvocationJob(['hython', ':/work_to_do.py'], env=env)
        inv.set_extra_file('work_to_do.py', script)
        res = ProcessingResult(job=inv)
        return res

    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()
