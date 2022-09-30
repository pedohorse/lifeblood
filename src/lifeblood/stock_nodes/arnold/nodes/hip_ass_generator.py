from copy import copy
from lifeblood.basenode import BaseNodeWithTaskRequirements
from lifeblood.enums import NodeParameterType
from lifeblood.nodethings import ProcessingResult, ProcessingError
from lifeblood.invocationjob import InvocationJob, InvocationEnvironment
from lifeblood.text import filter_by_pattern

from typing import Iterable


def node_class():
    return HipAssGenerator


class HipAssGenerator(BaseNodeWithTaskRequirements):
    @classmethod
    def label(cls) -> str:
        return 'ass generator'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'hip', 'houdini', 'ass', 'arnold', 'generator', 'render', 'stock'

    @classmethod
    def type_name(cls) -> str:
        return 'hip_ass_generator'

    def __init__(self, name):
        super(HipAssGenerator, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.0, 0.54, 0.56)
            ui.add_output_for_spawned_tasks()
            ui.add_parameter('hip path', 'hip file path', NodeParameterType.STRING, "`task['hipfile']`")
            with ui.parameters_on_same_line_block():
                mask_hip = ui.add_parameter('mask as different hip', 'mask as different hip file', NodeParameterType.BOOL, False)
                ui.add_parameter('mask hip path', '', NodeParameterType.STRING, "`task.get('hipfile_orig', task['hipfile'])`")\
                    .append_visibility_condition(mask_hip, '==', True)
            ui.add_parameter('driver path', 'mantra node path', NodeParameterType.STRING, "`task['hipdriver']`")
            ui.add_parameter('attrs to context', 'set these attribs as context', NodeParameterType.STRING, '')
            ui.add_parameter('ass file path', 'ass file path', NodeParameterType.STRING, "`config['global_scratch_location']`/`node.name`/`task.name`_`task.id`/ass/`node.name`.$F4.ass")
            with ui.parameters_on_same_line_block():
                skipparam = ui.add_parameter('skip if exists', 'skip if result already exists', NodeParameterType.BOOL, False)
                ui.add_parameter('gen for skipped', 'generate children for skipped', NodeParameterType.BOOL, True).append_visibility_condition(skipparam, '==', True)
            ui.add_parameter('attrs', 'attributes to copy to children', NodeParameterType.STRING, '')

            ui.parameter('worker type').set_hidden(True)
            ui.parameter('worker type').set_locked(True)

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
        matching_attrnames = filter_by_pattern(context.param_value('attrs'), attrs.keys())
        attr_to_trans = tuple((x, attrs[x]) for x in matching_attrnames if x not in ('frames', 'file'))
        attr_to_context = filter_by_pattern(context.param_value('attrs to context'), attrs.keys())

        env = InvocationEnvironment()

        spawnlines = \
            f"        filepath = node.evalParm('ar_ass_file')\n" \
            f"        outimage = node.evalParm('ar_picture')\n" \
            f"        attrs = {{'frames': [frame], 'file': filepath, 'hipfile': {repr(hippath)}, 'outimage': outimage}}\n" \
            f"        for attr, val in {repr(attr_to_trans)}:\n" \
            f"            attrs[attr] = val\n" \
            f"        lifeblood_connection.create_task(node.name() + '_spawned frame %g' % frame, attrs)\n"

        if not self.is_output_connected('spawned'):
            spawnlines = ''

        script = \
            f'import os\n' \
            f'import hou\n' \
            f'import lifeblood_connection\n'

        if context.param_value('mask as different hip'):
            mask_path = context.param_value('mask hip path')
            script += 'def __fix_hip_env__(event_type=None):\n' \
                      '    if event_type == hou.hipFileEventType.BeforeSave:\n' \
                      '        hou.hipFile.setName(os.devnull)\n' \
                      '        return\n' \
                      '    if event_type not in (hou.hipFileEventType.AfterSave, hou.hipFileEventType.AfterLoad) and event_type is not None:\n' \
                      '        return\n' \
                     f'    hou.hipFile.setName({repr(mask_path)})\n' \
                      'hou.hipFile.addEventCallback(__fix_hip_env__)\n'

        script += \
            f'print("opening file" + {repr(hippath)})\n' \
            f'hou.hipFile.load({repr(hippath)}, ignore_load_warnings=True)\n' \
            f'node = hou.node({repr(driverpath)})\n'

        for attrname in attr_to_context:
            script += f'hou.setContextOption({repr(attrname)}, {repr(attrs[attrname])})\n'

        script += \
            f'if node.parm("ar_ass_export_enable").evalAsInt() != 1:\n' \
            f'    node.parm("ar_ass_export_enable").set(1)\n'
        asspath = context.param_value('ass file path').strip()
        if asspath != '':
            script += \
                f'node.parm("ar_ass_file").set({repr(asspath)})\n'
        script += \
            f'for frame in {repr(frames)}:\n' \
            f'    hou.setFrame(frame)\n' \
            f'    skipped = False\n'
        if context.param_value('skip if exists'):
            script += \
                f'    skipped = os.path.exists(node.parm("ar_ass_file").evalAsString())\n'
        script += \
            f'    if skipped:\n' \
            f'        print("output file already exists, skipping frame %d" % frame)\n' \
            f'    else:\n' \
            f'        print("rendering frame %d" % frame)\n' \
            f'        node.render(frame_range=(frame, frame))\n'
        if spawnlines is not None:
            script += \
                f'    if {repr(context.param_value("gen for skipped"))} or not skipped:\n' \
                f'{spawnlines}'
        script += \
            f'print("all done!")\n'

        inv = InvocationJob(['hython', ':/work_to_do.py'], env=env)
        inv.set_extra_file('work_to_do.py', script)
        res = ProcessingResult(job=inv)
        return res

    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()
