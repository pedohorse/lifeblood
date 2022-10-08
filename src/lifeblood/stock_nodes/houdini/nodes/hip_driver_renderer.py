from copy import copy
from lifeblood.basenode import BaseNodeWithTaskRequirements
from lifeblood.enums import NodeParameterType
from lifeblood.nodethings import ProcessingResult, ProcessingError
from lifeblood.invocationjob import InvocationJob, InvocationEnvironment
from lifeblood.text import filter_by_pattern

from typing import Iterable


def node_class():
    return HipDriverRenderer


class HipDriverRenderer(BaseNodeWithTaskRequirements):
    @classmethod
    def label(cls) -> str:
        return 'hip driver renderer'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'hip', 'houdini', 'driver', 'render', 'stock'

    @classmethod
    def type_name(cls) -> str:
        return 'hip_driver_renderer'

    @classmethod
    def description(cls) -> str:
        return 'renders given driver node\n' \
               ':hip path: hip file to open\n' \
               ':rop node path: path to rop node to render inside hip\n' \
               ':set these attribs as context: given attribs will be set as global contest in hip. this allows to use them just like in TOPs: with @attribname syntax\n' \
               '    NOTE: your attribute names must be houdini-compatible for that. Lifeblood is way more permissive for attribute names\n' \
               ':render whole range (needed for alembics):\n' \
               ':skip if result already exists:\n' \
               ':override output path:\n' \
               ':use specific parameter name for output:\n'

    def __init__(self, name):
        super(HipDriverRenderer, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.5, 0.25, 0.125)
            ui.add_output_for_spawned_tasks()
            ui.add_parameter('hip path', 'hip path', NodeParameterType.STRING, "`task['hipfile']`")
            with ui.parameters_on_same_line_block():
                mask_hip = ui.add_parameter('mask as different hip', 'mask as different hip file', NodeParameterType.BOOL, False)
                ui.add_parameter('mask hip path', '', NodeParameterType.STRING, "`task.get('hipfile_orig', task['hipfile'])`")\
                    .append_visibility_condition(mask_hip, '==', True)
            ui.add_parameter('driver path', 'rop node path', NodeParameterType.STRING, "`task['hipdriver']`")
            ui.add_parameter('ignore inputs', 'ignore rop inputs', NodeParameterType.BOOL, True)
            ui.add_parameter('attrs to context', 'set these attribs as context', NodeParameterType.STRING, '')
            ui.add_parameter('whole range', 'render whole range (needed for alembics)', NodeParameterType.BOOL, False)
            with ui.parameters_on_same_line_block():
                skipparam = ui.add_parameter('skip if exists', 'skip if result already exists', NodeParameterType.BOOL, False)
                ui.add_parameter('gen for skipped', 'generate children for skipped', NodeParameterType.BOOL, True).append_visibility_condition(skipparam, '==', True)
            with ui.parameters_on_same_line_block():
                override = ui.add_parameter('do override output', 'override output path', NodeParameterType.BOOL, False)
                ui.add_parameter('override output', None, NodeParameterType.STRING, '').append_visibility_condition(override, '==', True)
            with ui.parameters_on_same_line_block():
                pnoverride = ui.add_parameter('do override parmname', 'use specific parameter name for output', NodeParameterType.BOOL, False)
                ui.add_parameter('override parmname', None, NodeParameterType.STRING, 'sopoutput').append_visibility_condition(pnoverride, '==', True)

            ui.add_parameter('attrs', 'attributes to copy to children', NodeParameterType.STRING, '*')
            ui.add_parameter('attrs to extract', 'detail attributes to extract', NodeParameterType.STRING, '')
            ui.add_parameter('intrinsics to extract', 'detail intrinsics to extract', NodeParameterType.STRING, '')

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
            raise ProcessingError('required attribs not found')
        hippath = context.param_value('hip path')
        driverpath = context.param_value('driver path')
        frames = attrs['frames']
        matching_attrnames = filter_by_pattern(context.param_value('attrs'), attrs.keys())
        attr_to_trans = tuple((x, attrs[x]) for x in matching_attrnames if x not in ('frames', 'file'))
        attr_to_promote_mask = context.param_value('attrs to extract').strip()
        intr_to_promote_mask = context.param_value('intrinsics to extract').strip()
        attr_to_context = filter_by_pattern(context.param_value('attrs to context'), attrs.keys())

        env = InvocationEnvironment()

        spawnlines =       "kwargs = {{'frames': {frames}}}\n" \
                          f"for attr, val in {repr(attr_to_trans)}:\n" \
                          f"    kwargs[attr] = val\n" \
                          f"kwargs['hipfile'] = {repr(hippath)}\n" \
                          f"kwargs['file'] = node.evalParm(output_parm_name)\n"
        if attr_to_promote_mask != '' or intr_to_promote_mask != '':
            spawnlines += f"geo = hou.Geometry()\n" \
                          f"geo.loadFromFile(node.parm(output_parm_name).evalAsString())\n"
            if attr_to_promote_mask != '':
                spawnlines += \
                          f"detail_attrs_names = geo.intrinsicValue('globalattributes')\n" \
                          f"if not isinstance(detail_attrs_names, (tuple, list)):  # believe it or not - in case of a single attrib - the return value is a string, not a tuple!\n" \
                          f"    detail_attrs_names = (detail_attrs_names,)\n" \
                          f"for detail_attr_name in (x for x in detail_attrs_names if hou.patternMatch({repr(attr_to_promote_mask)}, x)):\n" \
                          f"    kwargs[detail_attr_name] = geo.attribValue(detail_attr_name)\n"
            if intr_to_promote_mask != '':
                spawnlines += \
                          f"for intrinsic_name in geo.intrinsicNames():\n" \
                          f"    if not hou.patternMatch({repr(intr_to_promote_mask)}, intrinsic_name):\n" \
                          f"        continue\n" \
                          f"    kwargs[intrinsic_name] = geo.intrinsicValue(intrinsic_name)\n"
            spawnlines += f"geo.clear()\n"
        spawnlines +=     f"lifeblood_connection.create_task(node.name() + '_spawned frame %g' % frame, kwargs)\n"

        if not self.is_output_connected('spawned'):
            spawnlines = None

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

        if context.param_value('do override parmname'):
            parm_name = context.param_value('override parmname')
            script += \
                f'output_parm_name = {repr(parm_name)}\n'
        else:  # try to autodetect
            script += \
                f'if node.type().name() in ("geometry", "rop_geometry"):\n' \
                f'    output_parm_name = "sopoutput"\n' \
                f'elif node.type().name() in ("alembic", "rop_alembic"):\n' \
                f'    output_parm_name = "filename"\n' \
                f'elif node.parm("sopoutput"):  # blindly trying common names\n' \
                f'    output_parm_name = "sopoutput"\n' \
                f'elif node.parm("filename"):\n' \
                f'    output_parm_name = "filename"\n' \
                f'else:\n' \
                f'    raise RuntimeError("cannot autodetect output parameter for given node type %s " % node.type().name())\n'

        if context.param_value('do override output'):
            override_path = context.param_value('override output').strip()
            if override_path != '':
                script += f'node.parm(output_parm_name).set({repr(override_path)})\n'

        if context.param_value('whole range'):
            script += \
                f'hou.setFrame({frames[0]})\n' \
                f'skipped = False\n'
            if context.param_value('skip if exists'):
                script += 'skipped = os.path.exists(node.parm(output_parm_name).evalAsString())\n'

            script += 'if skipped:\n' \
                      '    print("output file already exists, skipping")\n' \
                      'else:\n' \
                      f'    node.render(frame_range=({frames[0]}, {frames[-1]}, {frames[min(1, len(frames)-1)] - frames[0] or 1}), ignore_inputs={context.param_value("ignore inputs")})\n'
            if spawnlines is not None:
                script +=  f'if {repr(context.param_value("gen for skipped"))} or not skipped:\n'
                spawnlines = spawnlines.format(frames=repr(frames))
                script += '\n'.join(f'    {line}' for line in spawnlines.split('\n')) + '\n'

        else:
            # temporary disallow rendering alembics not in full range
            script += f'if os.path.splitext(node.parm(output_parm_name).evalAsString())[1] == ".abc":\n' \
                      f'    raise RuntimeError("not whole range rendering for alembics is not implemented yet")\n'
            ##

            script += \
                f'for i, frame in enumerate({repr(frames)}):\n' \
                f'    hou.setFrame(frame)\n' \
                f'    skipped = False\n' \
                f'    print("ALF_PROGRESS {{}}%".format(int(i*100.0/{len(frames)})))\n'
            if context.param_value('skip if exists'):
                script += '    skipped = os.path.exists(node.parm(output_parm_name).evalAsString())\n'

            script += f'    if skipped:\n' \
                      f'        print("output file already exists, skipping frame {{}}".format(frame))\n' \
                      f'    else:\n' \
                      f'        print("rendering frame {{}}".format(frame))\n' \
                      f'        node.render(frame_range=(frame, frame), ignore_inputs={context.param_value("ignore inputs")})\n'
            if spawnlines is not None:
                script += f'    if {repr(context.param_value("gen for skipped"))} or not skipped:\n'
                spawnlines = spawnlines.format(frames='[frame]')
                script += '\n'.join(f'        {line}' for line in spawnlines.split('\n')) + '\n'

        script += f'print("all done!")\n'

        inv = InvocationJob(['hython', ':/work_to_do.py'], env=env)
        inv.set_extra_file('work_to_do.py', script)
        res = ProcessingResult(job=inv)
        return res

    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()
