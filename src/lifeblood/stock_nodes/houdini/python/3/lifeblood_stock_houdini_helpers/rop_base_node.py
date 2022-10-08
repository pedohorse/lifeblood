from lifeblood.basenode import BaseNodeWithTaskRequirements
from lifeblood.enums import NodeParameterType
from lifeblood.nodethings import ProcessingResult, ProcessingError
from lifeblood.invocationjob import InvocationJob, InvocationEnvironment
from lifeblood.text import filter_by_pattern

from typing import Iterable, Optional


class RopBaseNode(BaseNodeWithTaskRequirements):
    def __init__(self, name):
        super(RopBaseNode, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_output_for_spawned_tasks()
            ui.add_parameter('hip path', 'hip file path', NodeParameterType.STRING, "`task['hipfile']`")
            with ui.parameters_on_same_line_block():
                mask_hip = ui.add_parameter('mask as different hip', 'mask as different hip file', NodeParameterType.BOOL, False)
                ui.add_parameter('mask hip path', '', NodeParameterType.STRING, "`task.get('hipfile_orig', task['hipfile'])`")\
                    .append_visibility_condition(mask_hip, '==', True)
            ui.add_parameter('driver path', 'rop node path', NodeParameterType.STRING, "`task['hipdriver']`")
            ui.add_parameter('attrs to context', 'set these attribs as context', NodeParameterType.STRING, '')
            ui.add_parameter('scene file output', 'scene description file path', NodeParameterType.STRING, "")
            with ui.parameters_on_same_line_block():
                skipparam = ui.add_parameter('skip if exists', 'skip if result already exists', NodeParameterType.BOOL, False)
                ui.add_parameter('gen for skipped', 'generate children for skipped', NodeParameterType.BOOL, True).append_visibility_condition(skipparam, '==', True)
            ui.add_parameter('attrs', 'attributes to copy to children', NodeParameterType.STRING, '')

            ui.parameter('worker type').set_hidden(True)
            ui.parameter('worker type').set_locked(True)

    def _take_parm_name(self, context) -> str:
        raise NotImplementedError()

    def _parms_to_set_before_render(self, context) -> dict:
        return {}

    def _scene_file_getting_code(self, context) -> Optional[str]:
        """
        if string is returned - it's expected to be body of a function returning str
        aruments provided to the function: node - driver node, frame - frame currently being rendered
        code is expected to return string (scene_file path)
        result of _scene_file_parm_name is ignored then

        :param context:
        :return:
        """
        return None

    def _scene_file_parm_name(self, context) -> str:
        raise NotImplementedError()

    def _node_specialization_code(self, context) -> Optional[str]:
        """
        if string is returned - it's expected to be body of a function returning str
        arguments provided to the function: node - driver node
        code is expected to return node

        sometimes you want flexibility of allowing providing multiple types of driver nodes in the interface
        in that ces this function code must find the needed node from node provided

        :param context:
        :return:
        """
        return None

    def _image_getting_code(self, context) -> Optional[str]:
        """
        if string is returned - it's expected to be body of a function returning str
        aruments provided to the function: node - driver node, frame - frame currently being rendered
        code is expected to return string (image path)
        result of _image_path_parm_name is ignored then

        :param context:
        :return:
        """
        return None

    def _image_path_parm_name(self, context) -> str:
        raise NotImplementedError()

    def process_task(self, context) -> ProcessingResult:
        """
        this node expects to find the following attributes:
        frames
        hipfile
        hipdriver

        :param context:
        :return:
        """
        take_parm_name = self._take_parm_name(context)
        parms_to_set_before_render = self._parms_to_set_before_render(context)
        scene_file_code = self._scene_file_getting_code(context)
        scene_file_parm_name = self._scene_file_parm_name(context)
        image_path_code = self._image_getting_code(context)
        image_path_parm_name = self._image_path_parm_name(context)
        node_specialization_code = self._node_specialization_code(context)

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
            f"        filepath = _that_one_scene_file_path_getting_function(node, frame)\n" \
            f"        outimage = _that_one_image_path_getting_function(node, frame)\n" \
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

        script += 'def _that_one_image_path_getting_function(node, frame) -> str:\n'
        if image_path_code:
            script += '\n'.join(f'    {x}' for x in image_path_code.splitlines()) + '\n\n'
        else:
            script += f'    return node.evalParm({repr(image_path_parm_name)})\n\n'

        script += 'def _that_one_scene_file_path_getting_function(node, frame) -> str:\n'
        if scene_file_code:
            script += '\n'.join(f'    {x}' for x in scene_file_code.splitlines()) + '\n\n'
        else:
            script += f'    return node.evalParm({repr(scene_file_parm_name)})\n\n'

        if node_specialization_code:
            script += 'def _that_one_node_specialization_function(node):\n'
            script += '\n'.join(f'    {x}' for x in node_specialization_code.splitlines()) + '\n\n'

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

        if node_specialization_code:
            script += 'node = _that_one_node_specialization_function(node)\n'

        # set fake take to allow editing even locked nodes
        if take_parm_name:
            script += f'base_take_name = node.parm({repr(take_parm_name)}).evalAsString()\n' \
                      f'if base_take_name == "_current_":\n' \
                      f'    base_take = hou.takes.currentTake()\n' \
                      f'else:\n' \
                      f'    base_take = hou.takes.findTake(base_take_name)\n' \
                      f'fake_take = base_take.addChildTake()\n' \
                      f'hou.takes.setCurrentTake(fake_take)\n' \
                      f'fake_take.addParmTuplesFromNode(node)\n'

        for parm_name, value in parms_to_set_before_render.items():
            script += \
                f'if node.parm({repr(parm_name)}).eval() != {repr(value)}:\n' \
                f'    node.parm({repr(parm_name)}).deleteAllKeyframes()\n' \
                f'    node.parm({repr(parm_name)}).set({repr(value)})\n'
        scene_description_path = context.param_value('scene file output').strip()
        if scene_description_path != '':
            script += \
                f'node.parm({repr(scene_file_parm_name)}).deleteAllKeyframes()\n' \
                f'node.parm({repr(scene_file_parm_name)}).set({repr(scene_description_path)})\n'

        script += \
            f'for frame in {repr(frames)}:\n' \
            f'    hou.setFrame(frame)\n' \
            f'    skipped = False\n'
        if context.param_value('skip if exists'):
            script += \
                f'    skipped = os.path.exists(node.parm({repr(scene_file_parm_name)}).evalAsString())\n'
        script += \
            f'    if skipped:\n' \
            f'        print("output file already exists, skipping frame %d" % frame)\n' \
            f'    else:\n' \
            f'        print("rendering frame %d" % frame)\n' \
            f'        node.render(frame_range=(frame, frame))\n'
        if spawnlines:
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
