from lifeblood.basenode import BaseNodeWithTaskRequirements
from lifeblood.enums import NodeParameterType
from lifeblood.nodethings import ProcessingResult, ProcessingContext, ProcessingError
from lifeblood.invocationjob import InvocationJob, InvocationEnvironment
from lifeblood.text import filter_by_pattern

from typing import Iterable


def node_class():
    return BlenderBatchRender


class BlenderBatchRender(BaseNodeWithTaskRequirements):
    @classmethod
    def label(cls) -> str:
        return 'blender batch render'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'blender', 'batch', 'render', 'stock'

    @classmethod
    def type_name(cls) -> str:
        return 'blender_batch_render'

    @classmethod
    def description(cls) -> str:
        return 'This node renders a blender file.\n' \
               '\n' \
               '- If second input is disconnected: attribute "images" will be created\n' \
               '  on processed task containing a list of all images rendered by it.\n' \
               '- If second input is connected: attribute "images" will NOT be created\n' \
               '  on processed task, instead a child task will be spawned from the second\n' \
               '  input for EACH frame with "file" attribute having the path to the image\n' \
               '  Also spawned children tasks will get all attributes from the parent that\n' \
               '  match "attributes to copy to children" parameter mask\n' \
               '\n' \
               'Pre Script: if provided - this python script is run on the given file BEFORE render\n' \
               'If in addition "Skip Actual Render" checkbox is set - then ONLY this "Pre Script"\n' \
               'will be executed, no render will be done\n' \
               '\n' \
               '(Note: currently you CANNOT modify task attributes from the pre-script)'

    def __init__(self, name):
        super(BlenderBatchRender, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.922, 0.467, 0.0)
            ui.color_scheme().set_secondary_color(0.129, 0.337, 0.533)
            ui.add_output_for_spawned_tasks()

            ui.add_parameter('file_path', 'File Path', NodeParameterType.STRING, "`task['file']`")
            with ui.parameters_on_same_line_block():
                do_over = ui.add_parameter('do_override_frame', 'Override Frame', NodeParameterType.BOOL, False)
                ui.add_parameter('override_frame_start', None, NodeParameterType.INT, 1) \
                    .append_visibility_condition(do_over, '==', True)
                ui.add_parameter('override_frame_end', None, NodeParameterType.INT, 100) \
                    .append_visibility_condition(do_over, '==', True)
                ui.add_parameter('override_frame_step', None, NodeParameterType.INT, 1) \
                    .append_visibility_condition(do_over, '==', True)
                ui.add_parameter('frames_attrib_name', 'Frames Attribute', NodeParameterType.STRING, 'frames') \
                    .append_visibility_condition(do_over, '==', False)
            with ui.parameters_on_same_line_block():
                do_over = ui.add_parameter('do_override_scene', 'Override Scene', NodeParameterType.BOOL, False)
                ui.add_parameter('override_scene', None, NodeParameterType.STRING, 'scene001') \
                    .append_visibility_condition(do_over, '==', True)
            with ui.parameters_on_same_line_block():
                do_over = ui.add_parameter('do_override_output', 'Override Output', NodeParameterType.BOOL, False)
                ui.add_parameter('override_output', None, NodeParameterType.STRING, '//something.####.png') \
                    .append_visibility_condition(do_over, '==', True)
            ui.add_parameter('attrs', 'attributes to copy to children', NodeParameterType.STRING, '*')

            ui.add_separator()

            do_over = ui.add_parameter('do_pre_script', 'Pre Script', NodeParameterType.BOOL, False)
            ui.add_parameter('pre_script', None, NodeParameterType.STRING, '# task var is available here\n# get task attribute with task[\'attr_name\']\n\n') \
                .append_visibility_condition(do_over, '==', True) \
                .set_text_multiline(syntax_hint='python')
            ui.add_parameter('skip_render', 'Skip Actual Render', NodeParameterType.BOOL, False) \
                .append_visibility_condition(do_over, '==', True)

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        file_path = context.param_value('file_path')
        do_frames = context.param_value('do_override_frame')
        do_scene = context.param_value('do_override_scene')
        do_output = context.param_value('do_override_output')
        do_prescript = context.param_value('do_pre_script')

        script_prefix = f'class _TaskWrapper:\n' \
                        f'    def __init__(self):\n' \
                        f'        self.__attrs = {repr(dict(context.task_attributes()))}\n' \
                        f'    @property\n' \
                        f'    def name(self):\n' \
                        f'        return {repr(context.task_field("name"))}\n' \
                        f'    @property\n' \
                        f'    def id(self):\n' \
                        f'        return {repr(context.task_id())}\n' \
                        f'    def attrib_names(self):\n' \
                        f'        return tuple(self.__attrs.keys())\n' \
                        f'    def __getitem__(self, key):\n' \
                        f'        return self.__attrs[key]\n' \
                        f'\n' \
                        f'task = _TaskWrapper()\n' \
                        f'\n\n'
        scripts = {}

        args = ['blender', '-b', file_path]

        if do_scene:
            args += ['-S', context.param_value('override_scene')]

        if do_output:
            args += ['-o', context.param_value('override_output')]

        if do_prescript:
            scripts['prescript.py'] = script_prefix + context.param_value('pre_script')
            args += ['-P', ':/prescript.py']

        # if not self.is_output_connected('spawned'):  # in case we spawn tasks we need to do everything very different
        #     if do_frames:
        #         f_start = context.param_value('override_frame_start')
        #         f_end = context.param_value('override_frame_end')
        #         f_step = context.param_value('override_frame_step')
        #         args += ['-j', f'{f_step}']
        #         args += ['-f', f'{f_start}..{f_end}']
        #     else:
        #         frames_name = context.param_value('frames_attrib_name')
        #         if not context.task_has_attribute(frames_name):
        #             raise ProcessingError(f'attribute "{frames_name}" not found')
        #         frames = context.task_attribute(frames_name)
        #         if not isinstance(frames, list):
        #             if not isinstance(frames, (int, float)):
        #                 raise ProcessingError(f'attribute "{frames_name}" is of wrong type "{type(frames)}"')
        #             frames = [frames]
        #         if any(not isinstance(x, (int, float)) for x in frames):
        #             raise ProcessingError(f'attribute "{frames_name}" has non numeric items')
        #         args += ['-f', ','.join(str(x) for x in frames)]
        #
        # else:
        attrs = context.task_attributes()
        matching_attrnames = filter_by_pattern(context.param_value('attrs'), attrs.keys())
        attr_to_trans = tuple((x, attrs[x]) for x in matching_attrnames if x not in ('frames', 'file', 'outimage'))

        if do_frames:
            f_start = context.param_value('override_frame_start')
            f_end = context.param_value('override_frame_end')
            f_step = context.param_value('override_frame_step')
            # currently override frames parameters are INTs, so we can do range
            frames = list(range(f_start, f_end+1, f_step))  # end inclusive
        else:
            frames_name = context.param_value('frames_attrib_name')
            if not context.task_has_attribute(frames_name):
                raise ProcessingError(f'attribute "{frames_name}" not found')
            frames = context.task_attribute(frames_name)
            if not isinstance(frames, list):
                if not isinstance(frames, (int, float)):
                    raise ProcessingError(f'attribute "{frames_name}" is of wrong type "{type(frames)}"')
                frames = [frames]
            if any(not isinstance(x, (int, float)) for x in frames):
                raise ProcessingError(f'attribute "{frames_name}" has non numeric items')

        # is correct one frame_current or frame_current_final ?
        spawnlines = \
            f"    attrs = {{'frames': [frame], 'blendfile': {repr(file_path)}, 'file': outimage}}\n" \
            f"    for attr, val in {repr(attr_to_trans)}:\n" \
            f"        attrs[attr] = val\n" \
            f"    lifeblood_connection.create_task('{context.task_name()}_spawned frame %g' % frame, attrs)\n"

        do_spawn = self.is_output_connected('spawned')
        main_script = f'import bpy\n' \
                      f'import lifeblood_connection\n' \
                      f'scene = bpy.context.scene\n' \
                      f'all_outimages = []\n' \
                      f'for frame in {repr(frames)}:\n' \
                      f'    scene.frame_set(int(frame), subframe=frame%1.0)\n' \
                      f'    outimage = scene.render.frame_path()\n' \
                      f'    all_outimages.append(outimage)\n' \
                      f'    _filepath_stash = scene.render.filepath\n' \
                      f'    scene.render.filepath = outimage\n' \
                      f'    bpy.ops.render.render(write_still=True)\n' \
                      + (spawnlines if do_spawn else '') + \
                      f'    scene.render.filepath = _filepath_stash\n' \
                      + ('lifeblood_connection.set_attributes({"images": all_outimages})\n' if not do_spawn else '') \
                      + (f'lifeblood_connection.wait_for_currently_running_async_operations()\n' if do_spawn else '')

        if not context.param_value('do_pre_script') or not context.param_value('skip_render'):
            scripts['main_script.py'] = main_script
            args += ['-P', ':/main_script.py']

        invoc = InvocationJob(args)
        for file_name, file_contents in scripts.items():
            invoc.set_extra_file(file_name, file_contents)
        return ProcessingResult(invoc)
