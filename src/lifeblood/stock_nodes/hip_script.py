from lifeblood.basenode import BaseNodeWithTaskRequirements
from lifeblood.enums import NodeParameterType
from lifeblood.nodethings import ProcessingResult, ProcessingError
from lifeblood.invocationjob import InvocationJob, InvocationEnvironment
from lifeblood.text import filter_by_pattern

from typing import Iterable


def node_class():
    return HipScript


class HipScript(BaseNodeWithTaskRequirements):
    @classmethod
    def label(cls) -> str:
        return 'hip script executor'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'hip', 'houdini', 'script', 'python', 'hou'

    @classmethod
    def type_name(cls) -> str:
        return 'hip_script'

    @classmethod
    def description(cls) -> str:
        return 'opens hip, executes script and saves hip to the same or new location\n'

    def __init__(self, name):
        super(HipScript, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.5, 0.25, 0.125)
            ui.add_parameter('hip path', 'hip path', NodeParameterType.STRING, "`task['hipfile']`")
            with ui.parameters_on_same_line_block():
                mask_hip = ui.add_parameter('mask as different hip', 'mask as different hip file', NodeParameterType.BOOL, False)
                ui.add_parameter('mask hip path', '', NodeParameterType.STRING, "`task.get('hipfile_orig', task['hipfile'])`")\
                    .append_visibility_condition(mask_hip, '==', True)
            ui.add_parameter('script', 'script', NodeParameterType.STRING, '').set_text_multiline(syntax_hint='python')

            with ui.parameters_on_same_line_block():
                save_hip = ui.add_parameter('save different hip', 'save resulted file to a different location', NodeParameterType.BOOL, False)
                ui.add_parameter('save hip path', '', NodeParameterType.STRING, '')\
                    .append_visibility_condition(save_hip, '==', True)

            ui.parameter('worker type').set_hidden(True)
            ui.parameter('worker type').set_locked(True)

    def process_task(self, context) -> ProcessingResult:
        script = 'import os, hou\n'

        source_hip = context.param_value('hip path')
        dest_hip = source_hip
        if context.param_value('save different hip'):
            dest_hip = context.param_value('save hip path')

        if context.param_value('mask as different hip'):
            mask_path = context.param_value('mask hip path')
            script += 'def __fix_hip_env__(event_type=None):\n' \
                      '    if event_type == hou.hipFileEventType.BeforeSave:\n' \
                     f'        hou.hipFile.setName({repr(dest_hip)})\n' \
                      '        return\n' \
                      '    if event_type not in (hou.hipFileEventType.AfterSave, hou.hipFileEventType.AfterLoad) and event_type is not None:\n' \
                      '        return\n' \
                     f'    hou.hipFile.setName({repr(mask_path)})\n' \
                      'hou.hipFile.addEventCallback(__fix_hip_env__)\n'

        script += 'def __main_body__():\n'
        script += '\n'.join(f'    {line}' for line in context.param_value('script').splitlines())

        script += '\n\n' \
                 f'hou.hipFile.load({repr(source_hip)}, ignore_load_warnings=True)\n' \
                  '__main_body__()\n' \
                 f'hou.hipFile.save({repr(dest_hip)})\n'

        job = InvocationJob(['hython', ':/work_to_do.py'])
        job.set_extra_file('work_to_do.py', script)
        return ProcessingResult(job=job)

    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()
