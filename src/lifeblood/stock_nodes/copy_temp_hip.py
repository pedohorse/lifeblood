from lifeblood.basenode import BaseNodeWithTaskRequirements
from lifeblood.enums import NodeParameterType
from lifeblood.nodethings import ProcessingResult, ProcessingError
from lifeblood.invocationjob import InvocationJob, InvocationEnvironment
from lifeblood.text import filter_by_pattern

import os
import shutil

from typing import Iterable


def node_class():
    return CopyHipFile


class CopyHipFile(BaseNodeWithTaskRequirements):
    @classmethod
    def label(cls) -> str:
        return 'hip script executor'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'hip', 'houdini'

    @classmethod
    def type_name(cls) -> str:
        return 'copy hip file'

    @classmethod
    def description(cls) -> str:
        return ''

    def __init__(self, name):
        super(CopyHipFile, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.5, 0.25, 0.125)
            ui.add_parameter('on workers', 'submit to workers', NodeParameterType.BOOL, False)
            ui.add_parameter('hip path', 'source hip path', NodeParameterType.STRING, "`task['hipfile']`")
            ui.add_parameter('dst hip path', 'dest hip path', NodeParameterType.STRING, "")
            with ui.parameters_on_same_line_block():
                mask_hip = ui.add_parameter('save orig', 'save original hip path', NodeParameterType.BOOL, False)
                ui.add_parameter('mask hip path attribute', 'attribute name', NodeParameterType.STRING, "hipfile_orig")\
                    .append_visibility_condition(mask_hip, '==', True)

    def process_task(self, context) -> ProcessingResult:
        src_path = context.param_value('hip path')
        dst_path = context.param_value('dst hip path')
        dst_dir = os.path.dirname(dst_path)

        if context.param_value('on workers'):
            script = 'import os, shutil\n' \
                    f'if not os.path.exists({repr(dst_dir)}):\n' \
                    f'    os.makedirs({repr(dst_dir)}, exist_ok=True)\n' \
                    f'shutil.copy2({repr(src_path)}, {repr(dst_path)})\n'
            job = InvocationJob(['python', ':/work_to_do.py'])
            job.set_extra_file('work_to_do.py', script)
            res = ProcessingResult(job=job)
        else:
            res = ProcessingResult()
            if not os.path.exists(dst_dir):
                os.makedirs(dst_dir, exist_ok=True)
            shutil.copy2(src_path, dst_path)

        res.set_attribute('hipfile', dst_path)
        if context.param_value('save orig'):
            attr_name = context.param_value('mask hip path attribute')
            res.set_attribute(attr_name, src_path)
        return res

    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()
