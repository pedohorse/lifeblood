from lifeblood.basenode import BaseNodeWithTaskRequirements
from lifeblood.enums import NodeParameterType, WorkerType
from lifeblood.nodethings import ProcessingResult, ProcessingError
from lifeblood.invocationjob import InvocationJob, InvocationEnvironment
from lifeblood.text import filter_by_pattern


import os
import shutil
import tempfile

from typing import Iterable


def node_class():
    return CopyHipFile


class CopyHipFile(BaseNodeWithTaskRequirements):
    @classmethod
    def label(cls) -> str:
        return 'hip file operations start'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'hip', 'houdini', 'copy', 'start'

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
            ui.add_parameter('dst hip path', 'dest hip path', NodeParameterType.STRING, "`config['global_scratch_location']`/`node.name`")
            ui.add_parameter('append unique hipname', 'append unique temporary hip name to destination', NodeParameterType.BOOL, True)
            with ui.parameters_on_same_line_block():
                mask_hip = ui.add_parameter('save orig', 'save original hip path', NodeParameterType.BOOL, False)
                ui.add_parameter('mask hip path attribute', 'attribute name', NodeParameterType.STRING, "hipfile_orig")\
                    .append_visibility_condition(mask_hip, '==', True)

                ui.parameter('worker type').set_value(WorkerType.SCHEDULER_HELPER.value)

    def process_task(self, context) -> ProcessingResult:
        src_path = context.param_value('hip path')
        dst_path = context.param_value('dst hip path')

        if context.param_value('on workers'):
            script = 'import os, shutil, tempfile\n' \
                     'import lifeblood_connection\n' \
                    f'dst_path = {repr(dst_path)}\n'
            if context.param_value('append unique hipname'):
                script += "_fd, dst_path = tempfile.mkstemp(suffix='.hip', dir=dst_path)\n" \
                          'os.close(_fd)\n'
            script += 'dst_dir = os.path.dirname(dst_path)\n' \
                      'if not os.path.exists(dst_dir):\n' \
                      '    os.makedirs(dst_dir, exist_ok=True)\n' \
                     f'shutil.copy2({repr(src_path)}, dst_path)\n' \
                      'attrs_to_set = {}\n' \
                      'attrs_to_set["hipfile"] = dst_path\n'
            if context.param_value('save orig'):
                script += f'attrs_to_set[{repr(context.param_value("mask hip path attribute"))}] = {repr(src_path)}\n'
            script += 'lifeblood_connection.set_attributes(attrs_to_set, blocking=True)\n'
            job = InvocationJob(['python', ':/work_to_do.py'])
            job.set_extra_file('work_to_do.py', script)
            return ProcessingResult(job=job)

        if context.param_value('append unique hipname'):
            fd, dst_path = tempfile.mkstemp(suffix='.hip', dir=dst_path)
            os.close(fd)
        dst_dir = os.path.dirname(dst_path)
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
