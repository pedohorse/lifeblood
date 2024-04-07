import os
import fnmatch
import inspect
from lifeblood.taskspawn import TaskSpawn
from lifeblood.basenode import BaseNodeWithTaskRequirements
from lifeblood.nodethings import ProcessingResult, ProcessingError
from lifeblood.invocationjob import InvocationJob
from lifeblood.processingcontext import ProcessingContext
from lifeblood.uidata import NodeParameterType
from lifeblood.enums import WorkerType
from lifeblood.text import match_pattern

from typing import Iterable


def node_class():
    return FilePattern


def _scan_one_level(basepath, levels, *, do_files: bool, do_dirs: bool):
    files = []
    if len(levels) == 0:
        return [basepath]
    level = levels[0]
    if level == '' and len(levels) > 1:
        return _scan_one_level(basepath, levels[1:], do_files=do_files, do_dirs=do_dirs)
    if '*' not in level:  # constant value, not a pattern
        path = os.path.join(basepath, level)
        if not os.path.exists(path):
            return []
        return _scan_one_level(path, levels[1:], do_files=do_files, do_dirs=do_dirs)
    else:
        if not os.path.isdir(basepath):
            return []
        for file_name in os.listdir(basepath):
            if not fnmatch.fnmatch(file_name, level):
                continue
            path = os.path.join(basepath, file_name)
            if not os.path.exists(path):
                continue
            if len(levels) == 1:
                if not do_files and os.path.isfile(path)\
                        or not do_dirs and os.path.isdir(path):
                    continue

            files.extend(_scan_one_level(path, levels[1:], do_files=do_files, do_dirs=do_dirs))
    return files


class FilePattern(BaseNodeWithTaskRequirements):
    @staticmethod
    def _scan_one_level(*args, **kwargs):
        """
        exposed for testing
        """
        return _scan_one_level(*args, **kwargs)

    def __init__(self, name):
        super(FilePattern, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_output_for_spawned_tasks()
            ui.color_scheme().set_main_color(0.24, 0.25, 0.48)
            ui.add_parameter('on workers', 'submit to workers', NodeParameterType.BOOL, False)
            ui.add_parameter('pattern', 'file pattern', NodeParameterType.STRING, '')
            ui.add_parameter('file type', 'type', NodeParameterType.INT, 1).add_menu((('files', 1), ('dirs', 2), ('files and dirs', 3)))
            ui.add_parameter('inherit attributes', 'inherit attributes from parent', NodeParameterType.STRING, '')
            ui.parameter('worker type').set_value(WorkerType.SCHEDULER_HELPER.value)

    @classmethod
    def label(cls) -> str:
        return 'file pattern'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'file', 'pattern', 'list', 'ls'

    @classmethod
    def type_name(cls) -> str:
        return 'filepattern'

    @classmethod
    def description(cls) -> str:
        return 'scans file system according to given pattern, like /some/path/dir_*_smth/imagefile.*.exr\n' \
               'if "on worker" is checked - worker will perform the scan,  \n' \
               'otherwise scan will be done by scheduler.\n' \
               '\n' \
               'if you scan thousands of files it\'s more optimal to do work "on worker"  \n' \
               'as it will produce new items asynchronously,  \n' \
               'while when not "on worker" - scan happens synchronously\n'

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        pattern = context.param_value('pattern')
        pattern = pattern.replace('\\', '/')  # simpler to just work with /  no point working with OS-dependant ones
        attribs_to_inherit_pattern = context.param_value('inherit attributes')
        new_attributes_base = {attr: val for attr, val in context.task_attributes().items() if match_pattern(attribs_to_inherit_pattern, attr)}
        file_type_mask = context.param_value('file type')
        do_files = bool(file_type_mask >> 0 & 1)
        do_dirs = bool(file_type_mask >> 1 & 1)

        if context.param_value('on workers'):
            script = (
                'import sys\n'
                'import os\n'
                'import fnmatch\n'
                'import lifeblood_connection\n'
                '\n'
            )
            script += inspect.getsource(_scan_one_level)
            script += (
                '\n\n'
               f"pattern = {repr(pattern)}\n"
               f"parent_task_name = {repr(context.task_field('name'))}\n"
                "parts = pattern.split('/')\n"
                "if len(parts) == 0:\n"
                "    sys.exit(0)\n"
               f"files = _scan_one_level(parts[0] + os.path.sep, parts[1:], do_files={repr(do_files)}, do_dirs={repr(do_dirs)})\n"
                "\n"
                "for i, file in enumerate(files):\n"
               f"    attrs = {repr(new_attributes_base)}\n"
                "    attrs.update({'file': file})\n"
                "    lifeblood_connection.create_task(f\"{parent_task_name}_file{i}\", attributes=attrs)\n"
            )

            job = InvocationJob(['python', ':/script.py'])
            job.set_extra_file('script.py', script)
            return ProcessingResult(job)
        else:
            parts = pattern.split('/')
            if len(parts) == 0:
                return ProcessingResult()
            files = _scan_one_level(parts[0] + os.path.sep, parts[1:], do_files=do_files, do_dirs=do_dirs)
            res = ProcessingResult()
            for i, file in enumerate(files):
                attrs = {**new_attributes_base, 'file': file}
                res.add_spawned_task(TaskSpawn(f"{context.task_field('name')}_file{i}", task_attributes=attrs))

        return res

    def postprocess_task(self, context: ProcessingContext) -> ProcessingResult:
        return ProcessingResult()
