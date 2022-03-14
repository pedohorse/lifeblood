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

from typing import Iterable


def node_class():
    return FilePattern


def _scan_one_level(basepath, levels):
    files = []
    if len(levels) == 0:
        return [basepath]
    level = levels[0]
    if level == '':
        return _scan_one_level(basepath, levels[1:])
    if '*' not in level:  # constant value, not a pattern
        path = os.path.join(basepath, level)
        if not os.path.exists(path):
            return []
        return _scan_one_level(path, levels[1:])
    else:
        if not os.path.isdir(basepath):
            return []
        for file_name in os.listdir(basepath):
            if not fnmatch.fnmatch(file_name, level):
                continue
            path = os.path.join(basepath, file_name)
            if not os.path.exists(path):
                continue
            files.extend(_scan_one_level(path, levels[1:]))
    return files


class FilePattern(BaseNodeWithTaskRequirements):
    def __init__(self, name):
        super(FilePattern, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.24, 0.25, 0.48)
            ui.add_parameter('on workers', 'submit to workers', NodeParameterType.BOOL, False)
            ui.add_parameter('pattern', 'file pattern', NodeParameterType.STRING, '')
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
        if context.param_value('on workers'):
            script = inspect.getsource(_scan_one_level)
            script += '\n\n' \
                      "parts = pattern.split('/')\n" \
                      "if len(parts):\n" \
                      "    return ProcessingResult()\n" \
                      "files = _scan_one_level(parts[0] or '/', parts[1:])\n"

            job = InvocationJob(['python', ':/script.py'])
            job.set_extra_file('script.py', script)
            return ProcessingResult(job)
        else:
            parts = pattern.split('/')
            if len(parts):
                return ProcessingResult()
            files = _scan_one_level(parts[0] or '/', parts[1:])
            res = ProcessingResult()
            for i, file in enumerate(files):
                attrs = {'file': file}
                res.add_spawned_task(TaskSpawn(f"{context.task_field('name')}_file{i}", task_attributes=attrs))

        return res

    def postprocess_task(self, context: ProcessingContext) -> ProcessingResult:
        return ProcessingResult()
