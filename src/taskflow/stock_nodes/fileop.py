import os
import shutil
from taskflow.basenode import BaseNode
from taskflow.nodethings import ProcessingResult, ProcessingError
from taskflow.invocationjob import InvocationJob
from taskflow.processingcontext import ProcessingContext
from taskflow.uidata import NodeParameterType

from typing import Iterable


def node_class():
    return FileOperations


class FileOperations(BaseNode):
    def __init__(self, name):
        super(FileOperations, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('on workers', 'submit to workers', NodeParameterType.BOOL, False)
            with ui.multigroup_parameter_block('item count'):
                with ui.parameters_on_same_line_block():
                    ui.add_parameter('path', 'do', NodeParameterType.STRING, '')
                    ui.add_parameter('op', 'operation', NodeParameterType.STRING, 'nop') \
                        .add_menu((('nothing', 'nop'),
                                   ('create', 'touch'),
                                   ('create dir', 'mkdir'),
                                   ('create base dir', 'mkdirbase'),
                                   ('delete', 'rm')))

    @classmethod
    def label(cls) -> str:
        return 'file operations'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'file', 'operation', 'create', 'delete', 'remove'

    @classmethod
    def type_name(cls) -> str:
        return 'fileops'

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        item_count = context.param_value('item count')
        if context.param_value('on workers'):
            scriptlines = []
            for i in range(item_count):
                path = os.path.realpath(context.param_value(f'path_{i}'))
                op = context.param_value(f'op_{i}')

                if op == 'nop':
                    pass
                elif op == 'touch':
                    scriptlines.extend(
                        [f'os.makedirs(os.path.dirname({repr(path)}), exist_ok=True)',
                         f'with open({repr(path)}, "wb") as f:',
                         f'    pass'])
                elif op == 'mkdir':
                    scriptlines.append(f'os.makedirs({repr(path)}, exist_ok=True)')
                elif op == 'mkdirbase':
                    scriptlines.append(f'os.makedirs(os.path.dirname({repr(path)}, exist_ok=True)')
                elif op == 'rm':
                    scriptlines.extend(
                        [f'if os.path.exists({repr(path)}):',
                         f'    if os.path.isfile({repr(path)}):',
                         f'        os.remove({repr(path)})',
                         f'    elif os.path.isdir({repr(path)}):',
                         f'        shutil.rmtree({repr(path)})',
                         f'    else:',
                         f'        raise RuntimeError(f\'cannot determine is "{path}" a file or a dir\')'])
                else:
                    raise ProcessingError(f'unknown operation: "{op}"')
            if len(scriptlines) == 0:
                return ProcessingResult()
            scriptlines = ['import os', 'import shutil'] + scriptlines

            job = InvocationJob(['python', ':/script.py'])
            job.set_extra_file('script.py', '\n'.join(scriptlines))
            return ProcessingResult(job)
        else:
            for i in range(item_count):
                path = os.path.realpath(context.param_value(f'path_{i}'))
                op = context.param_value(f'op_{i}')

                if op == 'nop':
                    pass
                elif op == 'touch':
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    with open(path, 'wb') as f:
                        pass
                elif op == 'mkdir':
                    os.makedirs(path, exist_ok=True)
                elif op == 'mkdirbase':
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                elif op == 'rm':
                    if os.path.exists(path):
                        if os.path.isfile(path):
                            os.remove(path)
                        elif os.path.isdir(path):
                            shutil.rmtree(path)
                        else:
                            raise ProcessingError(f'cannot determine is "{path}" a file or a dir')
                else:
                    raise ProcessingError(f'unknown operation: "{op}"')
        return ProcessingResult()

    def postprocess_task(self, context: ProcessingContext) -> ProcessingResult:
        return ProcessingResult()
