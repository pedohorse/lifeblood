import os
import shutil
from lifeblood.basenode import BaseNode
from lifeblood.nodethings import ProcessingResult, ProcessingError
from lifeblood.invocationjob import InvocationJob
from lifeblood.processingcontext import ProcessingContext
from lifeblood.uidata import NodeParameterType

from typing import Iterable


def node_class():
    return FileOperations


class FileOperations(BaseNode):
    def __init__(self, name):
        super(FileOperations, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.24, 0.25, 0.48)
            ui.add_parameter('on workers', 'submit to workers', NodeParameterType.BOOL, False)
            with ui.multigroup_parameter_block('item count'):
                with ui.parameters_on_same_line_block():
                    ui.add_parameter('path', 'do', NodeParameterType.STRING, '')
                    opparam = ui.add_parameter('op', 'operation', NodeParameterType.STRING, 'nop') \
                        .add_menu((('nothing', 'nop'),
                                   ('list files', 'ls'),
                                   ('create', 'touch'),
                                   ('create dir', 'mkdir'),
                                   ('create base dir', 'mkdirbase'),
                                   ('delete', 'rm'),
                                   ('copy', 'cp'),
                                   ('move', 'mv')))
                    ui.add_parameter('other path', 'to here', NodeParameterType.STRING, '') \
                        .append_visibility_condition(opparam, 'in', ('cp', 'mv'))
                    ui.add_parameter('op res', 'save as this attribute', NodeParameterType.STRING, 'files') \
                        .append_visibility_condition(opparam, '==', 'ls')

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
            scriptlines = []  # init with no lines, so later easier to check if nothing was added to them
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
                elif op == 'ls':
                    res_attr_name = context.param_value(f"op res_{i}").strip()
                    if res_attr_name == '':
                        raise ProcessingError('resulting attribute name cannot be empty')
                    scriptlines.append(f'attribs_to_set[{repr(res_attr_name)}] = os.listdir({repr(path)})')
                elif op == 'mkdirbase':
                    scriptlines.append(f'os.makedirs(os.path.dirname({repr(path)}, exist_ok=True)')
                elif op in ('cp', 'mv', 'rm'):
                    if op in ('cp', 'mv'):
                        other_path = os.path.realpath(context.param_value(f'other path_{i}'))
                        scriptlines.extend(
                            [f'if os.path.isfile({repr(path)}):',
                             f'    shutil.copy2({repr(path)}, {repr(other_path)})',
                             f'elif os.path.isdir(path):',
                             f'    shutil.copytree(path, other_path, dirs_exist_ok=True)',
                             f'else:',
                             f'    raise RuntimeError(\'cannot determine is "{path}" a file or a dir\')']
                        )
                    if op in ('mv', 'rm'):
                        scriptlines.extend(
                            [f'if os.path.exists({repr(path)}):',
                             f'    if os.path.isfile({repr(path)}):',
                             f'        os.remove({repr(path)})',
                             f'    elif os.path.isdir({repr(path)}):',
                             f'        shutil.rmtree({repr(path)})',
                             f'    else:',
                             f'        raise RuntimeError(\'cannot determine is "{path}" a file or a dir\')'])
                else:
                    raise ProcessingError(f'unknown operation: "{op}"')
            if len(scriptlines) == 0:
                return ProcessingResult()
            scriptlines = ['import os',
                           'import shutil',
                           'import lifeblood_connection',
                           'attribs_to_set = {}'
                           ] + scriptlines + \
                          ['if attribs_to_set:',
                           '    lifeblood_connection.set_attributes(attribs_to_set, blocking=True)'
                           ]

            job = InvocationJob(['python', ':/script.py'])
            job.set_extra_file('script.py', '\n'.join(scriptlines))
            return ProcessingResult(job)
        else:
            result = ProcessingResult()
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
                elif op == 'ls':
                    res_attr_name = context.param_value(f"op res_{i}").strip()
                    if res_attr_name == '':
                        raise ProcessingError('resulting attribute name cannot be empty')
                    result.set_attribute(res_attr_name, list(os.listdir(path)))
                elif op == 'mkdirbase':
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                elif op == 'rm':
                    delete_path(path)
                elif op in ('cp', 'mv'):
                    other_path = os.path.realpath(context.param_value(f'other path_{i}'))
                    os.makedirs(os.path.dirname(other_path), exist_ok=True)
                    if os.path.isfile(path):
                        shutil.copy2(path, other_path)
                    elif os.path.isdir(path):
                        shutil.copytree(path, other_path, dirs_exist_ok=True)
                    else:
                        raise ProcessingError(f'cannot determine is "{path}" a file or a dir')
                    if op == 'mv':
                        delete_path(path)
                else:
                    raise ProcessingError(f'unknown operation: "{op}"')
            return result

    def postprocess_task(self, context: ProcessingContext) -> ProcessingResult:
        return ProcessingResult()


def delete_path(path):
    if not os.path.exists(path):
        return
    if os.path.isfile(path):
        os.remove(path)
    elif os.path.isdir(path):
        shutil.rmtree(path)
    else:
        raise ProcessingError(f'cannot determine is "{path}" a file or a dir')
