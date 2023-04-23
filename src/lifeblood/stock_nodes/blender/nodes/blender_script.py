from lifeblood.basenode import BaseNodeWithTaskRequirements
from lifeblood.enums import NodeParameterType
from lifeblood.nodethings import ProcessingResult, ProcessingContext, ProcessingError
from lifeblood.invocationjob import InvocationJob, InvocationEnvironment
from lifeblood.text import filter_by_pattern

from typing import Iterable


def node_class():
    return BlenderScript


class BlenderScript(BaseNodeWithTaskRequirements):
    @classmethod
    def label(cls) -> str:
        return 'blender scene script'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'blender', 'script', 'python', 'stock'

    @classmethod
    def type_name(cls) -> str:
        return 'blender_scene_script'

    @classmethod
    def description(cls) -> str:
        return 'This node runs given python script on a given blender scene.\n'

    def __init__(self, name):
        super(BlenderScript, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.922, 0.467, 0.0)
            ui.color_scheme().set_secondary_color(0.129, 0.337, 0.533)

            ui.add_parameter('file_path', 'File Path', NodeParameterType.STRING, "`task['file']`")
            ui.add_parameter('script', None, NodeParameterType.STRING, '# task var is available here\n# get task attribute with task[\'attr_name\']\n\n')\
                .set_text_multiline('python')

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        file_path = context.param_value('file_path')

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

        scripts['script.py'] = script_prefix + context.param_value('script')
        args += ['-P', ':/script.py']

        invoc = InvocationJob(args)
        for file_name, file_contents in scripts.items():
            invoc.set_extra_file(file_name, file_contents)
        return ProcessingResult(invoc)
