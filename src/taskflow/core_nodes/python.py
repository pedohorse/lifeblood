import time

from taskflow.basenode import BaseNode
from taskflow.invocationjob import InvocationJob, InvocationEnvironment
from taskflow.nodethings import ProcessingResult
from taskflow.uidata import NodeParameterType

from typing import TYPE_CHECKING, Iterable
if TYPE_CHECKING:
    from taskflow.scheduler import Scheduler


def node_class():
    return Python


process_help = "# use task variable to get/set attribs, e.g.     task['attr'] = 123\n" \
               "# use schedule() to decide if you want to create an invocation to be done by worker\n" \
               "# this code will be executed on scheduler, so don't make in heavy.\n" \
               "# leave heavy things for 'invoke' parameter below\n" \
               "\n"

invoke_help = "# use task variable to get attribs, e.g.     task['attr'] = 123\n" \
              "# this code will be executed on remote worker, all calls to task['smth'] will be resolved beforehand\n" \
              "\n"

class Python(BaseNode):
    def __init__(self, name):
        super(Python, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('process', 'process', NodeParameterType.STRING, process_help).set_text_multiline('python')
            ui.add_parameter('invoke', 'invoke', NodeParameterType.STRING, invoke_help).set_text_multiline('python')
            # ui.add_parameter('postprocess', 'postprocess', NodeParameterType.STRING, '').set_text_multiline('python')

    @classmethod
    def label(cls) -> str:
        return 'python script'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'script', 'python'

    def process_task(self, task_dict) -> ProcessingResult:
        class _TaskHelper:
            def __init__(self, attribs: dict, readonly: bool):
                self.__attribs = attribs
                self.__overrides = {}
                self.__todel = set()
                self.__readonly = readonly

            def __getitem__(self, item):
                if item in self.__todel:
                    raise KeyError(f'key {item} was deleted')
                return self.__overrides.get(item, self.__attribs[item])

            def __setitem__(self, key, value):
                if self.__readonly:
                    raise RuntimeError('cannot set attrib for readonly task')
                if key in self.__todel:
                    self.__todel.remove(key)
                self.__overrides[key] = value

            def __delitem__(self, key):
                if key in self.__todel:
                    raise KeyError(f'key {key} was already deleted')
                if key in self.__overrides:
                    del self.__overrides[key]
                self.__todel.add(key)

            def attribute_names(self):
                return set(self.__attribs.keys()) | set(self.__overrides.keys())

            def _overrides(self):
                return self.__overrides

            def _removes(self):
                return self.__todel

        def _set_sched():
            nonlocal do_schedule
            do_schedule = True

        task_helper = _TaskHelper(self._get_task_attributes(task_dict), readonly=False)

        exec_locals = {'task': task_helper, 'schedule': _set_sched}
        do_schedule = False
        exec(self.param_value('process'), {}, exec_locals)

        if do_schedule:
            inv = InvocationJob(['python', ':/main_invocation.py'])
            inok_task_attributes = {k: task_helper[k] for k in task_helper.attribute_names()}

            inv.set_extra_file('main_invocation.py', f'task = {repr(inok_task_attributes)}\n{self.param_value("invoke")}')
        else:
            inv = None
        res = ProcessingResult(inv)

        for k, v in task_helper._overrides().items():
            res.set_attribute(k, v)

        for k in task_helper._removes():
            res.remove_attribute(k)

        return res

    def postprocess_task(self, task_dict) -> ProcessingResult:
        return ProcessingResult()