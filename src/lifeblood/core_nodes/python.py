import re
import time

from lifeblood.basenode import BaseNodeWithTaskRequirements
from lifeblood.invocationjob import InvocationJob, InvocationEnvironment
from lifeblood.processingcontext import ProcessingContext
from lifeblood.nodethings import ProcessingResult, ProcessingError
from lifeblood.uidata import NodeParameterType

from types import MappingProxyType
from typing import TYPE_CHECKING, Iterable
if TYPE_CHECKING:
    from lifeblood.scheduler import Scheduler


def node_class():
    return Python


process_help = "# use task variable to get/set attribs, e.g.     task['attr'] = 123\n" \
               "# use schedule() to decide if you want to create an invocation to be done by worker\n" \
               "# this code will be executed on scheduler, so don't make in heavy.\n" \
               "# leave heavy things for 'invoke' parameter below\n" \
               "\n" \
               "# schedule()  # uncomment this to launch code in invoke parameter below on a remote worker\n" \
               "\n"

invoke_help = "# use task variable to get attribs, e.g.     task['attr'] = 123\n" \
              "# this code will be executed on remote worker, all calls to task['smth'] will be resolved beforehand\n" \
              "\n"

description = ("This node executes python code.\n"
               "\n"
               "Code from 'process' is executed by scheduler, so it is NOT suggested to do anything heavy here  \n"
               "use `schedule()` command in `process` parameter to schedule `invoke` code  \n"
               "to be scheduled for execution on a worker.\n"
               "\n"
               "You can use parameter `good exit codes` to provide invocation exit codes that should be considered good.  \n"
               "By default it's 0, also empty value here defaults to 0 exit code\n"
               "You can also use `retry exit codes` to provide invocation exit codes that should trigger a retry.  \n"
               "Usually, an error is an error, and there is no reason to retry an invocation,  "
               "but some error might happen due to circumstances:\n"
               "- license return race conditions\n"
               "- out of recourses due to mis-estimations\n"
               "- mars is misaligned\n"
               "In such case you want to signal scheduler that invocation needs to be restarted.\n"
               "So if an invocation exits with exit code that matches this parameter's value -  \n"
               "it will be attempted again, till it reaches maximum invocation attempts.\n"
               "\n"
               "Read help comment in the parameter itself.\n")


def parse_range_pattern(pattern: str):
    # TODO: make better pattern expansion, add common pattern expansion methods
    all_numbers = set()
    for subpatt in pattern.strip().split(' '):
        if not subpatt:
            continue
        range_match = re.match(r'(-?\d+)-(-?\d+)', subpatt)
        if range_match:
            all_numbers.update(list(range(int(range_match.group(1)), int(range_match.group(2)) + 1)))
        else:
            all_numbers.add(int(subpatt))
    return all_numbers


class Python(BaseNodeWithTaskRequirements):
    def __init__(self, name: str):
        super(Python, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.4, 0.4, 0.2)
            with ui.collapsable_group_block('extra_param_group', 'extra parameters'):
                ui.add_parameter('good_codes_pattern', 'good exit codes', NodeParameterType.STRING, "0")
                ui.add_parameter('retry_codes_pattern', 'retry exit codes', NodeParameterType.STRING, "")
            ui.add_parameter('process', 'process', NodeParameterType.STRING, process_help).set_text_multiline('python')
            ui.add_parameter('invoke', 'invoke', NodeParameterType.STRING, invoke_help).set_text_multiline('python')
            # ui.add_parameter('postprocess', 'postprocess', NodeParameterType.STRING, '').set_text_multiline('python')

    @classmethod
    def label(cls) -> str:
        return 'python script'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'script', 'python', 'core'

    @classmethod
    def description(cls) -> str:
        return description

    @classmethod
    def type_name(cls) -> str:
        return 'python'

    def process_task(self, context) -> ProcessingResult:
        class _TaskHelper:
            def __init__(self, context: ProcessingContext, readonly: bool):
                self.__context = context
                self.__overrides = {}
                self.__todel = set()
                self.__readonly = readonly

            def __getitem__(self, item):
                if item in self.__todel:
                    raise KeyError(f'key {item} was deleted')
                if item in self.__overrides:
                    return self.__overrides[item]
                return self.__context.task_attribute(item)

            def __setitem__(self, key, value):
                if self.__readonly:
                    raise RuntimeError('cannot set attrib for readonly task')
                if key in self.__todel:
                    self.__todel.remove(key)
                self.__overrides[key] = value

            def get(self, item, default=None):
                try:
                    return self[item]
                except KeyError:
                    return default

            def __delitem__(self, key):
                if key in self.__todel:
                    raise KeyError(f'key {key} was already deleted')
                if key in self.__overrides:
                    del self.__overrides[key]
                self.__todel.add(key)

            def __getattr__(self, item):
                if not self.__context.task_has_field(item):
                    raise AttributeError(f'task has not field {item}')
                return self.__context.task_field(item)

            def attribute_names(self):
                return set(self.__context.task_attributes().keys()) | set(self.__overrides.keys())

            def _overrides(self):
                return self.__overrides

            def _removes(self):
                return self.__todel

        def _set_sched():
            nonlocal do_schedule
            do_schedule = True

        task_helper = _TaskHelper(context, readonly=False)

        exec_locals = {'task': task_helper, 'schedule': _set_sched}
        do_schedule = False
        exec(context.param_value('process'), exec_locals)

        if do_schedule:
            try:
                retry_exit_codes = parse_range_pattern(context.param_value('retry_codes_pattern'))
                ok_exit_codes = parse_range_pattern(context.param_value('good_codes_pattern'))
            except ValueError as e:
                raise ProcessingError(f'bad exit codes pattern! {str(e)}') from None

            inv = InvocationJob(['python', ':/main_invocation.py'], retry_exitcodes=retry_exit_codes, good_exitcodes=ok_exit_codes)
            inok_task_attributes = {k: task_helper[k] for k in task_helper.attribute_names()}
            inv.set_extra_file('main_invocation.py', f'task = {repr(inok_task_attributes)}\n{context.param_value("invoke")}')
        else:
            inv = None
        res = ProcessingResult(inv)

        for k, v in task_helper._overrides().items():
            res.set_attribute(k, v)

        for k in task_helper._removes():
            res.remove_attribute(k)

        return res

    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()
