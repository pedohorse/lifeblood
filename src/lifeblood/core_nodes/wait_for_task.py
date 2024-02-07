from threading import Lock
import shlex
from lifeblood.basenode import BaseNode
from lifeblood.nodethings import ProcessingResult, ProcessingContext
from lifeblood.taskspawn import TaskSpawn
from lifeblood.exceptions import NodeNotReadyToProcess
from lifeblood.enums import NodeParameterType
from lifeblood.uidata import NodeUi, MultiGroupLayout, Parameter
from lifeblood.node_visualization_classes import NodeColorScheme

from typing import Dict, Iterable, List, Optional, Set


def node_class():
    return WaitForTaskValue


class WaitForTaskValue(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'wait for task values'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'wait', 'barrier', 'pool', 'core'

    @classmethod
    def type_name(cls) -> str:
        return 'wait_for_task_value'

    @classmethod
    def description(cls) -> str:
        return 'the node keeps a GLOBAL pool of values\n' \
               'when a task is processed:\n' \
               '  - task\'s "condition value" is added to the GLOBAL value pool\n' \
               '  - task\'s "Expected Value(s)" are tested against the node\'s value pool\n' \
               '    - if ALL expected values are found in the pool - task is released\n' \
               '    - else - task will be waiting\n' \
               '\n' \
               'all inputs are the same, they are there for convenience, to separate multiple streams\n' \
               'tasks from input number N will exit through corresponding output number N\n' \
               '\n' \
               'NOTE: changing "Condition Value" will NOT take into account the tasks that were ALREADY processed,\n' \
               '      so be very careful changing this on a live graph\n' \
               '\n' \
               'NOTE: GLOBAL value pool means that all tasks from all task groups contribute to the pool\n' \
               '      so if you need to have pools per group (in case of non-intersecting groups for ex) - \n' \
               '      you will have to ensure values are unique per group, for example by prepending group name\n' \
               '      but it all depends on specific case'

    def __init__(self, name: str):
        super(WaitForTaskValue, self).__init__(name)
        self.__values_set_cache: Set[str] = set()
        self.__values_map: Dict[int, str] = {}  # we rely on dict being "python-atomic"
        self.__expect_map: Dict[int, List[str]] = {}

        # map of expected value to tasks to POKE (poke does NOT mean release)
        # this is a loose map - we can afford poking tasks that should not yet be unblocked
        self.__awaiters_map: Dict[str, Set[int]] = {}
        self.__main_lock = Lock()

        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_input('aux1')
            ui.add_input('aux2')
            ui.add_input('aux3')
            ui.add_output('aux1')
            ui.add_output('aux2')
            ui.add_output('aux3')
            ui.color_scheme().set_main_color(0.15, 0.24, 0.25)
            ui.add_parameter('condition value', 'Condition Value', NodeParameterType.STRING, '`task.id`')
            ui.add_parameter('expected values', 'Expected value(s)', NodeParameterType.STRING, '`task.get("some attrib", 0)`')

    def __get_default_result(self, context: ProcessingContext, *, tasks_to_unblock: Optional[List[int]] = None):
        task_id = context.task_id()
        expected_values = self.__get_expected_values(context)

        res = ProcessingResult()
        if tasks_to_unblock:
            res.tasks_to_unblock = tasks_to_unblock

        # clear awaiters_map as task is about to pass through
        for exp in expected_values:
            if task_id not in self.__awaiters_map.get(exp, ()):
                continue
            self.__awaiters_map[exp].remove(task_id)
            if len(self.__awaiters_map[exp]) == 0:
                self.__awaiters_map.pop(exp)

        res.set_node_output_name(context.task_field('node_input_name'))
        return res

    @classmethod
    def __get_expected_values(cls, context) -> List[str]:
        return shlex.split(context.param_value('expected values'))

    @classmethod
    def __get_condition_value(cls, context) -> str:
        return context.param_value('condition value').strip()

    # TODO: this node's inner state grows infinitely, this is a design flaw
    #  need to figure out a way for it to controllably shrink
    #  as an extreme use case think of the asset dependency graph,
    #  that brought the need for this node in the first place

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        task_id = context.task_id()
        expected_values = self.__get_expected_values(context)
        condition_value = self.__get_condition_value(context)

        # pre-cover most trivial case to avoid locking
        if task_id not in self.__values_map and condition_value == '' and not expected_values:  # in case no conditions, no expectations
            return self.__get_default_result(context)

        with self.__main_lock:
            # we use value map to ensure each task contribute only one condition value,
            # in case task was resubmitted multiple times with changed parameters/attributes
            existing_value = self.__values_map.get(task_id)
            existing_expec = self.__expect_map.get(task_id, [])
            if existing_value != condition_value:
                self.__values_map[task_id] = condition_value
                if existing_value is None:  # was not set before
                    self.__values_set_cache.add(condition_value)
                else:  # else value was changed, task reprocessed, so we should recreate the whole value set cache (cannot just remove cuz maybe another task contributes same value)
                    self.__values_set_cache = set(self.__values_map.values())

            # if expectations of a task changed - we need to update
            # awaiters_map that is used to unblock tasks
            if existing_expec != expected_values:
                for old_exp in existing_expec:
                    if task_id not in self.__awaiters_map.get(old_exp, ()):
                        continue
                    self.__awaiters_map[old_exp].remove(task_id)
                    if len(self.__awaiters_map[old_exp]) == 0:
                        self.__awaiters_map.pop(old_exp)

                for exp in expected_values:
                    self.__awaiters_map.setdefault(exp, set()).add(task_id)

            tasks_to_poke_unblock = list(self.__awaiters_map.get(condition_value, ()))

            if all(x in self.__values_set_cache for x in expected_values):
                return self.__get_default_result(context, tasks_to_unblock=tasks_to_poke_unblock)
        raise NodeNotReadyToProcess(tasks_to_unblock=tasks_to_poke_unblock)

    def get_state(self) -> dict:
        return {
            'values_map': self.__values_map,
            'values_set_cache': self.__values_set_cache,
            'expect_map': self.__expect_map,
            'awaiters_map': self.__awaiters_map
        }

    def set_state(self, state: dict):
        self.__values_map = state['values_map']
        self.__values_set_cache = state['values_set_cache']
        self.__expect_map = state.get('expect_map', {})
        self.__awaiters_map = state.get('awaiters_map', {})
