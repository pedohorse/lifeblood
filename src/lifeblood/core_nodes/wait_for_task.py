from threading import Lock
import shlex
from lifeblood.basenode import BaseNode
from lifeblood.nodethings import ProcessingResult, ProcessingContext
from lifeblood.taskspawn import TaskSpawn
from lifeblood.exceptions import NodeNotReadyToProcess
from lifeblood.enums import NodeParameterType
from lifeblood.uidata import NodeUi, MultiGroupLayout, Parameter
from lifeblood.node_visualization_classes import NodeColorScheme

from typing import Iterable, Set, Dict


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

    def ready_to_process_task(self, task_dict) -> bool:
        # roughly estimate if we should try processing
        context = ProcessingContext(self, task_dict)
        task_id = context.task_id()

        expected_values = shlex.split(context.param_value('expected values'))
        condition_value = context.param_value('condition value').strip()

        if task_id in self.__values_map and self.__values_map[task_id] != condition_value:
            # this means task prob was rescheduled, cond value changed
            return True

        # empty string treated as no cond
        if condition_value != '' and condition_value not in self.__values_set_cache:
            return True
        return not expected_values or all(x in self.__values_set_cache for x in expected_values)

    @classmethod
    def __get_default_result(cls, context: ProcessingContext):
        res = ProcessingResult()
        res.set_node_output_name(context.task_field('node_input_name'))
        return res

    # TODO: this node's inner state grows infinitely, this is a design flaw
    #  need to figure out a way for it to controllably shrink
    #  as an extreme use case think of the asset dependency graph,
    #  that brought the need for this node in the first place

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        task_id = context.task_id()
        expected_values = shlex.split(context.param_value('expected values'))
        condition_value = context.param_value('condition value').strip()

        # pre-cover most trivial case to avoid locking
        if task_id not in self.__values_map and condition_value == '' and not expected_values:  # in case no conditions, no expectations
            return self.__get_default_result(context)

        with self.__main_lock:
            # we use value map to ensure each task contribute only one condition value,
            # in case task was resubmitted multiple times with changed parameters/attributes
            existing_value = self.__values_map.get(task_id)
            if existing_value != condition_value:
                self.__values_map[task_id] = condition_value
                if existing_value is None:  # was not set before
                    self.__values_set_cache.add(condition_value)
                else:  # else value was changed, task reprocessed, so we should recreate the whole value set cache (cannot just remove cuz maybe another task contributes same value)
                    self.__values_set_cache = set(self.__values_map.values())
            if all(x in self.__values_set_cache for x in expected_values):
                return self.__get_default_result(context)
        raise NodeNotReadyToProcess()

    def get_state(self) -> dict:
        return {
            'values_map': self.__values_map,
            'values_set_cache': list(self.__values_set_cache)
        }

    def set_state(self, state: dict):
        self.__values_map = {int(k): v for k,v in state['values_map'].items()}
        self.__values_set_cache = set(state['values_set_cache'])
