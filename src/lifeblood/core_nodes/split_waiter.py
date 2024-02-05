from dataclasses import dataclass
import time
import json
from lifeblood.basenode import BaseNode
from lifeblood.nodethings import ProcessingResult
from lifeblood.taskspawn import TaskSpawn
from lifeblood.exceptions import NodeNotReadyToProcess
from lifeblood.enums import NodeParameterType
from lifeblood.uidata import NodeUi
from lifeblood.processingcontext import ProcessingContext

from threading import Lock

from typing import Dict, TypedDict, Set, Iterable, Optional, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from lifeblood.scheduler import Scheduler


@dataclass
class SplitAwaiting:
    arrived: Dict[int, dict]  # num in split -2-> attributes
    awaiting: Set[int]
    processed: Set[int]
    first_to_arrive: Optional[int]


def node_class():
    return SplitAwaiterNode


class SplitAwaiterNode(BaseNode):

    @classmethod
    def label(cls) -> str:
        return 'split gatherer'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'split', 'wait', 'synchronization', 'barrier', 'gather', 'core'

    @classmethod
    def type_name(cls) -> str:
        return 'split_waiter'

    def __init__(self, name: str):
        super(SplitAwaiterNode, self).__init__(name)
        self.__cache: Dict[int, SplitAwaiting] = {}
        self.__main_lock = Lock()
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('wait for all', 'wait for all', NodeParameterType.BOOL, True)
            with ui.multigroup_parameter_block('transfer_attribs'):
                with ui.parameters_on_same_line_block():
                    ui.add_parameter('src_attr_name', 'attribute', NodeParameterType.STRING, 'attr1')
                    ui.add_parameter('transfer_type', 'as', NodeParameterType.STRING, 'extend')\
                        .add_menu((('Extend', 'extend'),
                                   ('Append', 'append'),
                                   ('First', 'first'),
                                   ('Sum', 'sum')))
                    ui.add_parameter('dst_attr_name', 'sort by', NodeParameterType.STRING, 'attr1')
                    ui.add_parameter('sort_by', None, NodeParameterType.STRING, '_builtin_id')
                    ui.add_parameter('reversed', 'reversed', NodeParameterType.BOOL, False)

    def __get_promote_attribs(self, context):
        attribs_to_promote = {}
        split_id = context.task_field('split_id')
        num_attribs = context.param_value('transfer_attribs')
        for i in range(num_attribs):
            src_attr_name = context.param_value(f'src_attr_name_{i}')
            transfer_type = context.param_value(f'transfer_type_{i}')
            dst_attr_name = context.param_value(f'dst_attr_name_{i}')
            sort_attr_name = context.param_value(f'sort_by_{i}')
            sort_reversed = context.param_value(f'reversed_{i}')
            if transfer_type == 'append':
                gathered_values = []
                for attribs in sorted(self.__cache[split_id].arrived.values(), key=lambda x: x.get(sort_attr_name, 0), reverse=sort_reversed):
                    if src_attr_name not in attribs:
                        continue

                    attr_val = attribs[src_attr_name]
                    gathered_values.append(attr_val)
                attribs_to_promote[dst_attr_name] = gathered_values
            elif transfer_type == 'extend':
                gathered_values = []
                for attribs in sorted(self.__cache[split_id].arrived.values(), key=lambda x: x.get(sort_attr_name, 0), reverse=sort_reversed):
                    if src_attr_name not in attribs:
                        continue

                    attr_val = attribs[src_attr_name]
                    if isinstance(attr_val, list):
                        gathered_values.extend(attr_val)
                    else:
                        gathered_values.append(attr_val)
                attribs_to_promote[dst_attr_name] = gathered_values
            elif transfer_type == 'first':
                _acd = self.__cache[split_id].arrived
                if len(_acd) > 0:
                    if sort_reversed:
                        attribs = max(_acd.values(), key=lambda x: x.get(sort_attr_name, 0))
                    else:
                        attribs = min(_acd.values(), key=lambda x: x.get(sort_attr_name, 0))
                    if src_attr_name in attribs:
                        attribs_to_promote[dst_attr_name] = attribs[src_attr_name]
            elif transfer_type == 'sum':
                # we don't care about the order, assume sum is associative
                gathered_values = None
                for attribs in self.__cache[split_id].arrived.values():
                    if src_attr_name not in attribs:
                        continue
                    if gathered_values is None:
                        gathered_values = attribs[src_attr_name]
                    else:
                        gathered_values += attribs[src_attr_name]
                attribs_to_promote[dst_attr_name] = gathered_values
            else:
                raise NotImplementedError(f'transfer type "{transfer_type}" is not implemented')
        return attribs_to_promote

    def process_task(self, context) -> ProcessingResult: #TODO: not finished, attrib not taken into account, rethink return type
        orig_id = context.task_field('split_origin_task_id')
        split_id = context.task_field('split_id')
        task_id = context.task_field('id')
        if orig_id is None:  # means no splits - just pass through
            return ProcessingResult()
        with self.__main_lock:
            if split_id not in self.__cache:
                self.__cache[split_id] = SplitAwaiting(
                                          {},
                                          set(range(context.task_field('split_count'))),
                                          set(),
                                          None
                )
            if self.__cache[split_id].first_to_arrive is None and len(self.__cache[split_id].arrived) == 0:
                self.__cache[split_id].first_to_arrive = task_id
            if context.task_field('split_element') not in self.__cache[split_id].arrived:
                self.__cache[split_id].arrived[context.task_field('split_element')] = json.loads(context.task_field('attributes'))
                self.__cache[split_id].arrived[context.task_field('split_element')]['_builtin_id'] = task_id

        # we will not wait in loop or we risk deadlocking threadpool
        # check if everyone is ready
        changed = False
        try:
            if context.param_value('wait for all'):
                with self.__main_lock:
                    if self.__cache[split_id].arrived.keys() == self.__cache[split_id].awaiting:
                        res = ProcessingResult()
                        res.tasks_to_unblock = [
                            attrs['_builtin_id'] for _, attrs in self.__cache[split_id].arrived.items() if attrs['_builtin_id'] != task_id
                        ]
                        res.kill_task()
                        self.__cache[split_id].processed.add(context.task_field('split_element'))
                        if self.__cache[split_id].first_to_arrive == task_id:
                            # transfer attributes
                            attribs_to_promote = self.__get_promote_attribs(context)

                            res.remove_split(attributes_to_set=attribs_to_promote)
                        changed = True
                        return res
            else:
                with self.__main_lock:
                    res = ProcessingResult()
                    res.kill_task()
                    self.__cache[split_id].processed.add(context.task_field('split_element'))
                    if self.__cache[split_id].first_to_arrive == task_id:
                        res.remove_split()
                    changed = True
                    return res

        finally:
            if self.__cache[split_id].processed == self.__cache[split_id].awaiting:  # kinda precheck, to avoid extra lockings
                with self.__main_lock:
                    if self.__cache[split_id].processed == self.__cache[split_id].awaiting:  # and proper check inside lock
                        del self.__cache[split_id]
            # if changed:
            #     self._state_changed()  # this cannot be called from non asyncio thread as this.

        raise NodeNotReadyToProcess()

    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()

    def _debug_has_internal_data_for_split(self, split_id: int) -> bool:
        """
        method for debug/testing purposes.
        returns True if some internal data is present for a given split_id
        """
        return split_id in self.__cache

    def get_state(self) -> dict:
        return {
            'cache': {k: {
                'arrived': v.arrived,
                'awaiting': list(v.awaiting),
                'processed': list(v.processed),
                'first_to_arrive': v.first_to_arrive,
            } for k, v in self.__cache.items()},
        }

    def set_state(self, state: dict):
        self.__cache = {int(k): SplitAwaiting(
            {int(i): j for i, j in v['arrived'].items()},
            set(v['awaiting']),
            set(v['processed']),
            v['first_to_arrive']
        ) for k, v in state['cache'].items()}
