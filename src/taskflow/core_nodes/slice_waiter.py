import time
import json
from taskflow.basenode import BaseNode
from taskflow.nodethings import ProcessingResult
from taskflow.taskspawn import TaskSpawn
from taskflow.exceptions import NodeNotReadyToProcess
from taskflow.enums import NodeParameterType
from taskflow.uidata import NodeUi
from taskflow.processingcontext import ProcessingContext

from threading import Lock

from typing import Dict, TypedDict, Set, Iterable, Optional, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from taskflow.scheduler import Scheduler


class SliceAwaiting(TypedDict):
    arrived: Dict[int, dict]  # num in slice -2-> attributes
    awaiting: Set[int]
    first_to_arrive: Optional[int]


def node_class():
    return SplitAwaiterNode


class SplitAwaiterNode(BaseNode):

    @classmethod
    def label(cls) -> str:
        return 'slice gatherer'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'slice', 'wait', 'synchronization', 'barrier', 'gather', 'core'

    @classmethod
    def type_name(cls) -> str:
        return 'slice_waiter'

    def __init__(self, name: str):
        super(SplitAwaiterNode, self).__init__(name)
        self.__cache: Dict[int: SliceAwaiting] = {}
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

    def ready_to_process_task(self, task_dict) -> bool:
        context = ProcessingContext(self, task_dict)
        split_id = context.task_field('split_id')
        # we don't even need to lock
        return split_id not in self.__cache or \
               not context.param_value('wait for all') or \
               context.task_field('split_element') not in self.__cache[split_id]['arrived'] or \
               self.__cache[split_id]['arrived'].keys() == self.__cache[split_id]['awaiting']

    def process_task(self, context) -> ProcessingResult: #TODO: not finished, attrib not taken into account, rethink return type
        orig_id = context.task_field('split_origin_task_id')
        split_id = context.task_field('split_id')
        task_id = context.task_field('id')
        if orig_id is None:  # means no splits - just pass through
            return ProcessingResult()
        with self.__main_lock:
            if split_id not in self.__cache:
                self.__cache[split_id] = {'arrived': {},
                                          'awaiting': set(range(context.task_field('split_count'))),
                                          'first_to_arrive': None}
            if self.__cache[split_id]['first_to_arrive'] is None and len(self.__cache[split_id]['arrived']) == 0:
                self.__cache[split_id]['first_to_arrive'] = task_id
            if context.task_field('split_element') not in self.__cache[split_id]['arrived']:
                self.__cache[split_id]['arrived'][context.task_field('split_element')] = json.loads(context.task_field('attributes'))
                self.__cache[split_id]['arrived'][context.task_field('split_element')]['_builtin_id'] = task_id

        # we will not wait in loop or we risk deadlocking threadpool
        # check if everyone is ready
        if context.param_value('wait for all'):
            with self.__main_lock:
                if self.__cache[split_id]['arrived'].keys() == self.__cache[split_id]['awaiting']:
                    res = ProcessingResult()
                    res.remove_split()
                    if orig_id != task_id:
                        res.kill_task()
                    else:
                        # transfer attributes  # TODO: delete cache for already processed splits
                        num_attribs = context.param_value('transfer_attribs')
                        for i in range(num_attribs):
                            src_attr_name = context.param_value(f'src_attr_name_{i}')
                            transfer_type = context.param_value(f'transfer_type_{i}')
                            dst_attr_name = context.param_value(f'dst_attr_name_{i}')
                            sort_attr_name = context.param_value(f'sort_by_{i}')
                            sort_reversed = context.param_value(f'reversed_{i}')
                            if transfer_type == 'append':
                                gathered_values = []
                                for attribs in sorted(self.__cache[split_id]['arrived'].values(), key=lambda x: x.get(sort_attr_name, 0), reverse=sort_reversed):
                                    if src_attr_name not in attribs:
                                        continue

                                    attr_val = attribs[src_attr_name]
                                    gathered_values.append(attr_val)
                                res.set_attribute(dst_attr_name, gathered_values)
                            elif transfer_type == 'extend':
                                gathered_values = []
                                for attribs in sorted(self.__cache[split_id]['arrived'].values(), key=lambda x: x.get(sort_attr_name, 0), reverse=sort_reversed):
                                    if src_attr_name not in attribs:
                                        continue

                                    attr_val = attribs[src_attr_name]
                                    if isinstance(attr_val, list):
                                        gathered_values.extend(attr_val)
                                    else:
                                        gathered_values.append(attr_val)
                                res.set_attribute(dst_attr_name, gathered_values)
                            elif transfer_type == 'first':
                                _acd = self.__cache[split_id]['arrived']
                                if len(_acd) > 0:
                                    if sort_reversed:
                                        attribs = max(_acd.values(), key=lambda x: x.get(sort_attr_name, 0))
                                    else:
                                        attribs = min(_acd.values(), key=lambda x: x.get(sort_attr_name, 0))
                                    if src_attr_name in attribs:
                                        res.set_attribute(dst_attr_name, attribs[src_attr_name])
                            elif transfer_type == 'sum':
                                # we don't care about the order, assume sum is associative
                                gathered_values = None
                                for attribs in self.__cache[split_id]['arrived'].values():
                                    if src_attr_name not in attribs:
                                        continue
                                    if gathered_values is None:
                                        gathered_values = attribs[src_attr_name]
                                    else:
                                        gathered_values += attribs[src_attr_name]
                                res.set_attribute(dst_attr_name, gathered_values)
                            else:
                                raise NotImplementedError(f'transfer type "{transfer_type}" is not implemented')

                    return res
        else:
            with self.__main_lock:
                res = ProcessingResult()
                res.remove_split()
                if self.__cache[split_id]['first_to_arrive'] != task_id:
                    res.kill_task()
                return res

        raise NodeNotReadyToProcess()

    def postprocess_task(self, context) -> ProcessingResult:
        return ProcessingResult()

    def __getstate__(self):
        d = super(SplitAwaiterNode, self).__getstate__()
        assert '_SplitAwaiterNode__main_lock' in d
        del d['_SplitAwaiterNode__main_lock']
        return d
