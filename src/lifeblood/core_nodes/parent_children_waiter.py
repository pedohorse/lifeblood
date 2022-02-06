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


def node_class():
    return ParentChildrenWaiterNode


class ParentChildrenWaiterNode(BaseNode):
    """
    this node will gather tasks from first and second inputs
    when a task from first input has all it's children arriving from the second input
    possibly recursively
    """
    class Entry:
        def __init__(self):
            self.children: Set[int] = set()
            self.parent_ready: bool = False
            self.all_children_dicts: Dict[int, dict] = {}

    @classmethod
    def label(cls) -> str:
        return 'parent-children waiter'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'hierarchy', 'gather', 'wait', 'synchronization', 'barrier', 'child', 'children', 'parent', 'core'

    @classmethod
    def type_name(cls) -> str:
        return 'parent_children_waiter'

    def __init__(self, name: str):
        super(ParentChildrenWaiterNode, self).__init__(name)
        self.__cache_children: Dict[int, "ParentChildrenWaiterNode.Entry"] = {}
        self.__main_lock = Lock()
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_input('children')
            ui.add_output('children')
            ui.add_parameter('recursive', 'recursive', NodeParameterType.BOOL, False)
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
        task_id = context.task_field('id')
        children_count = context.task_field('active_children_count')
        if context.task_field('node_input_name') == 'main':
            return children_count == 0 or \
                   task_id in self.__cache_children and children_count == len(self.__cache_children[task_id].children)

        ready: bool = True
        parent_id = context.task_field('parent_id')
        if context.param_value('recursive') and children_count > 0:
            ready = task_id in self.__cache_children and children_count == len(self.__cache_children[task_id].children)
        ready = ready and (
                parent_id not in self.__cache_children or
                task_id not in self.__cache_children[parent_id].children or
                self.__cache_children[parent_id].parent_ready
                )
        return ready

    def process_task(self, context) -> ProcessingResult:
        task_id = context.task_field('id')
        children_count = context.task_field('active_children_count')
        recursive = context.param_value('recursive')

        with self.__main_lock:
            if context.task_field('node_input_name') == 'main':  # parent task
                if children_count == 0:
                    return ProcessingResult()
                if task_id not in self.__cache_children:
                    raise NodeNotReadyToProcess()

                if children_count == len(self.__cache_children[task_id].children):
                    result = ProcessingResult()
                    # transfer attributes
                    num_attribs = context.param_value('transfer_attribs')
                    for i in range(num_attribs):
                        src_attr_name = context.param_value(f'src_attr_name_{i}')
                        transfer_type = context.param_value(f'transfer_type_{i}')
                        dst_attr_name = context.param_value(f'dst_attr_name_{i}')
                        sort_attr_name = context.param_value(f'sort_by_{i}')
                        sort_reversed = context.param_value(f'reversed_{i}')
                        if transfer_type == 'append':
                            gathered_values = []
                            for attribs in sorted(self.__cache_children[task_id].all_children_dicts.values(), key=lambda x: x.get(sort_attr_name, 0), reverse=sort_reversed):
                                if src_attr_name not in attribs:
                                    continue

                                attr_val = attribs[src_attr_name]
                                gathered_values.append(attr_val)
                            result.set_attribute(dst_attr_name, gathered_values)
                        elif transfer_type == 'extend':
                            gathered_values = []
                            for attribs in sorted(self.__cache_children[task_id].all_children_dicts.values(), key=lambda x: x.get(sort_attr_name, 0), reverse=sort_reversed):
                                if src_attr_name not in attribs:
                                    continue

                                attr_val = attribs[src_attr_name]
                                if isinstance(attr_val, list):
                                    gathered_values.extend(attr_val)
                                else:
                                    gathered_values.append(attr_val)
                            result.set_attribute(dst_attr_name, gathered_values)
                        elif transfer_type == 'first':
                            _acd = self.__cache_children[task_id].all_children_dicts
                            if len(_acd) > 0:
                                if sort_reversed:
                                    attribs = max(_acd.values(), key=lambda x: x.get(sort_attr_name, 0))
                                else:
                                    attribs = min(_acd.values(), key=lambda x: x.get(sort_attr_name, 0))
                                if src_attr_name in attribs:
                                    result.set_attribute(dst_attr_name, attribs[src_attr_name])
                        elif transfer_type == 'sum':
                            # we don't care about the order, assume sum is associative
                            gathered_values = None
                            for attribs in self.__cache_children[task_id].all_children_dicts.values():
                                if src_attr_name not in attribs:
                                    continue
                                if gathered_values is None:
                                    gathered_values = attribs[src_attr_name]
                                else:
                                    gathered_values += attribs[src_attr_name]
                            result.set_attribute(dst_attr_name, gathered_values)
                        else:
                            raise NotImplementedError(f'transfer type "{transfer_type}" is not implemented')
                    # release children
                    self.__cache_children[task_id].parent_ready = True
                    return result

                raise NodeNotReadyToProcess()
            else:  # child task
                parent_id = context.task_field('parent_id')
                if parent_id is None:
                    return ProcessingResult(node_output_name='children')
                if recursive and children_count > 0:  # if recursive - we first wait for our children, then add ourselves to our parent children list
                    if task_id not in self.__cache_children:
                        raise NodeNotReadyToProcess()
                    if children_count != len(self.__cache_children[task_id].children):
                        raise NodeNotReadyToProcess()

                if parent_id not in self.__cache_children:
                    self.__cache_children[parent_id] = ParentChildrenWaiterNode.Entry()

                children_attrs_to_pass_up = None
                if self.__cache_children[parent_id].parent_ready:
                    # in recursive case - mark as ready to release children
                    if recursive and task_id in self.__cache_children:  # if we are parent to smth
                        self.__cache_children[task_id].parent_ready = True
                        children_attrs_to_pass_up = self.__cache_children[task_id].all_children_dicts
                    # cleanup
                    self.__cache_children[parent_id].children.remove(task_id)
                    if len(self.__cache_children[parent_id].children) == 0:
                        del self.__cache_children[parent_id]
                    return ProcessingResult(node_output_name='children')

                if task_id not in self.__cache_children[parent_id].children:
                    self.__cache_children[parent_id].children.add(task_id)
                    self.__cache_children[parent_id].all_children_dicts[task_id] = json.loads(context.task_field('attributes'))
                    self.__cache_children[parent_id].all_children_dicts[task_id]['_builtin_id'] = task_id
                    if children_attrs_to_pass_up is not None:
                        self.__cache_children[parent_id].all_children_dicts.update(children_attrs_to_pass_up)
                raise NodeNotReadyToProcess()

        raise NodeNotReadyToProcess()

    def postprocess_task(self, context) -> ProcessingResult:
        res = ProcessingResult()
        res.set_node_output_name(context.task_field('node_output_name', 'main'))
        return res

    def __getstate__(self):
        d = super(ParentChildrenWaiterNode, self).__getstate__()
        assert '_ParentChildrenWaiterNode__main_lock' in d
        del d['_ParentChildrenWaiterNode__main_lock']
        return d
