import json
from taskflow.basenode import BaseNode
from taskflow.nodethings import ProcessingResult
from taskflow.taskspawn import TaskSpawn
from taskflow.exceptions import NodeNotReadyToProcess
from taskflow.enums import NodeParameterType
from taskflow.uidata import NodeUi

from threading import Lock

from typing import Dict, TypedDict, Set, Iterable, Optional, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from taskflow.scheduler import Scheduler


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
        return 'hierarchy', 'gather', 'wait', 'children', 'parent', 'core'

    def __init__(self, name: str):
        super(ParentChildrenWaiterNode, self).__init__(name)
        self.__cache_children: Dict[int, "ParentChildrenWaiterNode.Entry"] = {}
        self.__main_lock = Lock()
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_input('children')
            ui.add_output('children')
            ui.add_parameter('recursive', None, NodeParameterType.BOOL, False)
            with ui.multigroup_parameter_block('transfer_attribs'):
                with ui.parameters_on_same_line_block():
                    ui.add_parameter('src_attr_name', 'attribute', NodeParameterType.STRING, 'attr1')
                    ui.add_parameter('transfer_type', 'as', NodeParameterType.STRING, 'append').add_menu((('Append', 'append'),))
                    ui.add_parameter('dst_attr_name', 'sort by', NodeParameterType.STRING, 'attr1')
                    ui.add_parameter('sort_by', None, NodeParameterType.STRING, '_builtin_id')
                    ui.add_parameter('reversed', 'reversed', NodeParameterType.BOOL, False)

    def process_task(self, task_dict) -> ProcessingResult:
        task_id = task_dict['id']
        children_count = task_dict['children_count']
        recursive = self.param_value('recursive')

        with self.__main_lock:
            if task_dict['node_input_name'] == 'main':  # parent task
                if children_count == 0:
                    return ProcessingResult()
                if task_id not in self.__cache_children:
                    raise NodeNotReadyToProcess()

                if children_count == len(self.__cache_children[task_id].children):
                    result = ProcessingResult()
                    # transfer attributes
                    num_attribs = self.param_value('transfer_attribs')
                    for i in range(num_attribs):
                        src_attr_name = self.param_value(f'src_attr_name_{i}')
                        transfer_type = self.param_value(f'transfer_type_{i}')
                        dst_attr_name = self.param_value(f'dst_attr_name_{i}')
                        sort_attr_name = self.param_value(f'sort_by_{i}')
                        sort_reversed = self.param_value(f'reversed_{i}')
                        gathered_values = []
                        if transfer_type == 'append':
                            for attribs in sorted(self.__cache_children[task_id].all_children_dicts.values(), key=lambda x: x.get(sort_attr_name, 0), reverse=sort_reversed):
                                if src_attr_name not in attribs:
                                    continue

                                attr_val = attribs[src_attr_name]
                                gathered_values.append(attr_val)
                            result.set_attribute(dst_attr_name, gathered_values)
                        else:
                            raise NotImplementedError(f'transfer type "{transfer_type}" is not implemented')
                    # release children
                    self.__cache_children[task_id].parent_ready = True
                    return result

                raise NodeNotReadyToProcess()
            else:  # child task
                parent_id = task_dict['parent_id']
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
                    self.__cache_children[parent_id].all_children_dicts[task_id] = json.loads(task_dict['attributes'])
                    self.__cache_children[parent_id].all_children_dicts[task_id]['_builtin_id'] = task_id
                    if children_attrs_to_pass_up is not None:
                        self.__cache_children[parent_id].all_children_dicts.update(children_attrs_to_pass_up)
                raise NodeNotReadyToProcess()

        raise NodeNotReadyToProcess()

    def postprocess_task(self, task_dict) -> ProcessingResult:
        res = ProcessingResult()
        res.set_node_output_name(task_dict.get('node_output_name', 'main'))
        return res

    def __getstate__(self):
        d = super(ParentChildrenWaiterNode, self).__getstate__()
        assert '_ParentChildrenWaiterNode__main_lock' in d
        del d['_ParentChildrenWaiterNode__main_lock']
        del d['_ParentChildrenWaiterNode__cache_children']
        return d
