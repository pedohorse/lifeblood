import asyncio
from enum import Enum
import pickle
import json
from copy import copy
from typing import Dict, Optional, List, Any
from .nodethings import ProcessingResult
from .uidata import NodeUi
from .pluginloader import create_node

from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:
    from .scheduler import Scheduler


class BaseNode:
    @classmethod
    def label(cls) -> str:
        raise NotImplementedError()

    @classmethod
    def tags(cls) -> Iterable[str]:
        raise NotImplementedError()

    def __init__(self, name: str):
        self.__parent: Scheduler = None
        self.__parent_nid: int = None
        self._parameters: NodeUi = NodeUi(self)
        self.__name = name
        # subclass is expected to add parameters at this point

    def _set_parent(self, parent_scheduler, node_id):
        self.__parent = parent_scheduler
        self.__parent_nid = node_id

    def name(self):
        return self.__name

    def set_name(self, name: str):
        self.__name = name

    def param_value(self, param_name) -> Any:
        """
        shortcut to node.get_ui().parameter_value
        :param param_name:
        :return:
        """
        return self._parameters.parameter(param_name).value()

    def set_param_value(self, param_name, param_value) -> None:
        """
        shortcut to node.get_ui().set_parameter
        :param param_name:
        :return:
        """
        return self._parameters.parameter(param_name).set_value(param_value)

    def get_ui(self) -> NodeUi:
        return self._parameters

    def is_input_connected(self, input_name: str) -> bool:
        """
        returns wether or not specified input is connected to the node
        note that these methods are supposed to be called both from main thread AND from within executor pool thread
        so creating tasks becomes tricky.
        :param input_name:
        :return:
        """
        fut = asyncio.run_coroutine_threadsafe(self.__parent.get_node_input_connections(self.__parent_nid, input_name), self.__parent.get_event_loop())
        conns = fut.result(60)
        return len(conns) > 0

    def is_output_connected(self, output_name: str):
        """
        returns wether or not specified output is connected to the node
        :param output_name:
        :return:
        """
        fut = asyncio.run_coroutine_threadsafe(self.__parent.get_node_output_connections(self.__parent_nid, output_name), self.__parent.get_event_loop())
        conns = fut.result(60)
        return len(conns) > 0

    def _ui_changed(self):
        """
        this methods gets called by self and NodeUi when a parameter changes to trigger node's database update
        :return:
        """
        if self.__parent is not None:
            asyncio.get_event_loop().create_task(self.__parent.node_reports_ui_update(self.__parent_nid))

    def process_task(self, task_dict) -> ProcessingResult:
        raise NotImplementedError()

    def postprocess_task(self, task_dict) -> ProcessingResult:
        raise NotImplementedError()

    # some helpers
    def _get_task_attributes(self, task_row):
        return json.loads(task_row.get('attributes', '{}'))

    #
    # Serialize and back
    #
    def __reduce__(self):
        typename = type(self).__module__
        if '.' in typename:
            typename = typename.rsplit('.', 1)[-1]
        return create_node, (typename, '', None, None), self.__getstate__()

    def __getstate__(self):
        d = copy(self.__dict__)
        assert '_BaseNode__parent' in d
        d['_BaseNode__parent'] = None
        d['_BaseNode__parent_nid'] = None
        return d

    def serialize(self) -> bytes:
        """
        by default we just serialize
        :return:
        """
        return pickle.dumps(self)

    async def serialize_async(self) -> bytes:
        return await asyncio.get_event_loop().run_in_executor(None, self.serialize)

    @classmethod
    def deserialize(cls, data: bytes, parent_scheduler, node_id):
        newobj = pickle.loads(data)
        newobj.__parent = parent_scheduler
        newobj.__parent_nid = node_id
        return newobj

    @classmethod
    async def deserialize_async(cls, data: bytes, parent_scheduler, node_id):
        return await asyncio.get_event_loop().run_in_executor(None, cls.deserialize, data, parent_scheduler, node_id)
