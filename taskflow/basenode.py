import asyncio
from enum import Enum
import pickle
import json
from copy import copy
from typing import Dict, Optional, List, Any
from .nodethings import ProcessingResult
from .uidata import NodeUi
from .pluginloader import create_node

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .scheduler import Scheduler


class BaseNode:
    def __init__(self, name: str, parent_scheduler: "Scheduler"):
        self.__parent: Scheduler = parent_scheduler
        self._parameters: NodeUi = NodeUi(self)
        self.__name = name
        # subclass is expected to add parameters at this point

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
        return self._parameters.parameter_value(param_name)

    def set_param_value(self, param_name, param_value) -> None:
        """
        shortcut to node.get_ui().set_parameter
        :param param_name:
        :return:
        """
        return self._parameters.set_parameter(param_name, param_value)

    def get_ui(self) -> NodeUi:
        return self._parameters

    def _ui_changed(self, names_changed: Optional[List[str]] = None):
        """
        this methods gets called by self and NodeUi when a parameter changes to trigger node's database update
        :names_changed:
        :return:
        """
        if self.__parent is not None:
            asyncio.get_event_loop().create_task(self.__parent.node_reports_ui_update(self))

    def process_task(self, task_dict) -> ProcessingResult:
        raise NotImplementedError()

    def postprocess_task(self, task_dict) -> ProcessingResult:
        raise NotImplementedError()

    # some helpers
    def get_attributes(self, task_row):
        return json.loads(task_row.get('attributes', '{}'))

    #
    # Serialize and back
    #
    def __reduce__(self):
        typename = type(self).__module__
        if '.' in typename:
            typename = typename.rsplit('.', 1)[-1]
        return create_node, (typename, '', None), self.__getstate__()

    def __getstate__(self):
        d = copy(self.__dict__)
        assert '_BaseNode__parent' in d
        d['_BaseNode__parent'] = None
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
    def deserialize(cls, data: bytes, parent_scheduler):
        newobj = pickle.loads(data)
        newobj.__parent = parent_scheduler
        return newobj

    @classmethod
    async def deserialize_async(cls, data: bytes, parent_scheduler):
        return await asyncio.get_event_loop().run_in_executor(None, cls.deserialize, data, parent_scheduler)
