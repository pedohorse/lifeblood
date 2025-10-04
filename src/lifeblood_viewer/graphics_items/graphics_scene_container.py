from lifeblood import logging
from .graphics_scene_base import GraphicsSceneBase
from .graphics_items import Node, Task, NodeConnection
from PySide6.QtCore import QPointF

from types import MappingProxyType
from typing import Dict, Tuple, Mapping, Optional, Sequence

logger = logging.get_logger('viewer')


class GraphicsSceneWithNodesAndTasks(GraphicsSceneBase):
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.__task_dict: Dict[int, Task] = {}
        self.__node_dict: Dict[int, Node] = {}
        self.__node_connections_dict: Dict[int, NodeConnection] = {}

        # settings:
        self.__node_snapping_enabled = True

    def session_node_id_to_id(self, session_id: int) -> Optional[int]:
        node_id = super().session_node_id_to_id(session_id)
        if node_id is not None and self.get_node(node_id) is None:
            self._remove_session_id(session_id)
            node_id = None
        return node_id

    def get_task(self, task_id) -> Optional[Task]:
        return self.__task_dict.get(task_id, None)

    def get_node(self, node_id) -> Optional[Node]:
        return self.__node_dict.get(node_id, None)

    def nodes(self) -> Tuple[Node, ...]:
        return tuple(self.__node_dict.values())

    def tasks(self) -> Tuple[Task, ...]:
        return tuple(self.__task_dict.values())

    def tasks_dict(self) -> Mapping[int, Task]:
        return MappingProxyType(self.__task_dict)

    def get_node_connection(self, con_id) -> Optional[NodeConnection]:
        return self.__node_connections_dict.get(con_id, None)

    def get_node_connection_from_ends(self, out_id, out_name, in_id, in_name) -> Optional[NodeConnection]:
        for con in self.__node_connections_dict.values():
            onode, oname = con.output()
            inode, iname = con.input()
            if (onode.get_id(), oname) == (out_id, out_name) \
                    and (inode.get_id(), iname) == (in_id, in_name):
                return con

    def addItem(self, item):
        logger.debug('adding item %s', item)
        super().addItem(item)
        assert item in self.items()
        if isinstance(item, Task):
            self.__task_dict[item.get_id()] = item
        elif isinstance(item, Node):
            self.__node_dict[item.get_id()] = item
        elif isinstance(item, NodeConnection):
            self.__node_connections_dict[item.get_id()] = item
        logger.debug('added item')

    def removeItem(self, item):
        logger.debug('removing item %s', item)
        if item.scene() != self:
            logger.debug('item was already removed, just removing ids from internal caches')
        else:
            super().removeItem(item)
        if isinstance(item, Task):
            assert item.get_id() in self.__task_dict, 'inconsistency in internal caches. maybe item was doubleremoved?'
            del self.__task_dict[item.get_id()]
        elif isinstance(item, Node):
            assert item.get_id() in self.__node_dict, 'inconsistency in internal caches. maybe item was doubleremoved?'
            del self.__node_dict[item.get_id()]
        elif isinstance(item, NodeConnection):
            assert item.get_id() in self.__node_connections_dict
            self.__node_connections_dict.pop(item.get_id())
        logger.debug('item removed')

    def clear(self):
        logger.debug('clearing the scene...')
        super().clear()
        self.__task_dict = {}
        self.__node_dict = {}
        logger.debug('scene cleared')

    #

    def move_nodes(self, nodes_datas: Sequence[Tuple[Node, QPointF]]):
        """
        move nodes
        """
        for node, pos in nodes_datas:
            node.setPos(pos)

    # settings  # TODO: move to a dedicated settings provider

    def node_snapping_enabled(self):
        return self.__node_snapping_enabled

    def set_node_snapping_enabled(self, enabled: bool):
        self.__node_snapping_enabled = enabled
