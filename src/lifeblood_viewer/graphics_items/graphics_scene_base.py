from .network_item import NetworkItem

from PySide6.QtWidgets import QGraphicsScene, QWidget

from typing import Optional


class GraphicsSceneBase(QGraphicsScene):
    def __init__(self, parent: QWidget):
        super().__init__(parent=parent)
        self.__session_node_id_mapping = {}  # for consistent redo-undo involving node creation/deletion, as node_id will change on repetition
        self.__session_node_id_mapping_rev = {}
        self.__next_session_node_id = -1

    def get_inspected_item(self) -> Optional[NetworkItem]:
        """
        returns item that needs to be inspected.
        It's parameters should be displayed
        generally, it's the first selected item
        """
        sel = self.selectedItems()
        if len(sel) == 0:
            return None
        return sel[0]

    def session_node_id_to_id(self, session_id: int) -> Optional[int]:
        """
        the whole idea of session id is to have it consistent through undo-redos
        """
        node_id = self.__session_node_id_mapping.get(session_id)
        return node_id

    def _remove_session_id(self, session_id: int):
        node_id = self.__session_node_id_mapping.pop(session_id)
        self.__session_node_id_mapping_rev.pop(node_id)

    def __session_node_update_id(self, session_id: int, new_node_id: int):
        prev_node_id = self.__session_node_id_mapping.get(session_id)
        self.__session_node_id_mapping[session_id] = new_node_id
        if prev_node_id is not None:
            self.__session_node_id_mapping_rev.pop(prev_node_id)
        self.__session_node_id_mapping_rev[new_node_id] = session_id
        # TODO: self.__session_node_id_mapping should be cleared when undo stack is truncated, but so far it's a little "memory leak"

    def _session_node_update_session_id(self, new_session_id: int, node_id: int):
        if new_session_id in self.__session_node_id_mapping:
            raise RuntimeError(f'given session id {new_session_id} is already assigned')
        old_session_id = self.__session_node_id_mapping_rev.get(node_id)
        self.__session_node_id_mapping_rev[node_id] = new_session_id
        if old_session_id is not None:
            self.__session_node_id_mapping.pop(old_session_id)
        self.__session_node_id_mapping[new_session_id] = node_id

    def session_node_id_from_id(self, node_id: int):
        if node_id not in self.__session_node_id_mapping_rev:
            while self.session_node_id_to_id(self.__next_session_node_id) is not None:  # they may be taken by pasted nodes
                self.__next_session_node_id -= 1
            self.__session_node_update_id(self.__next_session_node_id, node_id)
            self.__next_session_node_id -= 1
        return self.__session_node_id_mapping_rev[node_id]
