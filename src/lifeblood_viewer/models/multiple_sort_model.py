from PySide2.QtCore import QAbstractItemModel, QObject, QSortFilterProxyModel

from typing import Any, Dict, Iterable, List, Optional


class MultipleFilterSortProxyModel(QSortFilterProxyModel):
    def __init__(self, source_model: QAbstractItemModel, indices_to_filter: Iterable[int], parent: QObject):
        indices_to_filter = list(indices_to_filter)
        if len(indices_to_filter) < 1:
            raise ValueError('indices_to_filter must not be empty')

        super().__init__(parent)
        self.__filter_models_order: List[QSortFilterProxyModel] = []
        self.__filter_models: Dict[int, QSortFilterProxyModel] = {}

        self.__source_model = source_model
        for i, idx in enumerate(indices_to_filter):
            if i == len(indices_to_filter) - 1:  # last element
                model = self
            else:
                model = QSortFilterProxyModel(self)
            model.setSourceModel(source_model)
            source_model = model
            model.setFilterKeyColumn(idx)
            model.setFilterWildcard('')
            self.__filter_models[idx] = model
            self.__filter_models_order.append(model)

        self.setDynamicSortFilter(True)  # careful with this if we choose to modify model through interface in future

    def set_filter_for_column(self, column_id: int, filter_wildcard: str):
        if column_id not in self.__filter_models:
            raise IndexError(f'column id {column_id} does not belong to this filter')
        self.__filter_models[column_id].setFilterWildcard(filter_wildcard)

    def setSortRole(self, role):
        assert self.__filter_models_order[-1] is self
        super().setSortRole(role)
        for model in self.__filter_models_order[:-1]:
            model.setSortRole(role)

    def setFilterRole(self, role):
        assert self.__filter_models_order[-1] is self
        super().setFilterRole(role)
        for model in self.__filter_models_order[:-1]:
            model.setFilterRole(role)

    def setDynamicSortFilter(self, enable):
        assert self.__filter_models_order[-1] is self
        super().setDynamicSortFilter(enable)
        for model in self.__filter_models_order[:-1]:
            model.setDynamicSortFilter(enable)
