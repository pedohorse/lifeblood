import re
import json
import shlex
from PySide2.QtWidgets import QDialog, QVBoxLayout, QHBoxLayout, QLineEdit, QLabel, QSpinBox, QPushButton
from PySide2.QtCore import Slot, QSize

from typing import TYPE_CHECKING, Optional, Tuple
if TYPE_CHECKING:
    from .graphics_items import Task


class LineEditWithDefaults(QLineEdit):
    def __init__(self, text='', parent=None):
        super(LineEditWithDefaults, self).__init__(text, parent)
        self.__default_text = text
        self.__default_state = True
        self.textChanged.connect(self._on_text_change)

    def _on_text_change(self, text):
        if not self.__default_state and self.__default_text == text:
            self.__default_state = True
            self.setStyleSheet("")
        elif self.__default_state and self.__default_text != text:
            self.__default_state = False
            self.setStyleSheet("LineEditWithDefaults { border-color: #FFDD22;}")

    def set_current_text_as_default(self):
        self.__default_text = self.text()
        self.__default_state = True
        self.setStyleSheet("")

    def is_at_default(self):
        assert self.__default_state == (self.__default_text == self.text())
        return self.__default_text == self.text()


class CreateTaskDialog(QDialog):
    def __init__(self, parent=None, task: Optional["Task"] = None):
        super(CreateTaskDialog, self).__init__(parent)

        self.__main_layout = QVBoxLayout(self)

        self.__name_edit = QLineEdit()
        self.__name_edit.setPlaceholderText('task name')
        self.__main_layout.addWidget(self.__name_edit)

        self.__groups_edit = QLineEdit()
        self.__groups_edit.setPlaceholderText('space separated task group list')
        self.__main_layout.addWidget(self.__groups_edit)

        attrcnt_layout = QHBoxLayout()
        attrcnt_layout.addWidget(QLabel('attribute count'))
        self.__attrcount_spinbox = QSpinBox()
        self.__attrcount_spinbox.setMinimum(0)
        attrcnt_layout.addWidget(self.__attrcount_spinbox)
        self.__main_layout.addLayout(attrcnt_layout)

        self.__attrs_layout = QVBoxLayout()
        self.__main_layout.addLayout(self.__attrs_layout)

        accept_layout = QHBoxLayout()
        accept_layout.addStretch()
        self.__ok_btn = QPushButton('Create')
        self.__cancel_btn = QPushButton('Cancel')
        accept_layout.addWidget(self.__ok_btn)
        accept_layout.addWidget(self.__cancel_btn)
        self.__main_layout.addLayout(accept_layout)

        # connec
        self.__ok_btn.clicked.connect(self.accept)
        self.__cancel_btn.clicked.connect(self.reject)

        self.__attrcount_spinbox.valueChanged.connect(self.attribute_count_changed)

        # init
        self.__initial_attrib_names = set()
        if task is not None:
            self.__name_edit.setText(task.name())
            self.__groups_edit.setText(shlex.join(task.groups()))
            attrs = task.attributes()
            self.__attrcount_spinbox.setValue(len(attrs))
            for i, (name, val) in enumerate(attrs.items()):
                attr_layout = self.__attrs_layout.itemAt(i).layout()
                attr_layout.itemAt(0).widget().setText(name)
                attr_layout.itemAt(1).widget().setText(json.dumps(val))
                attr_layout.itemAt(0).widget().set_current_text_as_default()
                attr_layout.itemAt(1).widget().set_current_text_as_default()
                self.__initial_attrib_names.add(name)

    def sizeHint(self) -> QSize:
        return QSize(384, 128)

    @Slot(int)
    def attribute_count_changed(self, val):
        old_attr_count = self.__attrs_layout.count()
        if val < old_attr_count:
            for i in range(old_attr_count-1, val-1, -1):
                lay = self.__attrs_layout.takeAt(i).layout()
                while lay.count() > 0:
                    lay.takeAt(0).widget().deleteLater()
                lay.deleteLater()
        elif val > old_attr_count:
            for _ in range(old_attr_count, val):
                attr_layout = QHBoxLayout()
                name = LineEditWithDefaults()
                name.setPlaceholderText('attribute name')
                val = LineEditWithDefaults()
                val.setPlaceholderText('json-style value')
                attr_layout.addWidget(name, 1)
                attr_layout.addWidget(val, 3)
                self.__attrs_layout.addLayout(attr_layout)

    def get_task_name(self):
        name = self.__name_edit.text().strip()
        if name == '':
            name = 'untitled'
        return name

    def get_task_groups(self):
        grps_raw = self.__groups_edit.text().strip()
        return shlex.split(grps_raw)

    def get_task_attributes(self) -> dict:
        attrs = {}
        for i in range(self.__attrs_layout.count()):
            attr_layout = self.__attrs_layout.itemAt(i).layout()
            name = attr_layout.itemAt(0).widget().text().strip()
            if name == '':
                continue
            val = attr_layout.itemAt(1).widget().text()

            try:
                val = json.loads(val)
            except json.JSONDecodeError:
                val = json.loads(f'"{val}"')

            attrs[name] = val

        return attrs

    def get_task_changed_attributes(self) -> Tuple[dict, set]:
        """
        get only the dict of attributes whose values were changed from defaults
        and get a list of attributes that were deleted

        :return:
        """
        attrs = {}
        all_attr_names = set()
        for i in range(self.__attrs_layout.count()):
            attr_layout = self.__attrs_layout.itemAt(i).layout()
            name = attr_layout.itemAt(0).widget().text().strip()
            if name == '':
                continue
            all_attr_names.add(name)
            val = attr_layout.itemAt(1).widget().text()
            if attr_layout.itemAt(0).widget().is_at_default() and attr_layout.itemAt(1).widget().is_at_default():
                continue

            try:
                val = json.loads(val)
            except json.JSONDecodeError:
                val = json.loads(f'"{val}"')

            attrs[name] = val

        return attrs, self.__initial_attrib_names - all_attr_names


def test():
    import sys
    from PySide2.QtWidgets import QApplication
    app = QApplication(sys.argv)

    wgt = CreateTaskDialog()

    def _show_stuff():
        print(f'name: {wgt.get_task_name()}')
        print(f'groups: {wgt.get_task_groups()}')
        print(f'attrs: {wgt.get_task_attributes()}')

    wgt.show()
    wgt.accepted.connect(_show_stuff)

    return app.exec_()


if __name__ == '__main__':
    test()
