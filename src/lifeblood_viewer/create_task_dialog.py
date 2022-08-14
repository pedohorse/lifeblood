import re
import json
import shlex
from PySide2.QtWidgets import QWidget, QDialog, QVBoxLayout, QHBoxLayout, QLineEdit, QLabel, QSpinBox, QPushButton
from PySide2.QtCore import Slot, QSize

from typing import TYPE_CHECKING, Optional, Tuple, List, Set
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


class AttributeEditorWidget(QWidget):
    def __init__(self, parent=None, init_attributes: Optional[dict] = None):
        super(AttributeEditorWidget, self).__init__(parent)
        self.__main_layout = QVBoxLayout(self)

        attrcnt_layout = QHBoxLayout()
        attrcnt_layout.addWidget(QLabel('attribute count'))
        self.__attrcount_spinbox = QSpinBox()
        self.__attrcount_spinbox.setMinimum(0)
        attrcnt_layout.addWidget(self.__attrcount_spinbox)
        self.__main_layout.addLayout(attrcnt_layout)

        self.__attrs_layout = QVBoxLayout()
        self.__main_layout.addLayout(self.__attrs_layout)

        self.__attrcount_spinbox.valueChanged.connect(self.attribute_count_changed)

        # init
        self.__initial_attrib_names = set()
        if init_attributes is not None:
            self.__attrcount_spinbox.setValue(len(init_attributes))
            for i, (name, val) in enumerate(init_attributes.items()):
                attr_layout = self.__attrs_layout.itemAt(i).layout()
                attr_layout.itemAt(0).widget().setText(name)
                attr_layout.itemAt(1).widget().setText(val if isinstance(val, str) else json.dumps(val))
                attr_layout.itemAt(0).widget().set_current_text_as_default()
                attr_layout.itemAt(1).widget().set_current_text_as_default()
                self.__initial_attrib_names.add(name)

    @Slot(int)
    def attribute_count_changed(self, val):
        old_attr_count = self.__attrs_layout.count()
        if val < old_attr_count:
            for i in range(old_attr_count - 1, val - 1, -1):
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

    def get_attributes(self) -> dict:
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

    def get_changed_attributes(self) -> Tuple[dict, set]:
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


class CreateTaskDialog(QDialog):
    def __init__(self, parent=None, task: Optional["Task"] = None):
        super(CreateTaskDialog, self).__init__(parent)

        self.__main_layout = QVBoxLayout(self)

        self.__name_edit = LineEditWithDefaults()
        self.__name_edit.setPlaceholderText('task name')
        self.__main_layout.addWidget(self.__name_edit)

        self.__groups_edit = LineEditWithDefaults()
        self.__groups_edit.setPlaceholderText('space separated task group list')
        self.__main_layout.addWidget(self.__groups_edit)

        self.__attribute_list = AttributeEditorWidget(init_attributes=task.attributes() if task else None)
        self.__main_layout.addWidget(self.__attribute_list)

        envreslayout = QHBoxLayout()
        envreslayout.addWidget(QLabel('environment resolver:'))
        self.__env_resolver_name_edit = LineEditWithDefaults()
        self.__env_resolver_name_edit.setText('StandardEnvironmentResolver')  # no set it as default. default is only if task has it
        envreslayout.addWidget(self.__env_resolver_name_edit)
        self.__main_layout.addLayout(envreslayout)

        self.__resolver_arguments = AttributeEditorWidget(init_attributes=task.environment_attributes().arguments() if task and task.environment_attributes() else None)
        self.__main_layout.addWidget(self.__resolver_arguments)

        accept_layout = QHBoxLayout()
        accept_layout.addStretch()
        self.__ok_btn = QPushButton('Accept')
        self.__cancel_btn = QPushButton('Cancel')
        accept_layout.addWidget(self.__ok_btn)
        accept_layout.addWidget(self.__cancel_btn)
        self.__main_layout.addLayout(accept_layout)

        # connec
        self.__ok_btn.clicked.connect(self.accept)
        self.__cancel_btn.clicked.connect(self.reject)

        # init
        if task is not None:
            self.__name_edit.setText(task.name())
            self.__name_edit.set_current_text_as_default()
            self.__groups_edit.setText(shlex.join(task.groups()))
            self.__groups_edit.set_current_text_as_default()
            env_attrs = task.environment_attributes()
            if env_attrs is not None:
                self.__env_resolver_name_edit.setText(env_attrs.name())
                self.__env_resolver_name_edit.set_current_text_as_default()
            else:
                self.__env_resolver_name_edit.setText('')  # treat empty name as None for resolver
                self.__env_resolver_name_edit.set_current_text_as_default()

    def sizeHint(self) -> QSize:
        return QSize(384, 128)

    def get_task_name(self):
        name = self.__name_edit.text().strip()
        if name == '':
            name = 'untitled'
        return name

    def get_task_groups(self):
        grps_raw = self.__groups_edit.text().strip()
        return shlex.split(grps_raw)

    def get_task_attributes(self) -> dict:
        return self.__attribute_list.get_attributes()

    def get_task_environment_resolver_and_arguments(self) -> Tuple[str, dict]:
        return self.__env_resolver_name_edit.text().strip(), self.__resolver_arguments.get_attributes()

    def get_task_changes(self) -> Tuple[Optional[str], Optional[List[str]], dict, set,
                                        Optional[str], dict, set]:
        """
        returns all changes made to the defaults
        calling this makes most sence if you specified non standard default state of the widget
        otherwise - everything is technically a change...
        :return: Tuple of:
                 * name if changed or None
                 * list of groups if changed or None
                 * dict of modified attributes
                 * set of deleted attrib names
                 * environment resolver name if changed or None
                 * dict of modified resolver arguments
                 * set of deleted resolver arguments
        """
        group_list: Optional[List[str]] = None if self.__groups_edit.is_at_default() else shlex.split(self.__groups_edit.text())
        modified_attributes, deleted_attributes = self.__attribute_list.get_changed_attributes()
        modified_resolver_arguments, deleted_resolver_arguments = self.__resolver_arguments.get_changed_attributes()
        return (None if self.__name_edit.is_at_default() else self.__name_edit.text(),
                group_list,
                modified_attributes, deleted_attributes,
                None if self.__env_resolver_name_edit.is_at_default() else self.__env_resolver_name_edit.text(),
                modified_resolver_arguments, deleted_resolver_arguments)


def test():
    import sys
    from PySide2.QtWidgets import QApplication
    app = QApplication(sys.argv)

    wgt = CreateTaskDialog()

    def _show_stuff():
        print(f'name: {wgt.get_task_name()}')
        print(f'groups: {wgt.get_task_groups()}')
        print(f'attrs: {wgt.get_task_attributes()}')
        print(f'resolver name and attrs: {wgt.get_task_environment_resolver_and_arguments()}')

    wgt.show()
    wgt.accepted.connect(_show_stuff)

    return app.exec_()


if __name__ == '__main__':
    test()
