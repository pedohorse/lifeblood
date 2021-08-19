import re
import json
from PySide2.QtWidgets import QDialog, QVBoxLayout, QHBoxLayout, QLineEdit, QLabel, QSpinBox, QPushButton
from PySide2.QtCore import Slot


class CreateTaskDialog(QDialog):
    def __init__(self, parent=None):
        super(CreateTaskDialog, self).__init__(parent)

        self.__main_layout = QVBoxLayout(self)

        self.__name_edit = QLineEdit()
        self.__name_edit.setPlaceholderText('task name')
        self.__main_layout.addWidget(self.__name_edit)

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
                name = QLineEdit()
                name.setPlaceholderText('attribute name')
                val = QLineEdit()
                val.setPlaceholderText('json-style value')
                attr_layout.addWidget(name)
                attr_layout.addWidget(val)
                self.__attrs_layout.addLayout(attr_layout)

    def get_task_name(self):
        return self.__name_edit.text()

    def get_attributes(self):
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


def test():
    import sys
    from PySide2.QtWidgets import QApplication
    app = QApplication(sys.argv)

    wgt = CreateTaskDialog()

    def _show_stuff():
        print(f'name: {wgt.get_task_name()}')
        print(f'attrs: {wgt.get_attributes()}')

    wgt.show()
    wgt.accepted.connect(_show_stuff)

    return app.exec_()


if __name__ == '__main__':
    test()
