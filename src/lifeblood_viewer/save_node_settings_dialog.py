from PySide2.QtWidgets import QDialog, QVBoxLayout, QCheckBox, QHBoxLayout, QPushButton, QLineEdit
from PySide2.QtCore import Slot, QSize

from typing import Iterable


class SaveNodeSettingsDialog(QDialog):
    def __init__(self, names: Iterable[str], parent=None):
        super(SaveNodeSettingsDialog, self).__init__(parent)
        self.__names = {name: True for name in names}

        self.__main_layout = QVBoxLayout(self)

        self.__settings_name = QLineEdit()
        self.__main_layout.addWidget(self.__settings_name)
        self.__settings_name.setPlaceholderText('choose a name for node settings')

        self.__name_widgets = {}
        for name, check in self.__names.items():
            wgt = QCheckBox(name)
            wgt.setChecked(check)
            self.__main_layout.addWidget(wgt)
            self.__name_widgets[name] = wgt

        accept_layout = QHBoxLayout()
        accept_layout.addStretch()
        self.__ok_btn = QPushButton('Accept')
        self.__cancel_btn = QPushButton('Cancel')
        accept_layout.addWidget(self.__ok_btn)
        accept_layout.addWidget(self.__cancel_btn)
        self.__ok_btn.setEnabled(False)

        self.__main_layout.addLayout(accept_layout)

        # connec
        self.__ok_btn.clicked.connect(self.accept)
        self.__cancel_btn.clicked.connect(self.reject)
        self.__settings_name.textChanged.connect(self._name_changed)

    def selected_names(self):
        return tuple(name for name, wgt in self.__name_widgets.items() if wgt.isChecked())

    def settings_name(self):
        return self.__settings_name.text().strip()

    def _name_changed(self):
        self.__ok_btn.setEnabled(len(self.settings_name()) > 0)


def test():
    import sys
    from PySide2.QtWidgets import QApplication
    app = QApplication(sys.argv)

    wgt = SaveNodeSettingsDialog(('qwe', 'asd', 'zxc', 'ass', 'boobs'))

    def _show_stuff():
        print(f'names: {wgt.selected_names()}')

    wgt.show()
    wgt.accepted.connect(_show_stuff)

    return app.exec_()


if __name__ == '__main__':
    test()
