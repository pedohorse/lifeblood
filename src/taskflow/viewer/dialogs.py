from PySide2.QtWidgets import QDialog, QHBoxLayout, QVBoxLayout, QTextEdit, QPushButton
from PySide2.QtCore import QSize


class MessageWithSelectableText(QDialog):
    def __init__(self, parent=None):
        super(MessageWithSelectableText, self).__init__(parent)

        self.__main_layout = QVBoxLayout(self)
        self.__btn_layout = QHBoxLayout()

        self.__btn_ok = QPushButton('Ok')
        self.__edit_field = QTextEdit()
        self.__edit_field.setReadOnly(True)

        self.__btn_layout.addStretch()
        self.__btn_layout.addWidget(self.__btn_ok)

        self.__main_layout.addWidget(self.__edit_field)
        self.__main_layout.addLayout(self.__btn_layout)

        # connect
        self.__btn_ok.pressed.connect(self.accept)

    def set_text(self, text: str):
        self.__edit_field.setText(text)

    def sizeHint(self):
        return QSize(800, 600)
