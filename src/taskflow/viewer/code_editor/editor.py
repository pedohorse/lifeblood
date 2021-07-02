from PySide2.QtWidgets import *
from PySide2.QtGui import QFont


class StringParameterEditor(QWidget):
    def __init__(self, parent=None):
        super(StringParameterEditor, self).__init__(parent)
        self.__main_layout = QVBoxLayout(self)
        self.__textarea = QTextEdit()
        font = QFont('monospace')
        font.setFixedPitch(True)
        self.__textarea.setFont(font)

        self.__bottom_layout = QHBoxLayout()
        self.__ok_button = QPushButton('Apply')
        self.__cancel_button = QPushButton('Cancel')

        self.__bottom_layout.addStretch()
        self.__bottom_layout.addWidget(self.__ok_button)
        self.__bottom_layout.addWidget(self.__cancel_button)

        self.__main_layout.addWidget(self.__textarea)
        self.__main_layout.addLayout(self.__bottom_layout)


def _test():
    import sys
    qapp = QApplication(sys.argv)

    wgt = StringParameterEditor()
    wgt.show()
    return qapp.exec_()


if __name__ == '__main__':
    _test()
