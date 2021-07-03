import re
from enum import Enum
from PySide2.QtWidgets import *
from PySide2.QtGui import QFont, QSyntaxHighlighter, QTextCharFormat, QColor
from PySide2.QtCore import Slot, Signal, Qt, QSize

from typing import List, Tuple, Pattern


class PythonSyntaxHighlighter(QSyntaxHighlighter):
    def __init__(self, doc):
        super(PythonSyntaxHighlighter, self).__init__(doc)

        kw_format = QTextCharFormat()
        kw_format.setForeground(QColor('#CC7832'))
        kw_format.setFontWeight(QFont.Bold)
        self.__highlights: List[Tuple[Pattern[str], QTextCharFormat]] = []
        for keyword in ('False', 'None', 'True', 'and', 'as', 'assert', 'async', 'await', 'break', 'class', 'continue', 'def', 'del', 'elif', 'else', 'except', 'finally', 'for', 'from', 'global', 'if', 'import', 'in', 'is', 'lambda', 'nonlocal', 'not', 'or', 'pass', 'raise', 'return', 'try', 'while', 'with', 'yield'):
            self.__highlights.append((re.compile(rf'\b{keyword}\b'), kw_format))

    def highlightBlock(self, text:str) -> None:
        for rule, format in self.__highlights:
            for m in rule.finditer(text):
                self.setFormat(m.start(), m.end()-m.start(), format)


class StringParameterEditor(QWidget):
    edit_done = Signal(str)

    class SyntaxHighlight(Enum):
        NO_HIGHLIGHT = 0
        PYTHON = 1

    def __init__(self, syntax_highlight: SyntaxHighlight = SyntaxHighlight.NO_HIGHLIGHT, parent=None):
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

        if syntax_highlight == StringParameterEditor.SyntaxHighlight.PYTHON:
            self.__syntax_highlighter = PythonSyntaxHighlighter(self.__textarea.document())

        # connec
        self.__cancel_button.clicked.connect(self.close)
        self.__ok_button.clicked.connect(self._done_editing)

    @Slot()
    def _done_editing(self):
        self.close()
        self.edit_done.emit(self.text())

    def sizeHint(self):
        return QSize(640, 480)

    def text(self):
        return self.__textarea.toPlainText()

    def set_text(self, text: str):
        self.__textarea.setPlainText(text)


def _test():
    import sys
    qapp = QApplication(sys.argv)

    wgt = StringParameterEditor(StringParameterEditor.SyntaxHighlight.PYTHON)
    wgt.set_text('line1\nline2\nline3 and more')
    wgt.edit_done.connect(lambda s: print(f'edit done, result: {s}'))
    wgt.show()
    return qapp.exec_()


if __name__ == '__main__':
    _test()
