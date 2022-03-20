import re
from enum import Enum
from PySide2.QtWidgets import *
from PySide2.QtGui import QFont, QSyntaxHighlighter, QTextCharFormat, QColor, QKeyEvent, QTextCursor, QTextFormat, QTextDocument
from PySide2.QtCore import Slot, Signal, Qt, QSize, QEvent

from typing import List, Tuple, Pattern, Union, Callable


class StringParameterEditorSyntaxHighlighter(QSyntaxHighlighter):
    def name(self) -> str:
        """
        displayed name of this syntax highlighter
        :return:
        """
        return 'no name'


class PythonSyntaxHighlighter(StringParameterEditorSyntaxHighlighter):
    def __init__(self, doc):
        super(PythonSyntaxHighlighter, self).__init__(doc)

        kw_format = QTextCharFormat()
        kw_format.setForeground(QColor('#CC7832'))
        kw_format.setFontWeight(QFont.Bold)

        fu_format = QTextCharFormat()
        fu_format.setForeground(QColor('#FFC66D'))

        co_format = QTextCharFormat()
        co_format.setForeground(QColor('#808080'))
        co_format.setFontItalic(True)

        ss_format = QTextCharFormat()
        ss_format.setForeground(QColor('#6A8759'))

        self.__highlights: List[Tuple[Pattern[str], QTextCharFormat]] = []
        for keyword in ('False', 'None', 'True', 'and', 'as', 'assert', 'async', 'await', 'break', 'class', 'continue', 'def', 'del', 'elif', 'else', 'except', 'finally', 'for', 'from', 'global', 'if', 'import', 'in', 'is', 'lambda', 'nonlocal', 'not', 'or', 'pass', 'raise', 'return', 'try', 'while', 'with', 'yield'):
            self.__highlights.append((re.compile(rf'\b{keyword}\b'), kw_format))
        self.__highlights.append((re.compile(r'def\s+(\w+)'), fu_format))
        self.__highlights.append((re.compile(r'#.*'), co_format))
        for qmark in ('"', "'"):
            self.__highlights.append((re.compile(rf'{qmark}[^{qmark}]*{qmark}'), ss_format))

    def highlightBlock(self, text: str) -> None:
        for rule, format in self.__highlights:
            for m in rule.finditer(text):
                grp = len(m.groups())
                self.setFormat(m.start(grp), m.end(grp)-m.start(grp), format)

    def name(self) -> str:
        return 'Python'


class LogSyntaxHighlighter(StringParameterEditorSyntaxHighlighter):
    def __init__(self, doc):
        super(LogSyntaxHighlighter, self).__init__(doc)

        sys_format = QTextCharFormat()
        sys_format.setForeground(QColor('#52e385'))

        out_format = QTextCharFormat()
        out_format.setForeground(QColor('#FFFFFF'))

        err_format = QTextCharFormat()
        err_format.setForeground(QColor('#ff3b3b'))
        err_format.setFontWeight(QFont.Bold)

        self.__highlights: List[Tuple[Pattern[str], QTextCharFormat]] = []
        self.__highlights.append((re.compile(r'\[SYS\].*$'), sys_format))
        self.__highlights.append((re.compile(r'\[OUT\].*$'), out_format))
        self.__highlights.append((re.compile(r'\[ERR\].*$'), err_format))

    def highlightBlock(self, text: str) -> None:
        for rule, format in self.__highlights:
            for m in rule.finditer(text):
                grp = len(m.groups())
                self.setFormat(m.start(grp), m.end(grp)-m.start(grp), format)

    def name(self) -> str:
        return 'Log'


class QTextEditButTabsAreSpaces(QPlainTextEdit):
    def __init__(self, *args, **kwargs):
        super(QTextEditButTabsAreSpaces, self).__init__(*args, **kwargs)
        self.__ident_re = re.compile(r'\s*')
        self.__last_highligh_block_num = -1
        self.__space_in_tab_count = 4
        self.__tab = ' '*self.__space_in_tab_count

        # connec
        self.cursorPositionChanged.connect(self._highlight_current_line)

    def keyPressEvent(self, event):

        def _block_helper(what_to_do_for_each_block):
            old_cur = self.textCursor()
            cur = self.textCursor()
            cur.beginEditBlock()
            blocks = [cur.block()]
            sstart = old_cur.selectionStart()
            send = old_cur.selectionEnd()
            if sstart != send:
                curblock = blocks[0]
                while sstart < curblock.position():
                    curblock = curblock.previous()
                    assert curblock.isValid()
                    blocks.append(curblock)
                curblock = blocks[0]
                while send > curblock.position() + curblock.length():
                    curblock = curblock.next()
                    assert curblock.isValid()
                    blocks.append(curblock)
            for block in blocks:
                what_to_do_for_each_block(block, cur)
            cur.endEditBlock()
            self.setTextCursor(old_cur)

        def _remove_indent(block, cur):
            if block.text().startswith(self.__tab):
                cur.setPosition(block.position())
                for _ in range(self.__space_in_tab_count):
                    cur.deleteChar()

        def _insert_indent(block, cur):
            cur.setPosition(block.position())
            cur.insertText(self.__tab)

        if event.key() == Qt.Key_Tab:  # tab means spaces!
            cur = self.textCursor()
            if cur.selectionStart() == cur.selectionEnd():
                event = QKeyEvent(QEvent.KeyPress, Qt.Key_Space, Qt.KeyboardModifiers(event.nativeModifiers()), self.__tab)
                super().keyPressEvent(event)
            else:
                _block_helper(_insert_indent)
        elif event.key() == Qt.Key_Backtab:  # shift+tab
            _block_helper(_remove_indent)
        elif event.key() == Qt.Key_Return:
            # check indent of prev line
            cur = self.textCursor()
            m = self.__ident_re.match(cur.block().text())
            event = QKeyEvent(QEvent.KeyPress, Qt.Key_Return, Qt.KeyboardModifiers())
            super().keyPressEvent(event)
            cur.insertText(m.group(0))
        else:
            super().keyPressEvent(event)

    def _highlight_current_line(self):
        cur = self.textCursor()
        if cur.blockNumber() == self.__last_highligh_block_num:
            return
        self.__last_highligh_block_num = cur.blockNumber()
        linesel = QTextEdit.ExtraSelection()
        linesel.format.setBackground(QColor('#2B2B2F'))
        linesel.format.setProperty(QTextFormat.FullWidthSelection, True)
        linesel.cursor = QTextCursor(cur)
        linesel.cursor.clearSelection()
        self.setExtraSelections([linesel])


class StringParameterEditor(QWidget):
    edit_done = Signal(str)

    class SyntaxHighlight(Enum):
        NO_HIGHLIGHT = 0
        PYTHON = 1
        LOG = 2

    def __init__(self, syntax_highlight: Union[SyntaxHighlight, Callable[[QTextDocument], StringParameterEditorSyntaxHighlighter]] = SyntaxHighlight.NO_HIGHLIGHT, parent=None):
        super(StringParameterEditor, self).__init__(parent, Qt.Dialog)
        self.__main_layout = QVBoxLayout(self)
        self.__textarea = QTextEditButTabsAreSpaces()
        font = QFont('monospace')
        font.setFixedPitch(True)
        self.__textarea.setFont(font)
        self.__textarea.setLineWrapMode(QPlainTextEdit.NoWrap)

        self.__bottom_layout = QHBoxLayout()
        self.__ok_button = QPushButton('Apply&Close')
        self.__apply_button = QPushButton('Apply')
        self.__cancel_button = QPushButton('Cancel')
        self.__status_bar = QStatusBar()
        self.__status_bar.setSizeGripEnabled(False)
        self.__status_pos_label = QLabel('0:0')
        self.__status_pos_label.setFont(QFont('monospace'))
        self.__status_bar.addPermanentWidget(self.__status_pos_label)

        self.__bottom_layout.addStretch()
        self.__bottom_layout.addWidget(self.__ok_button)
        self.__bottom_layout.addWidget(self.__apply_button)
        self.__bottom_layout.addWidget(self.__cancel_button)

        self.__main_layout.addWidget(self.__textarea)
        self.__main_layout.addWidget(self.__status_bar)
        self.__main_layout.addLayout(self.__bottom_layout)

        self.__syntax_highlighter = None
        if isinstance(syntax_highlight, StringParameterEditor.SyntaxHighlight):
            if syntax_highlight == StringParameterEditor.SyntaxHighlight.PYTHON:
                self.__syntax_highlighter = PythonSyntaxHighlighter(self.__textarea.document())
            elif syntax_highlight == StringParameterEditor.SyntaxHighlight.LOG:
                self.__syntax_highlighter = LogSyntaxHighlighter(self.__textarea.document())
        else:
            assert callable(syntax_highlight)
            self.__syntax_highlighter = syntax_highlight(self.__textarea.document())

        if self.__syntax_highlighter is not None:
            self.__status_bar.insertPermanentWidget(0, QLabel(f'syntax: {self.__syntax_highlighter.name()}'))

        # connec
        self.__cancel_button.clicked.connect(self.close)
        self.__ok_button.clicked.connect(self._done_editing)
        self.__apply_button.clicked.connect(lambda: self.edit_done.emit(self.text()))
        self.__textarea.cursorPositionChanged.connect(self._cursor_position_changed)

    @Slot()
    def _done_editing(self):
        self.close()
        self.edit_done.emit(self.text())

    @Slot()
    def _cursor_position_changed(self):
        cur = self.__textarea.textCursor()
        self.__status_pos_label.setText(f'{cur.blockNumber()}:{cur.positionInBlock()}')

    def sizeHint(self):
        return QSize(640, 480)

    def text(self):
        return self.__textarea.toPlainText()

    def set_text(self, text: str):
        self.__textarea.setPlainText(text)

    def set_title(self, title: str):
        self.setWindowTitle(title)

    def set_readonly(self, readonly: bool):
        self.__textarea.setReadOnly(readonly)
        self.__ok_button.setVisible(not readonly)
        self.__apply_button.setVisible(not readonly)


def _test():
    import sys
    qapp = QApplication(sys.argv)

    wgt = StringParameterEditor(StringParameterEditor.SyntaxHighlight.PYTHON)
    wgt.set_text('line1\nline2\nline3 and more\nclass cat:\n    def meow(self):\n        print("meow")')
    wgt.edit_done.connect(lambda s: print(f'edit done, result: {repr(s)}'))
    wgt.show()
    return qapp.exec_()


if __name__ == '__main__':
    _test()
