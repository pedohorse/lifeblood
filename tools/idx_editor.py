import struct

from typing import List, Optional


class Entry:
    def __init__(self):
        self.name: bytes = b''
        self.ctime: int = 0
        self.rawdata: bytes = b''


class Index:
    """
    this is simplified intex parsing, works only on simplest non-nesting indices
    enough to do the job in case of idx files
    """

    def __init__(self):
        self.__w0 = b''
        self.__w1 = b''
        self.entries: List[Entry] = []

    def serialize(self):
        parts = [b'INDX',
                 struct.pack('>I', len(self.__w0)),
                 self.__w0,
                 struct.pack('>I', len(self.__w1)),
                 self.__w1,
                 struct.pack('>I', len(self.entries))
                 ]
        offset = 0
        for entry in self.entries:
            entrylen = len(entry.rawdata)
            parts.extend([struct.pack('>I', len(entry.name)),
                          entry.name,
                          struct.pack('>III', offset,
                                              entrylen,
                                              entry.ctime)])
            offset += entrylen

        for entry in self.entries:
            parts.append(entry.rawdata)

        return b''.join(parts)

    @classmethod
    def parse_index_root(cls, data: bytes):
        magic = data[:4]
        assert magic == b'INDX'

        # As it seem, index structure us 8b=0, 8b=index_name_len, <index_name>, index_file_count
        # But my spider-senses hint me first 8b might also be some len and name
        dataoffset = 4
        w0size = struct.unpack('>I', data[dataoffset: dataoffset + 4])[0]
        w0 = data[dataoffset + 4: dataoffset + 4 + w0size]
        dataoffset += 4 + w0size
        w1size = struct.unpack('>I', data[dataoffset: dataoffset + 4])[0]
        w1 = data[dataoffset + 4: dataoffset + 4 + w1size]
        dataoffset += 4 + w1size
        entrycount = struct.unpack('>I', data[dataoffset: dataoffset + 4])[0]
        dataoffset += 4

        entries = []
        index = Index()
        index.__w0 = w0
        index.__w1 = w1
        index.entries = entries

        offsets: List[int] = []
        lens: List[int] = []
        for entrynum in range(entrycount):
            entry_name_size = struct.unpack('>I', data[dataoffset:dataoffset + 4])[0]
            dataoffset += 4
            entry = Entry()
            entry.name = data[dataoffset: dataoffset + entry_name_size]
            dataoffset += entry_name_size
            eoffset, elen, entry.ctime = struct.unpack('>III', data[dataoffset: dataoffset + 12])
            offsets.append(eoffset)
            lens.append(elen)
            dataoffset += 12
            entries.append(entry)

        # time to read data
        endoffset = dataoffset
        for i, entry in enumerate(entries):
            entry.rawdata = data[dataoffset + offsets[i]: dataoffset + offsets[i] + lens[i]]
            endoffset += lens[i]

        if endoffset != len(data):
            print('!!!WARNING!!! something present after index end offset!')
        assert index.serialize() == data
        return index


def bytes_to_text(data: bytes) -> str:
    partlist = []
    for c in data:
        sc = chr(c)
        if c <= 127 and sc in string.printable:
            if sc == '\\':  # escape all \
                partlist.append(r'\\')
            else:
                partlist.append(sc)
        else:
            partlist.append(rf'\x{c:02}')

    return ''.join(partlist)


def text_to_bytes(text: str) -> bytes:
    partlist = []
    escaping = False
    hexcode = None
    for c in text:
        if hexcode is not None:
            if hexcode < 0:  # first symbol
                hexcode = int(c, base=16) * 16
            else:  # second symbol
                hexcode += int(c, base=16)
                partlist.append(hexcode.to_bytes(1, 'big'))
                hexcode = None
        elif escaping:
            escaping = False
            if c == '\\':
                partlist.append(c.encode('ascii'))
            elif c == 'x':
                hexcode = -1
            else:
                raise RuntimeError('should not happen!')
        else:
            if c == '\\':
                escaping = True
            else:
                partlist.append(c.encode('ascii'))
    return b''.join(partlist)


from PySide2.QtWidgets import QWidget, QHBoxLayout, QVBoxLayout, QTabWidget, QTextEdit, QPushButton, QFileDialog,\
                              QInputDialog, QMessageBox
from PySide2.QtGui import QFont
import string


class Editor(QWidget):
    def __init__(self, parent=None):
        super(Editor, self).__init__(parent)

        self.__layout = QVBoxLayout(self)

        self.__tabwidget = QTabWidget()
        tabcontrol_layout = QHBoxLayout()
        self.__newtab_button = QPushButton('add')
        self.__deltab_button = QPushButton('delete')
        tabcontrol_layout.addStretch(1)
        tabcontrol_layout.addWidget(self.__newtab_button)
        tabcontrol_layout.addWidget(self.__deltab_button)
        self.__layout.addLayout(tabcontrol_layout)
        self.__layout.addWidget(self.__tabwidget)

        self.__load_button = QPushButton('load file')
        self.__save_button = QPushButton('save to file')
        btn_layout = QHBoxLayout()
        btn_layout.addWidget(self.__load_button)
        btn_layout.addWidget(self.__save_button)
        self.__layout.addLayout(btn_layout)

        self.__tabs: List[QWidget] = []

        self.__index: Index = Index()

        # connec
        self.__load_button.clicked.connect(self._load_button_callback)
        self.__save_button.clicked.connect(self._save_button_callback)
        self.__newtab_button.clicked.connect(self._newtab_callback)
        self.__deltab_button.clicked.connect(self._deltab_callback)

    def clear(self):
        self.__tabwidget.clear()
        for tab in self.__tabs:
            tab.deleteLater()
        self.__tabs.clear()
        self.__index = Index()

    def _new_tab_widget(self) -> QTextEdit:
        tab = QTextEdit()
        font = QFont('monospace')
        font.setFixedPitch(True)
        tab.setFont(font)
        return tab

    def load_file(self, filepath):
        self.clear()
        with open(filepath, 'rb') as f:
            self.__index = Index.parse_index_root(f.read())

        for entry in self.__index.entries:
            tab = self._new_tab_widget()

            text = bytes_to_text(entry.rawdata)
            assert entry.rawdata == text_to_bytes(text)
            tab.setText(text)

            self.__tabs.append(tab)
            self.__tabwidget.addTab(tab, entry.name.decode('ascii'))

    def save_file(self, filepath):
        for entry, tab in zip(self.__index.entries, self.__tabs):
            entry.rawdata = text_to_bytes(tab.toPlainText())
        with open(filepath, 'wb') as f:
            f.write(self.__index.serialize())

    def _load_button_callback(self):
        filepath, _ = QFileDialog.getOpenFileName(self)
        if not filepath:
            return
        self.load_file(filepath)

    def _save_button_callback(self):
        filepath, _ = QFileDialog.getSaveFileName(self)
        if not filepath:
            return
        self.save_file(filepath)

    def _newtab_callback(self):
        name, good =QInputDialog.getText(self, 'entry name', 'name of new preset')
        if not good or not name:
            return
        try:
            name = name.encode('ascii')
        except UnicodeEncodeError:
            QMessageBox.warning(self, 'bad name', 'name should be ascii')
            return
        entry = Entry()
        entry.name = name
        entry.ctime = 0
        tab = self._new_tab_widget()
        self.__tabs.append(tab)
        self.__tabwidget.addTab(tab, entry.name.decode('ascii'))
        self.__index.entries.append(entry)

    def _deltab_callback(self):
        if QMessageBox.warning(self, 'confirm', 'delete current tab?', QMessageBox.Ok | QMessageBox.Cancel) != QMessageBox.Ok:
            return
        wgt = self.__tabwidget.currentWidget()
        if wgt is None:
            return
        idx = self.__tabwidget.currentIndex()
        assert self.__tabs[idx] == wgt

        self.__tabwidget.removeTab(idx)
        self.__tabs.pop(idx)
        self.__index.entries.pop(idx)


if __name__ == '__main__':
    import sys
    from PySide2.QtWidgets import QApplication
    qapp = QApplication(sys.argv)

    wgt = Editor()
    wgt.resize(666, 666)
    wgt.show()

    sys.exit(qapp.exec_())
