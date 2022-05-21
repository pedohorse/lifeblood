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


from PySide2.QtWidgets import QWidget, QHBoxLayout, QVBoxLayout, QTabWidget, QTextEdit, QPushButton, QFileDialog
import string


class Editor(QWidget):
    def __init__(self, parent=None):
        super(Editor, self).__init__(parent)

        self.__layout = QVBoxLayout(self)

        self.__tabwidget = QTabWidget()
        self.__layout.addWidget(self.__tabwidget)

        self.__load_button = QPushButton('load file')
        self.__layout.addWidget(self.__load_button)

        self.__tabs: List[QWidget] = []

        self.__index: Optional[Index] = None

        # connec
        self.__load_button.clicked.connect(self.load_button_callback)

    def clear(self):
        self.__tabwidget.clear()
        for tab in self.__tabs:
            tab.deleteLater()
        self.__tabs.clear()
        self.__index = None

    def load_file(self, filepath):
        self.clear()
        with open(filepath, 'rb') as f:
            self.__index = Index.parse_index_root(f.read())

        for entry in self.__index.entries:
            tab = QTextEdit()
            print(repr(entry.rawdata))
            text = entry.rawdata.decode(encoding='ascii', errors='backslashreplace')
            text = ''.join(c if c in string.printable else rf'\x{ord(c):02}' for c in text)
            tab.setText(text)

            self.__tabs.append(tab)
            self.__tabwidget.addTab(tab, entry.name.decode('UTF-8'))

    def load_button_callback(self):
        filepath, _ = QFileDialog.getOpenFileName(self)
        print(filepath)
        if not filepath:
            return
        self.load_file(filepath)


if __name__ == '__main__':
    import sys
    from PySide2.QtWidgets import QApplication
    qapp = QApplication(sys.argv)

    wgt = Editor()
    wgt.show()

    sys.exit(qapp.exec_())
