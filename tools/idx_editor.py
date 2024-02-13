import struct

from typing import List, Optional, Tuple


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


class CpioSection:
    def __init__(self, headerstart: bytes, timestamp: int, name: bytes, data: bytes):
        self.__headerstart = headerstart
        self.__timestamp = timestamp
        self.__name = name
        self.__data = data

    @classmethod
    def empty(cls) -> "CpioSection":
        return CpioSection(
            b'070707000001000000000666000000000000000001000000',
            1,
            b'',
            b'',
        )

    def set_name(self, name: bytes):
        self.__name = name

    def set_data(self, data: bytes):
        self.__data = data

    @classmethod
    def from_bytes(cls, data: bytes) -> Tuple["CpioSection", bytes]:
        """
        returns section and remainings of data that does not belong to the section returned
        """
        header = data[:76]
        headerstart = header[:48]
        mtime = int(header[48:59], base=8)
        name_size = int(header[59:65], base=8)
        body_size = int(header[65:], base=8)
        name = data[76:76+name_size-1]  # -1 cuz strings are 0-terminated
        sec_data = data[76+name_size:76+name_size+body_size]
        rest = data[76+name_size+body_size:]

        return CpioSection(headerstart, mtime, name, sec_data), rest

    def to_bytes(self) -> bytes:
        return b''.join((
            self.__headerstart,
            b'%011o' % self.__timestamp,
            b'%06o' % (len(self.__name) + 1),  # for zero end
            b'%011o' % len(self.__data),
            self.__name, b'\0',
            self.__data
        ))

    def name(self) -> bytes:
        return self.__name

    def data(self) -> bytes:
        return self.__data


def split_curly_block(data: bytes) -> Tuple[bytes, bytes]:
    view = memoryview(data)
    assert view[0:1] == b'{'
    brackets = 1
    in_string = False
    escaping = False
    for i in range(1, len(view)):
        c = view[i:i+1]
        if not in_string and not escaping:
            if c == b'{':
                brackets += 1
            elif c == b'}':
                brackets -= 1
            elif c == b'"':
                in_string = True
        elif in_string and not escaping and c == b'"':
            in_string = False

        if not escaping and c == b'\\':
            escaping = True
        elif escaping:
            escaping = False

        if brackets == 0:
            if data[i+1:i+2] == b'\n':  # add newline to data if it's next symbol
                i += 1
            return data[:i+1], data[i+1:]
    raise ValueError('input data is malformed')


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
            partlist.append(rf'\x{c:02x}')

    return ''.join(partlist)


def text_to_bytes(text: str) -> bytes:  # TODO: wtd are these? at least add unit tests
    partlist = []
    escaping = False
    hexcode = None
    for i, c in enumerate(text):
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
                raise RuntimeError(f'should not happen: escaping "{c}" at {i}')
        else:
            if c == '\\':
                escaping = True
            else:
                partlist.append(c.encode('ascii'))
    return b''.join(partlist)


from PySide2.QtWidgets import QWidget, QHBoxLayout, QVBoxLayout, QTabWidget, QTextEdit, QPushButton, QFileDialog,\
                              QInputDialog, QMessageBox, QLineEdit
from PySide2.QtGui import QFont
import string


class SectionDisplay(QWidget):
    def __init__(self, parent=None):
        super(SectionDisplay, self).__init__(parent)
        layout = QVBoxLayout(self)
        self.__section_tabs = QTabWidget()
        self.__header_edit = QTextEdit()
        self.__def_section_edit = QTextEdit()
        self.__chan_section_edit = QTextEdit()
        self.__val_section_edit = QTextEdit()
        self.__def_section: CpioSection = CpioSection.empty()
        self.__find_edit = QLineEdit()

        font = QFont('monospace')
        font.setFixedPitch(True)
        self.__header_edit.setFont(font)
        self.__def_section_edit.setFont(font)
        self.__chan_section_edit.setFont(font)
        self.__val_section_edit.setFont(font)
        self.__find_edit.setPlaceholderText('find in text')

        self.__section_tabs.addTab(self.__header_edit, 'header')
        self.__section_tabs.addTab(self.__def_section_edit, 'opspareparmdef')
        self.__section_tabs.addTab(self.__chan_section_edit, 'opchannels')
        self.__section_tabs.addTab(self.__val_section_edit, 'opvalues')
        layout.addWidget(self.__find_edit)
        layout.addWidget(self.__section_tabs)

        self.__find_edit.editingFinished.connect(self.__find_in_tab_text)

    def __find_in_tab_text(self):
        editor: QTextEdit = self.__section_tabs.currentWidget()
        editor.find(self.__find_edit.text())

    def set_section_data(self, data: bytes):
        parts = data.split(b'\n', 2)
        self.__header_edit.setText(bytes_to_text(b'\n'.join(parts[:2])))

        sec_to_edit = {
            b'opspareparmdef': self.__def_section_edit,
            b'opchannels': self.__chan_section_edit,
            b'opvalues': self.__val_section_edit,
        }

        data = parts[2]
        while data:
            sec_name, sec_data = data.lstrip().split(b'\n', 1)
            if sec_name == b'opspareparmdef':
                self.__def_section, data = CpioSection.from_bytes(sec_data)
                sec_to_edit[sec_name].setText(bytes_to_text(self.__def_section.data()))
            else:
                cur_data, data = split_curly_block(sec_data)
                sec_to_edit[sec_name].setText(bytes_to_text(cur_data))

    def get_section_data(self):
        self.__def_section.set_data(text_to_bytes(self.__def_section_edit.toPlainText()))
        return b'\n'.join((
            text_to_bytes(self.__header_edit.toPlainText()),
            b'opspareparmdef',
            self.__def_section.to_bytes(),
            b'opchannels',
            text_to_bytes(self.__chan_section_edit.toPlainText()),
            b'opvalues',
            text_to_bytes(self.__val_section_edit.toPlainText()),
        ))


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

    def _new_tab_widget(self) -> SectionDisplay:
        tab = SectionDisplay()
        return tab

    def load_file(self, filepath):
        self.clear()
        with open(filepath, 'rb') as f:
            self.__index = Index.parse_index_root(f.read())

        for entry in self.__index.entries:
            tab = self._new_tab_widget()

            text = bytes_to_text(entry.rawdata)
            assert entry.rawdata == text_to_bytes(text)

            tab.set_section_data(entry.rawdata)

            self.__tabs.append(tab)
            self.__tabwidget.addTab(tab, entry.name.decode('ascii'))

    def save_file(self, filepath):
        for entry, tab in zip(self.__index.entries, self.__tabs):
            entry.rawdata = tab.get_section_data()
        with open(filepath, 'wb') as f:
            f.write(self.__index.serialize())

    def _load_button_callback(self):
        filepath, _ = QFileDialog.getOpenFileName(self)
        if not filepath:
            return
        try:
            self.load_file(filepath)
        except Exception as e:
            QMessageBox.warning(self, 'Error happened, file NOT loaded', f'exception happened: {str(e)}')

    def _save_button_callback(self):
        filepath, _ = QFileDialog.getSaveFileName(self)
        if not filepath:
            return
        try:
            self.save_file(filepath)
        except Exception as e:
            QMessageBox.warning(self, 'Error happened, file NOT saved', f'exception happened: {str(e)}')

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
