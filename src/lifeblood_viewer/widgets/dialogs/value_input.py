import re
from PySide6.QtWidgets import QDialog, QHBoxLayout, QLabel, QLineEdit, QPushButton, QVBoxLayout, QWidget
from PySide6.QtGui import QValidator
from PySide6.QtCore import Signal, Slot

from typing import Any, Iterable, List, Optional


def text_to_range(text: str):
    # TODO: this may be reused somewhere else, so better to put it somewhere else too
    frames = []

    parts = re.split(r'[ ,]+', text)
    for part in parts:
        if part == '':
            continue
        elif match := re.match(r'^-?\d+$', part):
            frames.append(int(match.group(0)))
        elif match := re.match(r'^(-?\d+)-(-?\d+)$', part):
            a = int(match.group(1))
            b = int(match.group(2))
            if a > b:
                a, b = (b, a)
            frames.extend(range(a, b+1))
        else:
            raise ValueError(f'bad text formatting, cannot parse part "{part}"')
    return frames


def range_to_text(ints: List[int], allow_sort=True):
    if len(ints) == 0:
        return ''
    if allow_sort:
        ints = sorted(ints)
    text_parts = []
    last_seq_candidate = []

    def _finalize_sequence():
        if not last_seq_candidate:
            return
        if val - last_seq_candidate[-1] == 1:  # so it's a sequential value
            last_seq_candidate.append(val)
        else:  # time to stop sequence
            if len(last_seq_candidate) >= 3:  # min num of elems to be considered a sequence
                text_parts.append(f'{last_seq_candidate[0]}-{last_seq_candidate[-1]}')
            else:
                text_parts.extend(str(x) for x in last_seq_candidate)
            # and start new sequence part
            last_seq_candidate.clear()
            last_seq_candidate.append(val)

    for val in ints:
        if not last_seq_candidate:
            last_seq_candidate.append(val)
        else:
            _finalize_sequence()
    _finalize_sequence()

    return ' '.join(text_parts)


def test_ranges():
    # TODO: this should be a porper unittest
    assert text_to_range('') == []
    assert text_to_range('1') == [1]
    assert text_to_range('1 4') == [1, 4]
    assert text_to_range('4 11') == [4, 11]
    assert text_to_range('2, , ,,,3,4,-5') == [2, 3, 4, -5]
    assert text_to_range(' ,,,3,55-59   -5') == [3, 55, 56, 57, 58, 59, -5]

    assert range_to_text([]) == ''
    assert range_to_text([1, 2, 3, 4, 5]) == '1-5'
    assert range_to_text([1, 2, 4, 5]) == '1 2 4 5'
    assert range_to_text([1, 2, 4, 5, 6]) == '1 2 4-6'
    assert range_to_text([0, 1, 2, 4, 5, 6]) == '0-2 4-6'


class FrameRangeValidator(QValidator):
    def validate(self, text_input, pos):
        try:
            text_to_range(text_input)
        except ValueError:
            try:
                # and what if there's an extra digit input?
                text_to_range(text_input[:pos] + '0' + text_input[pos:])
            except ValueError:
                return QValidator.Invalid
            return QValidator.Intermediate
        return QValidator.Acceptable


class InputWidget(QWidget):
    validity_changed = Signal(object, object)

    def get_value(self) -> Any:
        raise NotImplementedError()


class IntListInputWidget(InputWidget):
    def __init__(self, init_value: Iterable[int] = (), parent: Optional[QWidget] = None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setMargin(0)
        self.__input = QLineEdit()
        layout.addWidget(self.__input)
        self.__validator = FrameRangeValidator()
        self.__input.setValidator(self.__validator)
        self.__input.setToolTip('example: 1 2 3-10, 12,15')

        # connect
        self.__input.textChanged.connect(self._text_changed)

        # initial value
        self.__input.setText(range_to_text(list(init_value)))

    def _text_changed(self, text):
        state = self.__validator.validate(text, 0)
        self.validity_changed.emit(self, state)

    def get_value(self):
        return text_to_range(self.__input.text())


class MultiInputDialog(QDialog):
    """
    allows entering int list in form of ranges
    """

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)

        main_layout = QVBoxLayout(self)
        self.__inputs_layout = QVBoxLayout()
        main_layout.addLayout(self.__inputs_layout)
        self.__inputs: List[InputWidget] = []

        self.__ok_button = QPushButton('Ok')
        cancel_button = QPushButton('Cancel')

        btn_layout = QHBoxLayout()
        btn_layout.addWidget(self.__ok_button)
        btn_layout.addWidget(cancel_button)

        main_layout.addLayout(btn_layout)

        # connec
        self.__ok_button.pressed.connect(self.accept)
        cancel_button.pressed.connect(self.reject)

    def add_input(self, label: str, wgt: InputWidget):
        layout = QHBoxLayout()
        layout.setMargin(0)
        layout.addWidget(QLabel(label))
        layout.addWidget(wgt)
        self.__inputs_layout.addLayout(layout)
        self.__inputs.append(wgt)
        wgt.validity_changed.connect(self._validity_changed)

    def get_values(self) -> List[Any]:
        return [input_widget.get_value() for input_widget in self.__inputs]

    @Slot(object, object)
    def _validity_changed(self, widget: QWidget, state: QValidator.State):
        if state == QValidator.State.Invalid:
            self.__ok_button.setEnabled(False)
        elif state == QValidator.State.Intermediate:
            self.__ok_button.setEnabled(False)
        else:
            self.__ok_button.setEnabled(True)


def test_widget():
    import sys
    from PySide6.QtWidgets import QApplication
    app = QApplication(sys.argv)

    wgt = MultiInputDialog()
    wgt.add_input('frames', IntListInputWidget([1, 2, 3, 5, 6, 7, 8]))

    def _show_stuff():
        print(wgt.get_values())

    wgt.show()
    wgt.accepted.connect(_show_stuff)

    return app.exec_()


if __name__ == '__main__':
    test_ranges()
    test_widget()
