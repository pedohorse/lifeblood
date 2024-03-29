from PySide2.QtWidgets import QLabel
from PySide2.QtGui import QPalette, QColor, QFont, QFontMetrics
from PySide2.QtCore import QTimer, Qt

from typing import Tuple


class FlashyLabel(QLabel):
    def __init__(self, parent):
        super().__init__(parent=parent)
        self.__hide_timer = QTimer(self)
        self.__hide_timer.timeout.connect(self.__timeout)
        self.hide()
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.__font_size = 48
        self.__state = 2
        self.__time = 0
        self.__fade_timer = 0
        self.__color = (255, 255, 255, 255)

    def set_size(self, size):
        font = self.font()
        font.setPixelSize(size)
        self.setFont(font)

    @classmethod
    def __split_long(cls, text: str, max_length: int) -> Tuple[str, ...]:
        lines = []
        while len(text) > max_length:
            pref = ''
            if text.startswith('- '):  # TODO: replace with a more generic regex
                pref = '  '
            lines.append(text[:max_length])
            text = pref + text[max_length:]
        if text:
            lines.append(text)
        return tuple(lines)

    def show_label(self, label: str, time: float):
        self.__color = (255, 255, 255, 255)
        label = label.strip()
        warn_prefix = '::warning::'
        err_prefix = '::error::'
        if label.startswith(warn_prefix):
            label = label[len(warn_prefix):].lstrip()
            self.__color = (247, 142, 4, 255)
        elif label.startswith(err_prefix):
            label = label[len(err_prefix):].lstrip()
            self.__color = (165, 17, 9, 255)
        # estimate width before label processing
        max_length = 0
        lines = label.split('\n')
        if len(lines) > 1:  # for multiline we use smaller font by default
            self.__font_size = 32
        else:
            self.__font_size = 48

        temp_font = QFont(self.font())
        temp_font.setPixelSize(self.__font_size)
        font_metrics = QFontMetrics(temp_font)
        for line in lines:
            max_length = max(max_length, int(self.parent().width()*19/20 / max(1, font_metrics.size(0, line).width() / max(1, len(line)))))
        label = '\n'.join('\n'.join(self.__split_long(s, max_length)) for s in lines)

        self.__state = 0
        self.__time = time
        self.__fade_timer = time / 2
        self.__hide_timer.setInterval(self.__time*1000 // 2)
        self.__hide_timer.start()

        self.setText(label)
        self.setStyleSheet(f'QLabel{{font: {self.__font_size}px; color: rgba({", ".join(str(x) for x in self.__color)});}}')
        self.resize(self.sizeHint())
        self.show()

    def __timeout(self):
        if self.__state == 0:
            self.__state = 1
            self.__hide_timer.setInterval(100)
            self.__hide_timer.start()
        elif self.__state == 1:
            self.__fade_timer -= 0.1
            # why "max(2" below for alpha? seem to be Qt bug when it SOMETIMES shows lower alpha as full alpha. does it take 1 for float 1.0 in normalized alpha?
            self.setStyleSheet(f'QLabel{{font: {self.__font_size}px; color: rgba({", ".join(str(x) for x in self.__color[:-1])}, {max(2, int(self.__color[-1]*self.__fade_timer*2/self.__time))});}}')
            if self.__fade_timer <= 0:
                self.__state = 2
                self.hide()
