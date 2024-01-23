from PySide2.QtWidgets import QLabel
from PySide2.QtGui import QPalette, QColor
from PySide2.QtCore import QTimer, Qt


class FlashyLabel(QLabel):
    def __init__(self, parent):
        super().__init__(parent=parent)
        self.__hide_timer = QTimer(self)
        self.__hide_timer.timeout.connect(self.__timeout)
        self.hide()
        font = self.font()
        font.setPixelSize(48)
        self.setFont(font)
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.__state = 2
        self.__time = 0
        self.__fade_timer = 0
        self.__color = (255, 255, 255, 255)

    def set_size(self, size):
        font = self.font()
        font.setPixelSize(size)
        self.setFont(font)

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

        self.__state = 0
        self.__time = time
        self.__fade_timer = time / 2
        self.__hide_timer.setInterval(self.__time*1000 // 2)
        self.__hide_timer.start()
        self.setText(label)
        self.resize(self.sizeHint())
        self.setStyleSheet(f'QLabel{{color: rgba({", ".join(str(x) for x in self.__color)});}}')
        self.show()

    def __timeout(self):
        if self.__state == 0:
            self.__state = 1
            self.__hide_timer.setInterval(100)
            self.__hide_timer.start()
        elif self.__state == 1:
            self.__fade_timer -= 0.1
            self.setStyleSheet(f'QLabel{{color: rgba({", ".join(str(x) for x in self.__color[:-1])}, {max(0, int(self.__color[-1]*self.__fade_timer*2/self.__time))});}}')
            if self.__fade_timer <= 0:
                self.__state = 2
                self.hide()
