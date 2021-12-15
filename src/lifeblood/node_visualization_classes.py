
from typing import Tuple


class NodeColorScheme:
    def __init__(self):
        self.__main_color = (0, 0, 0)

    def set_main_color(self, r: float, g: float, b: float):
        self.__main_color = (r, g, b)

    def main_color(self):
        return self.__main_color