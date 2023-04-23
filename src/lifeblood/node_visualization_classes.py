
from typing import Tuple


class NodeColorScheme:
    def __init__(self):
        self.__main_color = (0, 0, 0)
        self.__secondary_color = None

    def set_main_color(self, r: float, g: float, b: float):
        self.__main_color = (r, g, b)

    def set_secondary_color(self, r: float, g: float, b: float):
        self.__secondary_color = (r, g, b)

    def main_color(self):
        return self.__main_color

    def secondary_color(self):
        return self.__secondary_color