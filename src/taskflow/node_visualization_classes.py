
from typing import Tuple


class NodeColorScheme:
    def __init__(self):
        self.__main_color = (0, 0, 0)

    def set_main_color(self, color: Tuple[float, float, float]):
        self.__main_color = color

    def main_color(self):
        return self.__main_color