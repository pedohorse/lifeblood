from time import perf_counter
from contextlib import contextmanager


class _TimeMeasurer:
    def __init__(self):
        self.__start = None
        self.__stop = None

    def start(self):
        self.__start = perf_counter()

    def stop(self):
        self.__stop = perf_counter()

    def elapsed(self):
        return self.__stop - self.__start


@contextmanager
def performance_measurer():
    tm = _TimeMeasurer()
    tm.start()
    yield tm
    tm.stop()
