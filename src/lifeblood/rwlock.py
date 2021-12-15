from threading import Lock
from contextlib import contextmanager


class RWLock:
    def __init__(self):
        self.__helper_lock = Lock()
        self.__wlock = Lock()
        self.__num_readers = 0

    def acquire_reader(self):
        with self.__helper_lock:
            if self.__num_readers == 0:
                self.__wlock.acquire()
            self.__num_readers += 1

    def release_reader(self):
        with self.__helper_lock:
            self.__num_readers -= 1
            if self.__num_readers == 0:
                self.__wlock.release()

    def acquire_writer(self):
        self.__wlock.acquire()

    def release_writer(self):
        self.__wlock.release()

    @contextmanager
    def reader_lock(self):
        try:
            self.acquire_reader()
            yield
        finally:
            self.release_reader()

    @contextmanager
    def writer_lock(self):
        try:
            self.acquire_writer()
            yield
        finally:
            self.release_writer()
