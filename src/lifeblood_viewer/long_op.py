from typing import Optional, Callable, Generator, Any


class LongOperation:
    _nextid = 0

    def __init__(self, progress_callback: Callable[["LongOperation"], Generator]):
        """

        :param progress_callback: this is supposed to be a generator able to take this operation object and opdata as yield arguments.
        if it returns True - we consider long operation done
        """
        self.__id = self._nextid
        LongOperation._nextid += 1
        self.__progress_callback_factory = progress_callback
        self.__progress_callback: Optional[Generator] = None

    def _start(self):
        """
        just to make sure it starts when we need.
        if it starts in constructor - we might potentially have race condition when longop is not yet registered where it's being managed
        and receive _progress too soon
        """
        self.__progress_callback: Generator = self.__progress_callback_factory(self)
        try:
            next(self.__progress_callback)
        except StopIteration:
            return False
        return True

    def _progress(self, opdata) -> bool:
        try:
            self.__progress_callback.send(opdata)
        except StopIteration:
            return True
        return False

    def opid(self) -> int:
        return self.__id

    def new_op_data(self) -> "LongOperationData":
        return LongOperationData(self, None)


class LongOperationData:
    def __init__(self, op: LongOperation, data: Any = None):
        self.op = op
        self.data = data


class LongOperationProcessor:
    def add_long_operation(self, generator_to_call, queue_name: Optional[str] = None):
        raise NotImplementedError()
