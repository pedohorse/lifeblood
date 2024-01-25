from typing import Any, Callable, Dict, Generator, Optional, Tuple


class LongOperation:
    _nextid = 0

    def __init__(self, progress_callback: Callable[["LongOperation"], Generator], status_change_callback: Optional[Callable[[int, str, float], None]] = None):
        """

        :param progress_callback: this is supposed to be a generator able to take this operation object and opdata as yield arguments.
        if it returns True - we consider long operation done
        """
        self.__id = self._nextid
        LongOperation._nextid += 1
        self.__progress_callback_factory = progress_callback
        self.__progress_callback: Optional[Generator] = None
        self.__status_report_callback = status_change_callback
        self.__display_name = ''
        self.__display_progress = None

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
        """
        NOTE: it's NOT about displayed progress!
        progress operation with new data
        """
        try:
            self.__progress_callback.send(opdata)
        except StopIteration:
            return True
        return False

    def opid(self) -> int:
        return self.__id

    def set_op_status(self, progress: Optional[float], name: Optional[str] = None):
        if progress is not None:
            self.__display_progress = progress
        if name is not None:
            self.__display_name = name
        if self.__status_report_callback is not None:
            self.__status_report_callback(self.opid(), self.__display_name or '', self.__display_progress or -1.0)

    def status(self) -> Tuple[Optional[float], str]:
        return self.__display_progress, self.__display_name

    def new_op_data(self) -> "LongOperationData":
        return LongOperationData(self, None)


class LongOperationData:
    def __init__(self, op: LongOperation, data: Any = None):
        self.op = op
        self.data = data


class LongOperationProcessor:
    def add_long_operation(self, generator_to_call, queue_name: Optional[str] = None):
        raise NotImplementedError()

    def long_operation_statuses(self) -> Tuple[Tuple[Tuple[int, Tuple[Optional[float], str]], ...], Dict[str, int]]:
        """
        statuses on all long operations in all queues

        returns:
            a tuple of tuples containing a pair of operation id and operation status (that is a tuple of optional completion percentage and status string),
            and a dict with statistics of queue names to the number of items in that queue
        """
        raise NotImplementedError()
