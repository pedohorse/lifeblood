from lifeblood.logging import get_logger
from .long_op import LongOperation, LongOperationData, LongOperationProcessor

from typing import Callable, List, Optional

logger = get_logger('undoable_operations')


class OperationError(RuntimeError):
    pass


class StackLockedError(OperationError):
    pass


class UndoableOperation:
    def do(self, callback: Optional[Callable[["UndoableOperation"], None]] = None) -> bool:
        """
        callback must be executed after done if present
        return True if operatin was completed, False if operation was started, but will complete asyncronously.
         In that case operation must call undo stack's _operation_finalized(self)
        """
        raise NotImplementedError()

    def undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None) -> bool:
        raise NotImplementedError()


class StackAwareOperation(UndoableOperation):
    def __init__(self, undo_stack: "UndoStack"):
        self.__stack = undo_stack

    def _undo_stack(self):
        return self.__stack

    def do(self, callback: Optional[Callable[["UndoableOperation"], None]] = None) -> bool:
        success = False
        finished = False
        if not self.__stack._start_do_operation(self, callback):
            return False
        try:
            finished = self._my_do(callback)
            success = True
        finally:
            if finished:  # if not finished - we expect the op to eventually call _operation_finalized by itself
                self.__stack._operation_finalized(self, True, success)
        return True

    def undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None) -> bool:
        success = False
        finished = False
        if not self.__stack._start_undo_operation(self, callback):
            return False
        try:
            finished = self._my_undo(callback)
            success = True
        finally:
            if finished:  # if not finished - we expect the op to eventually call _operation_finalized by itself
                self.__stack._operation_finalized(self, False, success)
        return True

    def _my_do(self, callback: Optional[Callable[["UndoableOperation"], None]] = None) -> bool:
        raise NotImplementedError()

    def _my_undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None) -> bool:
        raise NotImplementedError()


class SimpleUndoableOperation(StackAwareOperation):
    def __init__(self, undo_stack: "UndoStack", forward_op: Callable[[Optional[Callable[["UndoableOperation"], None]]], None], backward_op: Callable[[Optional[Callable[["UndoableOperation"], None]]], None]):
        super().__init__(undo_stack)
        self.__stack = undo_stack
        self.__fwd_op = forward_op
        self.__bkw_op = backward_op

    def _my_do(self, callback: Optional[Callable[["UndoableOperation"], None]] = None) -> bool:
        self.__fwd_op(callback)
        return True

    def _my_undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None) -> bool:
        self.__bkw_op(callback)
        return True


class AsyncOperation(StackAwareOperation):
    def __init__(self, undo_stack: "UndoStack", op_processor: LongOperationProcessor):
        super().__init__(undo_stack)
        self.__op_processor = op_processor
        self.__was_done = False

    def _my_do(self, callback: Optional[Callable[["UndoableOperation"], None]] = None) -> bool:
        def doop(longop: LongOperation):
            success = True
            try:
                yield from self._my_do_longop(longop)
                self.__was_done = True
            except Exception as e:
                logger.exception(f'exception happened during do operation "{self}"')
                success = False
            finally:
                self._undo_stack()._operation_finalized(self, True, success)

            if success and callback:
                callback(self)

        if self.__was_done:
            raise OperationError('operation was done already')
        self.__op_processor.add_long_operation(doop, 'undoqueue')  # TODO: move this add_long_operation to an interface, move this class to undo_stack (so no scene dep)
        return False

    def _my_undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None):
        def undoop(longop: LongOperation):
            success = True
            try:
                yield from self._my_undo_longop(longop)
                self.__was_done = False
            except Exception as e:
                logger.exception(f'exception happened during do operation "{self}"')
                success = False
            finally:
                self._undo_stack()._operation_finalized(self, False, success)

            if success and callback:
                callback(self)

        if not self.__was_done:
            raise OperationError('operation was not done yet')
        self.__op_processor.add_long_operation(undoop, 'undoqueue')
        return False

    def _my_do_longop(self, longop: LongOperation):
        raise NotImplementedError()

    def _my_undo_longop(self, longop: LongOperation):
        raise NotImplementedError()


class UndoStack:
    logger = get_logger('undo_stack')

    def __init__(self, max_undos=100):
        self.__operations: List[UndoableOperation] = []
        self.__max_undos = max_undos
        self.__name_cache = None
        self.__locked = False  # this is NOT designed to be multithreaded...
        self.__todo_queue = []  # operations MUST execute one after another, even when async

    def __add_operation(self, op: UndoableOperation):
        self.__name_cache = None
        self.__operations.append(op)
        if len(self.__operations) > self.__max_undos:
            self.__operations = self.__operations[len(self.__operations)-self.__max_undos:]

    def operation_names(self):
        if self.__name_cache is None:
            self.__name_cache = [str(x) for x in self.__operations]
        return self.__name_cache

    def _start_do_operation(self, op: UndoableOperation, callback: Optional[Callable[["UndoableOperation"], None]]) -> bool:
        if self.__locked:
            self.__todo_queue.append((op.do, callback))
            return False
        self.__locked = True
        return True

    def _start_undo_operation(self, op: UndoableOperation, callback: Optional[Callable[["UndoableOperation"], None]]) -> bool:
        if self.__locked:
            self.__todo_queue.append((op.undo, callback))
            return False
        self.__locked = True
        return True

    def _operation_finalized(self, op: UndoableOperation, add_to_stack: bool, success: bool):
        if not success:
            self.logger.error('undo stack operation failed. clearing undo stack to be safe')
            self.__operations.clear()
            self.__todo_queue.clear()
            self.__locked = False
            return
        if add_to_stack:
            self.__add_operation(op)
        self.__locked = False
        # now check queue
        if self.__todo_queue:
            optodo, arg = self.__todo_queue.pop(0)
            optodo(arg)

    def perform_undo(self, count=1) -> List[UndoableOperation]:
        """
        undoes up to count of operations in the stack, returns list of undoed operations
        if an error happens - undo cycle will break, excluding broken operation from the return list

        :param count:
        :return:
        """
        if count < 1:
            return []
        self.__name_cache = None
        ret = []
        while self.__operations and count > 0:
            count -= 1
            op = self.__operations.pop()
            try:
                op.undo()
            except Exception:
                self.logger.exception('failed to perform undo. clearing undo stack to be safe')
                self.__operations.clear()
                self.__todo_queue.clear()
                break
            else:
                ret.append(op)
        return ret
