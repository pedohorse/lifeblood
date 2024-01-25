from enum import Enum
from dataclasses import dataclass
from lifeblood.logging import get_logger
from .long_op import LongOperation, LongOperationProcessor

from typing import Callable, List, Optional

logger = get_logger('undoable_operations')


class OperationError(RuntimeError):
    """
    General Op-related error
    """
    pass


class StackLockedError(OperationError):
    pass


class OperationCompletionStatus(Enum):
    """
    This enum represents result of a successfully completed operation.
    operation is considered FAILED if an error occurred in operation execution/communication itself
    (especially for async operations that communicate with scheduler)

    Yet a successful operation may still perform not the expected result due to
     for example hitting scheduler's constraints, like not being able to delete node
     if that node has tasks.
    """
    FullSuccess = 0  # means operation was performed as intended
    PartialSuccess = 1  # means operation was performed, but not as it was intended (like u wanted to delete 2 nodes, but only 1 was actually deleted)
    NotPerformed = 2  # means operation was not performed or resulted in no changes


@dataclass
class OperationCompletionDetails:
    """
    information about a completed operation
    """
    status: OperationCompletionStatus
    details: Optional[str] = None


class UndoableOperation:
    """
    base class for undoable operations
    undoable operation keeps information how to undo itself
    """

    def do(self, callback: Optional[Callable[["UndoableOperation", OperationCompletionDetails], None]] = None) -> bool:
        """
        do the operation.
        operation can be done only once

        callback must be executed after done if present
        return True if operation was completed, False if operation was started, but will complete asynchronously.
         In that case operation must call undo stack's _operation_finalized(self)
        """
        raise NotImplementedError()

    def undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None) -> bool:
        """
        undo the operation.
        do() is supposed to be called before undo call, otherwise it is incorrect undo call
        """
        raise NotImplementedError()


class StackAwareOperation(UndoableOperation):
    """
    helper class
    undoable operation with the notion of undo stack
    """
    def __init__(self, undo_stack: "UndoStack"):
        self.__stack = undo_stack

    def _undo_stack(self):
        return self.__stack

    def do(self, callback: Optional[Callable[["UndoableOperation", OperationCompletionDetails], None]] = None) -> bool:
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

    def _my_do(self, callback: Optional[Callable[["UndoableOperation", OperationCompletionDetails], None]] = None) -> bool:
        """
        reimplement this function in child classes instead of do()
        """
        raise NotImplementedError()

    def _my_undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None) -> bool:
        """
        reimplement this function in child classes instead of undo()
        """
        raise NotImplementedError()


class SimpleUndoableOperation(StackAwareOperation):
    """
    simple undoable operation helper class
    you just need to provide op and undo as lambdas
    """
    def __init__(self, undo_stack: "UndoStack", forward_op: Callable[[Optional[Callable[["UndoableOperation", OperationCompletionDetails], None]]], None], backward_op: Callable[[Optional[Callable[["UndoableOperation"], None]]], None]):
        super().__init__(undo_stack)
        self.__stack = undo_stack
        self.__fwd_op = forward_op
        self.__bkw_op = backward_op

    def _my_do(self, callback: Optional[Callable[["UndoableOperation", OperationCompletionDetails], None]] = None) -> bool:
        self.__fwd_op(callback)
        return True

    def _my_undo(self, callback: Optional[Callable[["UndoableOperation"], None]] = None) -> bool:
        self.__bkw_op(callback)
        return True


class AsyncOperation(StackAwareOperation):
    """
    base class for all asynchronous operations
    async operations relay on LongOperation concept
    async operations' do and undo complete immediately,
    so it's more of a fire and forget thing.
    you may provide callback lambda that will be executed on operation success
    """
    def __init__(self, undo_stack: "UndoStack", op_processor: LongOperationProcessor):
        super().__init__(undo_stack)
        self.__op_processor = op_processor
        self.__was_done = False
        self.__op_result = OperationCompletionDetails(OperationCompletionStatus.FullSuccess, None)

    def _set_result(self, op_result: OperationCompletionDetails):
        """
        your overriden _my_do_longop may call this to define op the state
        of the operation result
        """
        self.__op_result = op_result

    def _my_do_result(self) -> Optional[OperationCompletionDetails]:
        if self.__was_done:
            return self.__op_result
        return None

    def _my_do(self, callback: Optional[Callable[["UndoableOperation", OperationCompletionDetails], None]] = None) -> bool:
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
                op_result = self._my_do_result()
                assert op_result is not None
                callback(self, op_result)

        if self.__was_done:
            raise OperationError('operation was done already')
        self.__op_processor.add_long_operation(doop, 'undo queue')  # TODO: move this add_long_operation to an interface, move this class to undo_stack (so no scene dep)
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
        self.__op_processor.add_long_operation(undoop, 'undo queue')
        return False

    def _my_do_longop(self, longop: LongOperation):
        """
        override this in the subclass

        :param longop: containing long operation, this op has to report back to given longop when done
        """
        raise NotImplementedError()

    def _my_undo_longop(self, longop: LongOperation):
        """
        override this in the subclass

        :param longop: containing long operation, this op has to report back to given longop when done
        """
        raise NotImplementedError()


class UndoStack:
    """
    stack of operations.
    operations can be added to the stack after they have been done,
    and removed from stack just before performing and undo
    """
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

    def operation_names(self) -> List[str]:
        """
        return list of short descriptions of operations on the stack
        """
        if self.__name_cache is None:
            self.__name_cache = [str(x) for x in self.__operations]
        return self.__name_cache

    def _start_do_operation(self, op: UndoableOperation, callback: Optional[Callable[["UndoableOperation", OperationCompletionDetails], None]]) -> bool:
        """
        stack aware operation will call this at the start of do op
        """
        if self.__locked:
            self.__todo_queue.append((op.do, callback))
            return False
        self.__locked = True
        return True

    def _start_undo_operation(self, op: UndoableOperation, callback: Optional[Callable[["UndoableOperation"], None]]) -> bool:
        """
        stack aware operation will call this at the start of undo op
        """
        if self.__locked:
            self.__todo_queue.append((op.undo, callback))
            return False
        self.__locked = True
        return True

    def _operation_finalized(self, op: UndoableOperation, add_to_stack: bool, success: bool):
        """
        stack aware operation will call this AFTER finished do or undo of an op
        """
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
