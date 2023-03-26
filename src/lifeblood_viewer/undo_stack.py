from lifeblood.logging import get_logger

from typing import Callable, List


class UndoableOperation:
    def redo(self):
        raise NotImplementedError()

    def undo(self):
        raise NotImplementedError()


class SimpleUndoableOperation(UndoableOperation):
    def __init__(self, forward_op: Callable[[], None], backward_op: Callable[[], None]):
        self.__fwd_op = forward_op
        self.__bkw_op = backward_op

    def redo(self):
        self.__fwd_op()

    def undo(self):
        self.__bkw_op()


class UndoStack:
    logger = get_logger('undo_stack')

    def __init__(self, max_undos=100):
        self.__operations: List[UndoableOperation] = []
        self.__max_undos = max_undos
        self.__name_cache = None

    def add_operation(self, op: UndoableOperation):
        self.__name_cache = None
        self.__operations.append(op)
        if len(self.__operations) > self.__max_undos:
            self.__operations = self.__operations[len(self.__operations)-self.__max_undos:]

    def operation_names(self):
        if self.__name_cache is None:
            self.__name_cache = [str(x) for x in self.__operations]
        return self.__name_cache

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
                self.logger.exception('failed to perform undo')
                break
            else:
                ret.append(op)
        return ret
