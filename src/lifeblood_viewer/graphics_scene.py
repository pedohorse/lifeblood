from lifeblood import logging
from lifeblood.config import get_config
from .graphics_items.graphics_scene_container import GraphicsSceneWithNodesAndTasks
from .long_op import LongOperation, LongOperationData, LongOperationProcessor
from .undo_stack import UndoStack, UndoableOperation
from PySide6.QtCore import Signal, Slot

from typing import Callable, Dict, Generator, List, Optional, Tuple

logger = logging.get_logger('viewer')


class GraphicsScene(GraphicsSceneWithNodesAndTasks, LongOperationProcessor):
    operation_started = Signal(int)  # operation id
    operation_progress_updated = Signal(int, str, float)  # operation id, name, progress 0.0 - 1.0
    operation_finished = Signal(int)  # operation id

    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.__config = get_config('viewer')

        self.__long_operations: Dict[int, Tuple[LongOperation, Optional[str]]] = {}
        self.__long_op_queues: Dict[str, List[Callable[["LongOperation"], Generator]]] = {}

        self.__undo_stack: UndoStack = UndoStack()
        self.reset_undo_stack()

    def reset_undo_stack(self):
        self.__undo_stack = UndoStack(max_undos=self.__config.get_option_noasync('viewer.max_undo_history_size', 100))

    def undo_stack(self) -> UndoStack:
        return self.__undo_stack

    def undo(self, count=1) -> List[UndoableOperation]:
        return self.__undo_stack.perform_undo(count)

    def undo_stack_names(self) -> List[str]:
        return self.__undo_stack.operation_names()

    #
    # long operations
    #

    @Slot(LongOperationData)
    def process_operation(self, op: LongOperationData):
        assert op.op.opid() in self.__long_operations
        try:
            done = op.op._progress(op.data)
        except Exception as e:
            logger.exception(f'exception proceeding a long operation: {e}')
            done = True
        if done:
            self.__end_long_operation(op.op.opid())

    def add_long_operation(self, generator_to_call, queue_name: Optional[str] = None):
        newop = LongOperation(generator_to_call, lambda opid, opname, opprog: self.operation_progress_updated.emit(opid, opname, opprog))
        if queue_name is not None:
            queue = self.__long_op_queues.setdefault(queue_name, [])
            queue.insert(0, generator_to_call)
            if len(queue) > 1:  # if there is already something in there beside us
                return
        self.__long_operations[newop.opid()] = (newop, queue_name)
        self.__start_long_operation(newop)

    def __start_long_operation(self, newop):
        try:
            started_long = newop._start()  # False means operation already finished
        except Exception as e:
            logger.exception(f'exception starting a long operation: {e}')
            self.__end_long_operation(newop.opid())
            return
        if not started_long:
            self.__end_long_operation(newop.opid())

    def __end_long_operation(self, opid):
        op, queue_name = self.__long_operations.pop(opid)
        if queue_name is None:
            return
        queue = self.__long_op_queues[queue_name]
        assert len(queue) > 0
        queue.pop()  # popping ourserves
        if len(queue) > 0:
            newop = LongOperation(queue[-1], lambda opid, opname, opprog: self.operation_progress_updated.emit(opid, opname, opprog))
            self.__long_operations[newop.opid()] = (newop, queue_name)
            self.__start_long_operation(newop)

        # just in case - force UI redraw
        self.invalidate(layers=self.SceneLayer.ForegroundLayer)

    def long_operation_statuses(self) -> Tuple[Tuple[Tuple[int, Tuple[Optional[float], str]], ...], Dict[str, int]]:
        def _op_status_list(ops) -> Tuple[Tuple[int, Tuple[Optional[float], str]], ...]:
            return tuple((op.opid(), op.status()) for op in ops)

        return _op_status_list(x[0] for x in self.__long_operations.values()), \
            {qname: len(qval) for qname, qval in self.__long_op_queues.items()}
