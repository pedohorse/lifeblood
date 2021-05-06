from enum import Enum


class NodeParameterType(Enum):
    INT = 0
    BOOL = 1
    FLOAT = 2
    STRING = 3


class WorkerState(Enum):
    OFF = 0
    IDLE = 1
    BUSY = 2
    ERROR = 3


class WorkerPingState(Enum):
    OFF = 0
    CHECKING = 1
    ERROR = 2
    WORKING = 3


class TaskState(Enum):
    WAITING = 0  # arrived at node, does not know what to do
    GENERATING = 1  # node is generating work load
    READY = 2  # ready to be scheduled
    IN_PROGRESS = 3  # is being worked on by a worker
    POST_WAITING = 4  # task is waiting to be post processed by node
    POST_GENERATING = 5  # task is being post processed by node
    DONE = 6  # done, needs further processing
    ERROR = 7  # some internal error, not allowing to process task. NOT INVOCATION ERROR
    SPAWNED = 8  # spawned tasks are just passed down from node's "spawned" output
    DEAD = 9  # task will not be processed any more


class InvocationState(Enum):
    IN_PROGRESS = 0
    FINISHED = 1