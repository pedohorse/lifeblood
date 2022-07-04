from enum import Enum


class NodeParameterType(Enum):
    INT = 0
    BOOL = 1
    FLOAT = 2
    STRING = 3


class SchedulerMode(Enum):
    STANDARD = 0
    DORMANT = 1


class WorkerState(Enum):
    OFF = 0
    IDLE = 1
    BUSY = 2
    ERROR = 3
    INVOKING = 4  # worker is being fed data to start an invocation job
    UNKNOWN = 5  # this state the worker should be initialized in, then scheduler should figure it out


class WorkerPingState(Enum):
    OFF = 0
    CHECKING = 2
    ERROR = 3
    WORKING = 1
    UNKNOWN = 5


class TaskState(Enum):
    WAITING = 0  # arrived at node, does not know what to do
    GENERATING = 1  # node is generating work load
    READY = 2  # ready to be scheduled
    INVOKING = 11  # task is being switched to IN_PROGRESS
    IN_PROGRESS = 3  # is being worked on by a worker
    POST_WAITING = 4  # task is waiting to be post processed by node
    POST_GENERATING = 5  # task is being post processed by node
    DONE = 6  # done, needs further processing
    ERROR = 7  # some internal error, not allowing to process task. NOT INVOCATION ERROR
    SPAWNED = 8  # spawned tasks are just passed down from node's "spawned" output
    DEAD = 9  # task will not be processed any more
    SPLITTED = 10  # task has been splitted, and will remain idle until splits are gathered


class TaskGroupArchivedState(Enum):
    NOT_ARCHIVED = 0
    ARCHIVED = 1


class WorkerType(Enum):
    STANDARD = 0
    SCHEDULER_HELPER = 1


class InvocationState(Enum):
    IN_PROGRESS = 0
    FINISHED = 1
    INVOKING = 2


class SpawnStatus(Enum):
    SUCCEEDED = 0
    FAILED = 1


class ProcessPriorityAdjustment(Enum):
    NO_CHANGE = 0
    LOWER = 1
