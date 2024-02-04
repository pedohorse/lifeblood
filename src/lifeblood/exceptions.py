from typing import Tuple


class ConfigurationError(RuntimeError):
    pass


class SchedulerConfigurationError(ConfigurationError):
    pass


class NodeNotReadyToProcess(Exception):
    pass


class NeedToRetryLater(RuntimeError):
    """
    Special exception that can be raised by certain functions
    that signify that the method called needs to wait for some db state to change before it can be performed.
    This is made to simplify implementation of functions that may need some number of attempts
    """
    pass


class AlreadyRunning(RuntimeError):
    pass


class NotEnoughResources(RuntimeError):
    pass


class WorkerNotAvailable(RuntimeError):
    pass


class ProcessInitializationError(RuntimeError):
    pass


class IncompleteReadError(ConnectionError):
    pass


class NotSubscribedError(RuntimeError):
    """
    UI State Accessor related exception: signifies that there is no subscription for the requested group
    """
    pass


class CouldNotNegotiateProtocolVersion(RuntimeError):
    def __init__(self,
                 our_supported_versions: Tuple[Tuple[int, int], ...],
                 their_supported_versions: Tuple[Tuple[int, int], ...]):
        self.__ours = our_supported_versions
        self.__theirs = their_supported_versions

    def __repr__(self):
        return f'<{self.__class__.__name__}: ours:{self.__ours}, theirs:{self.__theirs}>'


class InvocationMessageError(RuntimeError):
    pass


class InvocationMessageWrongInvocationId(InvocationMessageError):
    pass


class InvocationMessageAddresseeTimeout(InvocationMessageError):
    pass
