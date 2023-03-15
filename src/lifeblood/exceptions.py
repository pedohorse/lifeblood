
class ConfigurationError(RuntimeError):
    pass


class SchedulerConfigurationError(ConfigurationError):
    pass


class NodeNotReadyToProcess(Exception):
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
