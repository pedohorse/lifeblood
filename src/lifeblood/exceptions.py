
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
