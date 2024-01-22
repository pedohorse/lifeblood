from dataclasses import dataclass


@dataclass
class WorkerMetadata:
    """
    This class represents data that is not important for scheduler-worker work,
    but may be useful for UI, search and other extra operations
    Some metadata is optional, some, like hostname, is always expected to be present
    """
    hostname: str
