from datetime import datetime, timezone

from typing import Union

def global_timestamp_int() -> int:
    """
    tz/location independent global timestamp
    with persistent origin point
    """
    return int(datetime.now(tz=timezone.utc).timestamp())


def global_timestamp_float() -> float:
    """
    tz/location independent global timestamp
    with persistent origin point
    """
    return datetime.now(tz=timezone.utc).timestamp()


def global_timestamp_datetime() -> datetime:
    """
    tz/location independent global timestamp
    with persistent origin point
    """
    return datetime.now(tz=timezone.utc)


def global_timestamp_to_datetime(timestamp: Union[int, float, datetime]) -> datetime:
    if isinstance(timestamp, (int, float)):
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    elif isinstance(timestamp, datetime):
        return timestamp.astimezone(tz=timezone.utc)
    else:
        raise ValueError(f'wrong timestamp type {type(timestamp)}: {repr(timestamp)}')


def global_timestamp_to_local_datetime(timestamp: Union[int, float, datetime]) -> datetime:
    """
    convert global timestamp to local time datetime
    """
    return global_timestamp_to_datetime(timestamp).astimezone()
