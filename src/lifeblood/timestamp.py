from datetime import datetime, timezone

from typing import Union

def global_timestamp_int() -> int:
    """
    tz/location independent global timestamp
    with persistent origin point
    """
    return int(datetime.utcnow().timestamp())


def global_timestamp_float() -> float:
    """
    tz/location independent global timestamp
    with persistent origin point
    """
    return datetime.utcnow().timestamp()


def global_timestamp_datetime() -> datetime:
    """
    tz/location independent global timestamp
    with persistent origin point
    """
    return datetime.utcnow()


def global_timestamp_to_local_datetime(timestamp: Union[int, float, datetime]) -> datetime:
    """
    convert global (utc) timestamp to local time datetime
    """
    if isinstance(timestamp, (int, float)):
        ts = datetime.fromtimestamp(timestamp)
        ts = ts.replace(tzinfo=timezone.utc)
    elif isinstance(timestamp, datetime):
        ts = timestamp.replace(tzinfo=timezone.utc)
    else:
        raise ValueError(f'wrong timestamp type {type(timestamp)}: {repr(timestamp)}')

    return ts.astimezone()
