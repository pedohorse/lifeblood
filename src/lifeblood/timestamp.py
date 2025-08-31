from datetime import datetime


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
