import sys
import logging
# init default logging


class UnimportantOnlyFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno in (logging.DEBUG, logging.INFO)


__logger_cache = {}
__default_loglevel = 'INFO'


def set_default_loglevel(loglevel):
    global __default_loglevel
    __default_loglevel = loglevel


def getLogger(name):
    global __logger_cache
    if name in __logger_cache:
        return __logger_cache[name]
    logger = logging.getLogger(name)
    __logger_cache[name] = logger
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s'))
    handler.setStream(sys.stdout)
    handler.setLevel(logging.NOTSET)
    handler.addFilter(UnimportantOnlyFilter())
    logger.addHandler(handler)

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('[%(asctime)s][%(levelname)s][%(funcName)s] %(message)s'))
    handler.setLevel(logging.WARNING)
    handler.setStream(sys.stderr)
    logger.addHandler(handler)

    logger.setLevel(__default_loglevel)
    logger.propagate = False
    return logger
