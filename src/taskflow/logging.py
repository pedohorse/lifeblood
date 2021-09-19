import sys
import os
import time
import logging
from .paths import log_path
# init default logging


class UnimportantOnlyFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno in (logging.DEBUG, logging.INFO)


__logger_cache = {}
__default_loglevel = 'INFO'


def set_default_loglevel(loglevel):
    global __default_loglevel
    __default_loglevel = loglevel


def get_logger(name):
    global __logger_cache
    if name in __logger_cache:
        return __logger_cache[name]
    logger = logging.getLogger(name)
    __logger_cache[name] = logger
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('[%(asctime)s][%(module)s:%(process)d][%(levelname)s] %(message)s'))
    handler.setStream(sys.stdout)
    handler.setLevel(logging.NOTSET)
    handler.addFilter(UnimportantOnlyFilter())
    logger.addHandler(handler)

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('[%(asctime)s][%(module)s:%(process)d][%(levelname)s][%(funcName)s] %(message)s'))
    handler.setLevel(logging.WARNING)
    handler.setStream(sys.stderr)
    logger.addHandler(handler)

    logpath = log_path(f'{os.getpid()}.log', 'log')
    if logpath is not None:
        logfile_handler = logging.FileHandler(logpath)
        logfile_handler.setFormatter(logging.Formatter('[%(asctime)s][%(module)s:%(process)d][%(levelname)s][%(funcName)s] %(message)s'))
        logfile_handler.setLevel(logging.INFO)
        logger.addHandler(logfile_handler)

    logger.setLevel(__default_loglevel)
    logger.propagate = False
    return logger


def cleanup_logs():
    log_base_path = log_path(None, 'log', ensure_path_exists=False)
    if not log_base_path.exists():
        return
    keep_logs_period = 30 * 24 * 60 * 60
    now = time.time()
    for log in log_base_path.iterdir():
        if now - log.stat().st_mtime > keep_logs_period:
            try:
                log.unlink(missing_ok=True)
            except OSError:
                pass

# on first import - cleanup logs
cleanup_logs()
