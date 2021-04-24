import sys
import logging
from logging import getLogger
# init default logging


class UnimportantOnlyFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno in (logging.DEBUG, logging.INFO)


def init():
    logger = logging.getLogger('Scheduler')
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

    logger.setLevel('DEBUG')


init()
