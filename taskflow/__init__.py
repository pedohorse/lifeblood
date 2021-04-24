import sys
import logging
# init default logging

logger = logging.getLogger('Scheduler')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s'))
handler.setStream(sys.stdout)
logger.addHandler(handler)
logger.setLevel('DEBUG')

