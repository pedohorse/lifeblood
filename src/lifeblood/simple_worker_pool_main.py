import argparse
import asyncio
import signal
import json
from .config import get_config
from .broadcasting import await_broadcast

from .logging import get_logger

from .simple_worker_pool import SimpleWorkerPool
from .worker_pool_message_processor import WorkerPoolMessageProcessor
from .enums import WorkerType, ProcessPriorityAdjustment

from .net_messages.address import AddressChain


async def create_worker_pool(worker_type: WorkerType = WorkerType.STANDARD, *,
                             minimal_total_to_ensure=0, minimal_idle_to_ensure=0, maximum_total=256,
                             idle_timeout=10, worker_suspicious_lifetime=4, housekeeping_interval: float = 10,
                             idle_timeout_boost: float = 0.0,
                             priority=ProcessPriorityAdjustment.NO_CHANGE, scheduler_address: AddressChain):
    swp = SimpleWorkerPool(
        worker_type,
        minimal_total_to_ensure=minimal_total_to_ensure, minimal_idle_to_ensure=minimal_idle_to_ensure, maximum_total=maximum_total,
        idle_timeout=idle_timeout, worker_suspicious_lifetime=worker_suspicious_lifetime, housekeeping_interval=housekeeping_interval,
        idle_timeout_boost=idle_timeout_boost,
        priority=priority,
        scheduler_address=scheduler_address,
        message_processor_factory=WorkerPoolMessageProcessor,
    )
    return swp


async def async_main(argv):
    logger = get_logger('simple_worker_pool')
    parser = argparse.ArgumentParser(
        'lifeblood pool simple',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('--min-idle', '-m',
                        dest='minimal_idle_to_ensure',
                        default=1, type=int,
                        help='worker pool will ensure at least this amount of workers is up idle')
    parser.add_argument('--min-total',
                        dest='minimal_total_to_ensure',
                        default=0, type=int,
                        help='worker pool will ensure at least this amount of workers is up total')
    parser.add_argument('--max', '-M',
                        dest='maximum_total',
                        default=256, type=int,
                        help='no more than this amount of workers will be run locally at the same time')
    parser.add_argument('--idle-timeout',
                        dest='idle_timeout',
                        default=60.0, type=float,
                        help='workers idle for more than this period will be shut down if needed to respect given min constraints')
    parser.add_argument('--suspicious-lifetime',
                        dest='worker_suspicious_lifetime',
                        default=4.0, type=float,
                        help='if workers die within given interval - worker spawning will be throttled down')
    parser.add_argument('--idle-timeout-boost-interval',
                        dest='idle_timeout_boost',
                        default=30.0, type=float,
                        )
    parser.add_argument('--priority', choices=tuple(x.name for x in ProcessPriorityAdjustment), default=ProcessPriorityAdjustment.LOWER.name, help='pass to spawned workers: adjust child process priority')

    opts = parser.parse_args(argv)
    opts.priority = [x for x in ProcessPriorityAdjustment if x.name == opts.priority][0]  # there MUST be exactly 1 match

    graceful_closer_no_reentry = False

    def graceful_closer(*args):
        nonlocal graceful_closer_no_reentry
        if graceful_closer_no_reentry:
            print('DOUBLE SIGNAL CAUGHT: ALREADY EXITING')
            return
        graceful_closer_no_reentry = True
        logger.info('SIGINT/SIGTERM caught')
        nonlocal noloop
        noloop = True
        stop_event.set()
        if pool:
            pool.stop()

    noasync_do_close = False

    def noasync_windows_graceful_closer_event(*args):
        nonlocal noasync_do_close
        noasync_do_close = True

    async def windows_graceful_closer():
        while not noasync_do_close:
            await asyncio.sleep(1)
        graceful_closer()

    logger.debug(f'starting {__name__} with: ' + ', '.join(f'{key}={val}' for key, val in opts.__dict__.items()))
    pool = None
    noloop = False  # TODO: add arg

    # override event handlers
    win_signal_waiting_task = None
    try:
        asyncio.get_event_loop().add_signal_handler(signal.SIGINT, graceful_closer)
        asyncio.get_event_loop().add_signal_handler(signal.SIGTERM, graceful_closer)
    except NotImplementedError:  # solution for windows
        signal.signal(signal.SIGINT, noasync_windows_graceful_closer_event)
        signal.signal(signal.SIGBREAK, noasync_windows_graceful_closer_event)
        win_signal_waiting_task = asyncio.create_task(windows_graceful_closer())
    #

    stop_event = asyncio.Event()
    stop_task = asyncio.create_task(stop_event.wait())
    config = get_config('worker')

    start_attempt_cooldown = 0
    while True:
        if await config.get_option('worker.listen_to_broadcast', True):
            logger.info('listening for scheduler broadcasts...')
            broadcast_task = asyncio.create_task(await_broadcast('lifeblood_scheduler'))
            done, _ = await asyncio.wait((broadcast_task, stop_task), return_when=asyncio.FIRST_COMPLETED)
            if stop_task in done:
                broadcast_task.cancel()
                logger.info('broadcast listening cancelled')
                break
            assert broadcast_task.done()
            message = await broadcast_task
            scheduler_info = json.loads(message)
            logger.debug('received', scheduler_info)
            if 'message_address' not in scheduler_info:
                logger.debug('broadcast does not have "message_address" key, ignoring')
                continue
            addr = AddressChain(scheduler_info['message_address'])
        else:
            if stop_event.is_set():
                break
            logger.info('boradcast listening disabled')
            start_attempt_cooldown = 10
            if not config.has_option_noasync('worker.scheduler_address'):
                raise RuntimeError('worker.scheduler_address config option must be provided')
            addr = AddressChain(await config.get_option('worker.scheduler_address', None))
            logger.debug(f'using {addr}')

        try:
            pool = await create_worker_pool(WorkerType.STANDARD, scheduler_address=addr, **opts.__dict__)
            await pool.start()
        except Exception:
            logger.exception('could not start the pool')
            await asyncio.sleep(start_attempt_cooldown)
        else:
            await pool.wait_till_stops()
            logger.info('pool quited')
        if noloop:
            break

    if win_signal_waiting_task is not None:
        if not win_signal_waiting_task.done():
            win_signal_waiting_task.cancel()
    logger.info('pool loop stopped')


def main(argv):
    try:
        asyncio.run(async_main(argv))
    except KeyboardInterrupt:
        get_logger('simple_worker_pool').warning('SIGINT caught where it wasn\'t supposed to be caught')

